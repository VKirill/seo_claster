"""
Скрипт для обновления частот группы "фотосессия" из CSV файла в Master DB
"""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
from seo_analyzer.core.helpers import normalize_dataframe_columns, load_csv_data
from seo_analyzer.core.query_groups import QueryGroupManager


def update_frequencies_fotosesiya():
    """
    Обновляет частоты группы "фотосессия" из CSV файла в Master DB
    """
    group_name = "фотосессия"
    
    print("=" * 80)
    print(f"🔄 Обновление частот для группы '{group_name}' из CSV")
    print("=" * 80)
    print()
    
    # Инициализируем менеджер групп
    group_manager = QueryGroupManager()
    group_manager.discover_groups()
    
    # Получаем группу
    group = group_manager.get_group(group_name)
    if not group:
        print(f"❌ Группа '{group_name}' не найдена")
        print(f"💡 Проверьте наличие группы в keyword_group/")
        return False
    
    if not group.input_file.exists():
        print(f"❌ CSV файл не найден: {group.input_file}")
        return False
    
    # Загружаем данные из CSV
    print(f"📂 Загрузка CSV: {group.input_file}")
    raw_df = load_csv_data(group.input_file)
    
    if raw_df.empty:
        print("❌ CSV файл пустой")
        return False
    
    print(f"✓ Загружено {len(raw_df)} запросов из CSV")
    
    # Нормализуем колонки (обрабатывает частоты с пробелами)
    df = normalize_dataframe_columns(raw_df)
    
    # Проверяем частоты в CSV
    freq_world_count = 0
    freq_exact_count = 0
    
    if 'frequency_world' in df.columns:
        freq_world_count = (pd.to_numeric(df['frequency_world'], errors='coerce') > 0).sum()
        print(f"✓ Частота (мир) в CSV: {freq_world_count} из {len(df)} с ненулевой частотой")
    else:
        print("⚠️  Колонка 'frequency_world' не найдена в CSV")
    
    if 'frequency_exact' in df.columns:
        freq_exact_count = (pd.to_numeric(df['frequency_exact'], errors='coerce') > 0).sum()
        print(f"✓ Частота (точная) в CSV: {freq_exact_count} из {len(df)} с ненулевой частотой")
    else:
        print("⚠️  Колонка 'frequency_exact' не найдена в CSV")
    
    if freq_world_count == 0 and freq_exact_count == 0:
        print("\n⚠️  В CSV файле нет ненулевых частот!")
        print("💡 Проверьте CSV файл на наличие колонок frequency_world и frequency_exact")
        return False
    
    # Загружаем существующие данные из БД
    print(f"\n📦 Загрузка данных из Master DB...")
    master_db = MasterQueryDatabase()
    existing_df = master_db.load_queries(group_name, include_serp_urls=False)
    
    if existing_df is None or existing_df.empty:
        print(f"⚠️  Группа '{group_name}' не найдена в БД")
        print(f"💡 Запустите полный анализ для создания записи в БД")
        return False
    
    print(f"✓ Загружено {len(existing_df)} запросов из БД")
    
    # Проверяем текущие частоты в БД
    if 'frequency_world' in existing_df.columns:
        db_freq_world = (pd.to_numeric(existing_df['frequency_world'], errors='coerce') > 0).sum()
        print(f"  Текущие частоты в БД (мир): {db_freq_world} из {len(existing_df)} с ненулевой частотой")
    
    if 'frequency_exact' in existing_df.columns:
        db_freq_exact = (pd.to_numeric(existing_df['frequency_exact'], errors='coerce') > 0).sum()
        print(f"  Текущие частоты в БД (точная): {db_freq_exact} из {len(existing_df)} с ненулевой частотой")
    
    # Создаем результирующий DataFrame на основе данных из БД
    result_df = existing_df.copy()
    
    # Обновляем частоты из CSV
    print(f"\n🔄 Обновление частот из CSV...")
    frequencies_world_updated = 0
    frequencies_exact_updated = 0
    
    # Создаем индекс по keyword для быстрого поиска
    csv_indexed = df.set_index('keyword') if 'keyword' in df.columns else None
    
    for idx, row in result_df.iterrows():
        keyword = row.get('keyword')
        if not keyword:
            continue
        
        updated = False
        
        # Обновляем frequency_world
        if csv_indexed is not None and keyword in csv_indexed.index:
            csv_row = csv_indexed.loc[keyword]
            
            if 'frequency_world' in df.columns and 'frequency_world' in result_df.columns:
                csv_freq_world = csv_row.get('frequency_world', 0)
                # Преобразуем в число, обрабатываем пробелы и строки
                try:
                    if isinstance(csv_freq_world, str):
                        csv_freq_world = csv_freq_world.replace(' ', '').replace(',', '')
                    csv_freq_world = pd.to_numeric(csv_freq_world, errors='coerce')
                    if pd.notna(csv_freq_world):
                        result_df.at[idx, 'frequency_world'] = int(csv_freq_world)
                        frequencies_world_updated += 1
                        updated = True
                except:
                    pass
            
            if 'frequency_exact' in df.columns and 'frequency_exact' in result_df.columns:
                csv_freq_exact = csv_row.get('frequency_exact', 0)
                # Преобразуем в число, обрабатываем пробелы и строки
                try:
                    if isinstance(csv_freq_exact, str):
                        csv_freq_exact = csv_freq_exact.replace(' ', '').replace(',', '')
                    csv_freq_exact = pd.to_numeric(csv_freq_exact, errors='coerce')
                    if pd.notna(csv_freq_exact):
                        result_df.at[idx, 'frequency_exact'] = int(csv_freq_exact)
                        frequencies_exact_updated += 1
                        updated = True
                except:
                    pass
        
        # Альтернативный способ: поиск по строке
        if not updated and 'keyword' in df.columns:
            matching_rows = df[df['keyword'] == keyword]
            if not matching_rows.empty:
                csv_row = matching_rows.iloc[0]
                
                if 'frequency_world' in df.columns and 'frequency_world' in result_df.columns:
                    csv_freq_world = csv_row.get('frequency_world', 0)
                    try:
                        if isinstance(csv_freq_world, str):
                            csv_freq_world = csv_freq_world.replace(' ', '').replace(',', '')
                        csv_freq_world = pd.to_numeric(csv_freq_world, errors='coerce')
                        if pd.notna(csv_freq_world):
                            result_df.at[idx, 'frequency_world'] = int(csv_freq_world)
                            frequencies_world_updated += 1
                    except:
                        pass
                
                if 'frequency_exact' in df.columns and 'frequency_exact' in result_df.columns:
                    csv_freq_exact = csv_row.get('frequency_exact', 0)
                    try:
                        if isinstance(csv_freq_exact, str):
                            csv_freq_exact = csv_freq_exact.replace(' ', '').replace(',', '')
                        csv_freq_exact = pd.to_numeric(csv_freq_exact, errors='coerce')
                        if pd.notna(csv_freq_exact):
                            result_df.at[idx, 'frequency_exact'] = int(csv_freq_exact)
                            frequencies_exact_updated += 1
                    except:
                        pass
    
    print(f"✓ Обновлено частот:")
    print(f"  - frequency_world: {frequencies_world_updated} запросов")
    print(f"  - frequency_exact: {frequencies_exact_updated} запросов")
    
    # Проверяем частоты перед сохранением
    print(f"\n📊 Проверка частот перед сохранением:")
    if 'frequency_world' in result_df.columns:
        non_zero = (pd.to_numeric(result_df['frequency_world'], errors='coerce') > 0).sum()
        print(f"  ✓ Частота (мир): {non_zero} из {len(result_df)} с ненулевой частотой")
    
    if 'frequency_exact' in result_df.columns:
        non_zero = (pd.to_numeric(result_df['frequency_exact'], errors='coerce') > 0).sum()
        print(f"  ✓ Частота (точная): {non_zero} из {len(result_df)} с ненулевой частотой")
    
    # Сохраняем обновленные данные
    print(f"\n💾 Сохранение обновленных данных в Master DB...")
    try:
        master_db.save_queries(
            group_name=group_name,
            df=result_df,
            csv_path=group.input_file,
            csv_hash=None
        )
        
        print(f"✅ Данные успешно сохранены в Master DB")
        
        # Финальная статистика
        print(f"\n📊 Финальная статистика:")
        stats = master_db.get_statistics(group_name)
        print(f"  ✓ Всего запросов: {stats.get('total_queries', 0):,}")
        print(f"  ✓ С интентом: {stats.get('with_intent', 0):,}")
        print(f"  ✓ С SERP данными: {stats.get('with_serp', 0):,}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = update_frequencies_fotosesiya()
    sys.exit(0 if success else 1)
