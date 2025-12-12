"""
Скрипт для пересохранения частот из CSV файла в Master DB
Используется когда частоты в БД равны нулю, но есть в CSV
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


def refill_frequencies_from_csv(group_name: str):
    """
    Пересохраняет частоты из CSV файла в Master DB
    
    Args:
        group_name: Название группы
    """
    print(f"🔄 Пересохранение частот для группы '{group_name}' из CSV...")
    print("=" * 80)
    
    # Инициализируем менеджер групп
    group_manager = QueryGroupManager()
    group_manager.discover_groups()
    
    # Получаем группу
    group = group_manager.get_group(group_name)
    if not group:
        print(f"❌ Группа '{group_name}' не найдена")
        return
    
    if not group.input_file.exists():
        print(f"❌ CSV файл не найден: {group.input_file}")
        return
    
    # Загружаем данные из CSV
    print(f"📂 Загрузка CSV: {group.input_file}")
    raw_df = load_csv_data(group.input_file)
    
    if raw_df.empty:
        print("❌ CSV файл пустой")
        return
    
    print(f"✓ Загружено {len(raw_df)} запросов из CSV")
    
    # Нормализуем колонки (обрабатывает частоты с пробелами)
    df = normalize_dataframe_columns(raw_df)
    
    # Проверяем частоты в CSV
    if 'frequency_world' in df.columns:
        non_zero_freq_world = (df['frequency_world'] > 0).sum()
        print(f"✓ Частота (мир) в CSV: {non_zero_freq_world} из {len(df)} с ненулевой частотой")
    
    if 'frequency_exact' in df.columns:
        non_zero_freq_exact = (df['frequency_exact'] > 0).sum()
        print(f"✓ Частота (точная) в CSV: {non_zero_freq_exact} из {len(df)} с ненулевой частотой")
    
    # Загружаем существующие данные из БД
    master_db = MasterQueryDatabase()
    existing_df = master_db.load_queries(group_name, include_serp_urls=False)
    
    if existing_df is not None and not existing_df.empty:
        print(f"\n📦 Существующие данные в БД: {len(existing_df)} запросов")
        
        # ВАЖНО: Создаем DataFrame где частоты из CSV имеют приоритет
        # Объединяем: частоты из CSV, остальное из БД
        
        # Создаем индекс по keyword для быстрого поиска
        existing_df_indexed = existing_df.set_index('keyword')
        df_indexed = df.set_index('keyword') if 'keyword' in df.columns else None
        
        # Создаем результирующий DataFrame на основе данных из БД
        result_df = existing_df.copy()
        
        # Обновляем частоты из CSV
        frequencies_updated = 0
        for idx, row in df.iterrows():
            keyword = row.get('keyword')
            if keyword:
                # Находим индекс в result_df
                matching_rows = result_df[result_df['keyword'] == keyword]
                if not matching_rows.empty:
                    result_idx = matching_rows.index[0]
                    
                    # Обновляем частоты из CSV (даже если они нулевые - это приоритет CSV)
                    if 'frequency_world' in df.columns and 'frequency_world' in result_df.columns:
                        csv_freq_world = row.get('frequency_world', 0)
                        if pd.notna(csv_freq_world):
                            result_df.at[result_idx, 'frequency_world'] = csv_freq_world
                            frequencies_updated += 1
                    
                    if 'frequency_exact' in df.columns and 'frequency_exact' in result_df.columns:
                        csv_freq_exact = row.get('frequency_exact', 0)
                        if pd.notna(csv_freq_exact):
                            result_df.at[result_idx, 'frequency_exact'] = csv_freq_exact
        
        print(f"✓ Обновлено частот: {frequencies_updated}")
        
        # Проверяем частоты перед сохранением
        if 'frequency_world' in result_df.columns:
            non_zero = (result_df['frequency_world'] > 0).sum()
            print(f"✓ Частоты перед сохранением: {non_zero} из {len(result_df)} с ненулевой частотой (мир)")
        
        if 'frequency_exact' in result_df.columns:
            non_zero = (result_df['frequency_exact'] > 0).sum()
            print(f"✓ Частоты перед сохранением: {non_zero} из {len(result_df)} с ненулевой частотой (точная)")
        
        # Сохраняем обновленные данные
        print(f"\n💾 Сохранение обновленных данных в БД...")
        master_db.save_queries(
            group_name=group_name,
            df=result_df,
            csv_path=group.input_file,
            csv_hash=None
        )
        
        print(f"✓ Данные сохранены")
    else:
        print(f"\n⚠️  Группа '{group_name}' не найдена в БД")
        print(f"💡 Запустите полный анализ для создания записи в БД")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python refill_frequencies_from_csv.py <group_name>")
        print("Пример: python refill_frequencies_from_csv.py фотосессия")
        sys.exit(1)
    
    group_name = sys.argv[1]
    refill_frequencies_from_csv(group_name)
