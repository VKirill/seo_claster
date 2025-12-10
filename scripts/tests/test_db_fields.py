"""
Тестовый скрипт для проверки заполняемости полей в БД
Создает тестовый CSV с 10 запросами и запускает pipeline
"""

import asyncio
import pandas as pd
import sqlite3
from pathlib import Path
import sys
import os
import io

# Устанавливаем UTF-8 для вывода
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Добавляем корневую директорию в путь
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pipeline.analyzer import SEOAnalyzer
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase


def create_test_csv(num_queries: int = 10) -> Path:
    """Создать тестовый CSV файл с запросами"""
    test_queries = [
        'купить холодильник москва',
        'холодильник цена',
        'холодильник отзывы',
        'холодильник недорого',
        'где купить холодильник',
        'холодильник двухкамерный',
        'холодильник lg отзывы',
        'холодильник индезит цена',
        'холодильник с морозилкой',
        'холодильник для дома'
    ]
    
    # Берем только нужное количество
    test_queries = test_queries[:num_queries]
    
    # Создаем DataFrame
    df = pd.DataFrame({
        'Запрос': test_queries,
        'frequency_world': [1200, 800, 500, 450, 350, 600, 400, 300, 550, 250],
        'frequency_exact': [950, 650, 400, 350, 280, 480, 320, 240, 440, 200]
    })
    
    # Сохраняем в semantika/test_db_fields.csv
    test_dir = Path('semantika')
    test_dir.mkdir(exist_ok=True)
    
    test_file = test_dir / 'test_db_fields.csv'
    df.to_csv(test_file, index=False, encoding='utf-8-sig', sep=';')
    
    print(f"✅ Создан тестовый CSV: {test_file}")
    print(f"   Запросов: {len(test_queries)}")
    
    return test_file


async def run_test_analysis(test_file: Path):
    """Запустить анализ на тестовом файле"""
    print("\n" + "=" * 80)
    print("🚀 ЗАПУСК ТЕСТОВОГО АНАЛИЗА")
    print("=" * 80)
    
    # Создаем аргументы для анализатора
    class Args:
        def __init__(self):
            self.input_file = str(test_file)
            self.group = 'test_db_fields'
            self.skip_embeddings = True
            self.skip_graph = True
            self.skip_topics = True
            self.skip_hierarchical = True
            self.skip_forms = True
            self.skip_yandex_direct = True
            self.xmlstock_api_key = None
            self.serp_batch_async = True
            self.enable_graph = False  # Для обратной совместимости
            
            # Пробуем получить API ключ из config_local
            try:
                import config_local
                self.xmlstock_api_key = getattr(config_local, 'XMLSTOCK_API_KEY', None)
            except ImportError:
                pass
            
            # Если нет в config_local - пробуем переменную окружения
            if not self.xmlstock_api_key:
                self.xmlstock_api_key = os.getenv('XMLSTOCK_API_KEY')
    
    args = Args()
    
    # Создаем анализатор
    analyzer = SEOAnalyzer(args)
    
    # Запускаем pipeline
    try:
        await analyzer.run()
        print("\n✅ Анализ завершен успешно!")
        return True
    except Exception as e:
        print(f"\n⚠️  Ошибка при анализе: {e}")
        print("   Продолжаем проверку БД...")
        import traceback
        traceback.print_exc()
        
        # Пытаемся сохранить данные вручную, если они есть в DataFrame
        if hasattr(analyzer, 'df') and analyzer.df is not None and len(analyzer.df) > 0:
            print("\n💾 Попытка сохранить данные вручную...")
            try:
                from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
                master_db = MasterQueryDatabase()
                
                group_name = 'test_db_fields'
                csv_path = test_file
                
                master_db.save_queries(
                    group_name=group_name,
                    df=analyzer.df,
                    csv_path=csv_path,
                    csv_hash=None
                )
                print("✅ Данные сохранены вручную!")
            except Exception as save_error:
                print(f"❌ Ошибка при сохранении: {save_error}")
        
        return True  # Возвращаем True чтобы проверить БД


def check_db_fields(group_name: str = 'test_db_fields'):
    """Проверить заполняемость полей в БД"""
    print("\n" + "=" * 80)
    print("📊 ПРОВЕРКА ЗАПОЛНЯЕМОСТИ ПОЛЕЙ В БД")
    print("=" * 80)
    
    master_db = MasterQueryDatabase()
    db_path = master_db.db_path
    
    if not db_path.exists():
        print(f"❌ БД не найдена: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Проверяем наличие группы
    cursor.execute('''
        SELECT COUNT(*) FROM master_queries WHERE group_name = ?
    ''', (group_name,))
    
    count = cursor.fetchone()[0]
    
    if count == 0:
        print(f"❌ Группа '{group_name}' не найдена в БД")
        conn.close()
        return
    
    print(f"\n✅ Найдено запросов в группе '{group_name}': {count}\n")
    
    # Список полей для проверки
    fields_to_check = {
        'Геолокация': [
            'geo_type',
            'geo_country',
            'geo_city',
        ],
        'Интент': [
            'main_intent',
        ],
        'SERP базовые': [
            'serp_req_id',
            'serp_status',
            'serp_found_docs',
            'serp_titles_with_keyword',
        ],
        'SERP offer info': [
            'serp_docs_with_offers',
            'serp_total_docs',
            'serp_offer_ratio',
            'serp_avg_price',
            'serp_min_price',
            'serp_max_price',
            'serp_median_price',
            'serp_offers_count',
            'serp_offers_with_discount',
            'serp_avg_discount_percent',
        ]
    }
    
    # Проверяем каждое поле
    results = {}
    
    for category, fields in fields_to_check.items():
        print(f"\n📋 {category}:")
        results[category] = {}
        
        for field in fields:
            cursor.execute(f'''
                SELECT 
                    COUNT(*) as total,
                    COUNT({field}) as filled,
                    COUNT(CASE WHEN {field} IS NOT NULL AND {field} != '' AND {field} != 0 THEN 1 END) as non_empty
                FROM master_queries
                WHERE group_name = ?
            ''', (group_name,))
            
            row = cursor.fetchone()
            total, filled, non_empty = row
            
            percentage = (non_empty / total * 100) if total > 0 else 0
            
            status = "✅" if non_empty > 0 else "❌"
            results[category][field] = {
                'total': total,
                'filled': filled,
                'non_empty': non_empty,
                'percentage': percentage
            }
            
            print(f"  {status} {field:35} {non_empty:3}/{total:3} ({percentage:5.1f}%)")
    
    # Выводим примеры записей
    print("\n" + "=" * 80)
    print("📝 ПРИМЕРЫ ЗАПИСЕЙ:")
    print("=" * 80)
    
    cursor.execute('''
        SELECT 
            keyword,
            main_intent,
            geo_city,
            geo_country,
            serp_req_id,
            serp_status,
            serp_found_docs,
            serp_avg_price,
            serp_offer_ratio
        FROM master_queries
        WHERE group_name = ?
        LIMIT 5
    ''', (group_name,))
    
    rows = cursor.fetchall()
    
    if rows:
        print(f"\n{'Запрос':<30} {'Интент':<15} {'Город':<15} {'Страна':<10} {'req_id':<12} {'Статус':<12} {'Доков':<8} {'Цена':<10} {'Offer%':<8}")
        print("-" * 130)
        
        for row in rows:
            keyword, intent, city, country, req_id, status, docs, price, ratio = row
            keyword = (keyword[:27] + '...') if keyword and len(keyword) > 30 else (keyword or '')
            intent = intent or ''
            city = city or ''
            country = country or ''
            req_id = (req_id[:10] + '..') if req_id and len(req_id) > 12 else (req_id or '')
            status = status or ''
            docs = str(docs) if docs else ''
            price = f"{price:.0f}" if price else ''
            ratio = f"{ratio:.1%}" if ratio else ''
            
            print(f"{keyword:<30} {intent:<15} {city:<15} {country:<10} {req_id:<12} {status:<12} {docs:<8} {price:<10} {ratio:<8}")
    
    conn.close()
    
    # Итоговая статистика
    print("\n" + "=" * 80)
    print("📊 ИТОГОВАЯ СТАТИСТИКА:")
    print("=" * 80)
    
    total_fields = sum(len(fields) for fields in fields_to_check.values())
    filled_fields = sum(
        sum(1 for field_data in category_data.values() if field_data['non_empty'] > 0)
        for category_data in results.values()
    )
    
    print(f"\n✅ Заполнено полей: {filled_fields}/{total_fields} ({filled_fields/total_fields*100:.1f}%)")
    
    # Проверяем критические поля
    critical_fields = ['main_intent', 'geo_city', 'serp_found_docs', 'serp_avg_price']
    critical_filled = sum(
        1 for category_data in results.values()
        for field, field_data in category_data.items()
        if field in critical_fields and field_data['non_empty'] > 0
    )
    
    print(f"✅ Критические поля заполнены: {critical_filled}/{len(critical_fields)}")
    
    if critical_filled == len(critical_fields):
        print("\n🎉 ВСЕ КРИТИЧЕСКИЕ ПОЛЯ ЗАПОЛНЕНЫ!")
    else:
        print(f"\n⚠️  Не заполнено критических полей: {len(critical_fields) - critical_filled}")


async def main():
    """Главная функция"""
    print("=" * 80)
    print("ТЕСТ ЗАПОЛНЯЕМОСТИ ПОЛЕЙ В БД")
    print("=" * 80)
    
    # Создаем тестовый CSV
    test_file = create_test_csv(num_queries=10)
    
    # Запускаем анализ
    success = await run_test_analysis(test_file)
    
    if success:
        # Проверяем заполняемость полей
        check_db_fields('test_db_fields')
    else:
        print("\n❌ Анализ не завершен, проверка БД пропущена")
    
    print("\n" + "=" * 80)
    print("ТЕСТ ЗАВЕРШЕН")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

