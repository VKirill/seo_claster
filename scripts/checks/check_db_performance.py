"""
Проверка производительности Master Query Database
Показывает какие индексы используются и как быстро работают запросы
"""

from pathlib import Path
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
import time


def print_section(title: str):
    """Красивый вывод секции"""
    print()
    print("=" * 80)
    print(f" {title}")
    print("=" * 80)


def benchmark_query(db: MasterQueryDatabase, query: str, description: str):
    """Тест производительности запроса"""
    import sqlite3
    
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    
    # Прогрев кэша
    cursor.execute(query)
    cursor.fetchall()
    
    # Реальный тест
    start = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    elapsed = (time.time() - start) * 1000  # в миллисекундах
    
    conn.close()
    
    print(f"\n{description}")
    print(f"  ⏱️  Время: {elapsed:.2f} ms")
    print(f"  📊 Результатов: {len(results)}")
    
    return elapsed


def main():
    db_path = Path("output/master_queries.db")
    
    if not db_path.exists():
        print("❌ БД output/master_queries.db не найдена")
        print("   Запустите сначала анализ с сохранением в Master DB")
        return
    
    print_section("📊 Master Query Database Performance Check")
    
    db = MasterQueryDatabase(db_path)
    
    # Статистика БД
    print_section("1. Статистика БД")
    stats = db.get_statistics()
    
    print(f"""
Всего запросов:        {stats['total_queries']:,}
С интентом:            {stats['with_intent']:,} ({stats['with_intent']/stats['total_queries']*100:.1f}%)
С SERP данными:        {stats['with_serp']:,} ({stats['with_serp']/stats['total_queries']*100:.1f}%)
С Yandex Direct:       {stats['with_direct']:,} ({stats['with_direct']/stats['total_queries']*100:.1f}%)
Средний KEI:           {stats['avg_kei']:.2f}
Средний offer_ratio:   {stats['avg_offer_ratio']:.2%}
    """)
    
    # Список индексов
    print_section("2. Индексы")
    indexes = db.get_index_usage_stats()
    
    print(f"\n✓ Создано {len(indexes)} индексов:\n")
    for idx in indexes:
        print(f"  • {idx['index_name']}")
    
    # Тесты производительности
    print_section("3. Тесты производительности")
    
    test_queries = [
        (
            "SELECT * FROM master_queries WHERE group_name = 'видеонаблюдение' LIMIT 100",
            "📌 Поиск по группе (с индексом)"
        ),
        (
            "SELECT * FROM master_queries WHERE main_intent = 'commercial' LIMIT 100",
            "🎯 Фильтр по интенту (с индексом)"
        ),
        (
            "SELECT * FROM master_queries WHERE main_intent = 'commercial' AND frequency_world > 1000 ORDER BY kei DESC LIMIT 50",
            "⚡ Composite query (интент + частота + сортировка KEI)"
        ),
        (
            "SELECT * FROM master_queries WHERE has_geo = 1 AND geo_city = 'Москва' LIMIT 100",
            "🗺️  ГЕО-запросы по городу (с индексом)"
        ),
        (
            "SELECT keyword, kei, serp_offer_ratio FROM master_queries ORDER BY kei DESC LIMIT 100",
            "📊 TOP-100 по KEI (с индексом)"
        ),
        (
            "SELECT AVG(serp_avg_price), COUNT(*) FROM master_queries WHERE is_commercial = 1 AND serp_avg_price IS NOT NULL",
            "💰 Аггрегация цен (с индексом)"
        ),
    ]
    
    total_time = 0
    for query, description in test_queries:
        elapsed = benchmark_query(db, query, description)
        total_time += elapsed
    
    print(f"\n{'='*80}")
    print(f" ИТОГО: {total_time:.2f} ms для всех запросов")
    print(f"{'='*80}")
    
    # EXPLAIN QUERY PLAN для сложного запроса
    print_section("4. План выполнения (EXPLAIN QUERY PLAN)")
    
    complex_query = """
        SELECT keyword, kei, serp_offer_ratio, direct_avg_cpc
        FROM master_queries
        WHERE main_intent = 'commercial'
          AND frequency_world > 1000
          AND serp_offer_ratio > 0.5
        ORDER BY kei DESC
        LIMIT 100
    """
    
    plan = db.analyze_query_performance(complex_query)
    
    print("\nЗапрос:")
    print(complex_query)
    
    print("\nПлан выполнения:")
    for step in plan['execution_plan']:
        indent = "  " * (step['id'] + 1)
        print(f"{indent}• {step['detail']}")
    
    # Рекомендации
    print_section("5. Рекомендации")
    
    if total_time < 100:
        print("\n✅ Отличная производительность! Все запросы < 100ms")
    elif total_time < 500:
        print("\n✓ Хорошая производительность. Запросы < 500ms")
    else:
        print("\n⚠️  Можно улучшить производительность:")
        print("   1. Запустите: python check_db_performance.py --optimize")
        print("   2. Увеличьте cache_size в PRAGMA настройках")
        print("   3. Проверьте что индексы используются")
    
    print("\nКоманды для оптимизации:")
    print("  python check_db_performance.py --optimize   # VACUUM + ANALYZE")
    print("  python check_db_performance.py --reindex    # Пересоздать индексы")


def optimize_db():
    """Оптимизация БД"""
    db_path = Path("output/master_queries.db")
    
    if not db_path.exists():
        print("❌ БД не найдена")
        return
    
    db = MasterQueryDatabase(db_path)
    db.optimize_database()


def reindex_db():
    """Пересоздание индексов"""
    db_path = Path("output/master_queries.db")
    
    if not db_path.exists():
        print("❌ БД не найдена")
        return
    
    db = MasterQueryDatabase(db_path)
    db.rebuild_indexes()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--optimize":
            optimize_db()
        elif sys.argv[1] == "--reindex":
            reindex_db()
        else:
            print("Использование:")
            print("  python check_db_performance.py           # Проверка производительности")
            print("  python check_db_performance.py --optimize  # VACUUM + ANALYZE")
            print("  python check_db_performance.py --reindex   # Пересоздать индексы")
    else:
        main()

