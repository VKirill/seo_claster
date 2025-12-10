"""
Управление кэшем обработанных запросов

⚠️ DEPRECATED: Этот скрипт использует устаревшую БД query_cache.db
Все данные теперь хранятся в master_queries.db (MasterQueryDatabase)
Используйте MasterQueryDatabase напрямую для управления данными.
"""

import sys
import argparse
from pathlib import Path

# QueryCacheDatabase удалён - используйте MasterQueryDatabase
# from seo_analyzer.core.cache import QueryCacheDatabase

print("⚠️  ВНИМАНИЕ: Этот скрипт устарел!")
print("   query_cache.db больше не используется.")
print("   Все данные теперь в master_queries.db")
print("   Используйте MasterQueryDatabase для управления данными.")
sys.exit(1)


def list_cached_groups(cache_db: QueryCacheDatabase):
    """Список всех закэшированных групп"""
    groups = cache_db.get_all_groups()
    
    if not groups:
        print("📭 Кэш пустой - нет закэшированных групп")
        return
    
    print(f"📦 Закэшированных групп: {len(groups)}")
    print("=" * 80)
    
    for group_name in groups:
        stats = cache_db.get_group_stats(group_name)
        if stats:
            print(f"\n📊 Группа: {group_name}")
            print(f"   Файл: {stats['csv_file']}")
            print(f"   Запросов: {stats['unique_queries']:,} (удалено дублей: {stats['duplicates_removed']:,})")
            print(f"   Кэш создан: {stats['imported_at']}")
            print(f"   Hash CSV: {stats['csv_hash'][:16]}...")


def show_group_details(cache_db: QueryCacheDatabase, group_name: str):
    """Подробная информация о группе"""
    stats = cache_db.get_group_stats(group_name)
    
    if not stats:
        print(f"❌ Группа '{group_name}' не найдена в кэше")
        return
    
    print(f"📊 Группа: {group_name}")
    print("=" * 80)
    print(f"Файл CSV:           {stats['csv_file']}")
    print(f"Hash файла:         {stats['csv_hash']}")
    print(f"Всего запросов:     {stats['total_queries']:,}")
    print(f"Уникальных:         {stats['unique_queries']:,}")
    print(f"Удалено дублей:     {stats['duplicates_removed']:,}")
    print(f"Кэш создан:         {stats['imported_at']}")
    print(f"Обновлён:           {stats['updated_at']}")
    
    # Проверяем изменился ли CSV
    csv_path = Path(stats['csv_file'])
    if csv_path.exists():
        csv_changed = cache_db.is_csv_changed(group_name, csv_path)
        if csv_changed:
            print(f"\n⚠️  CSV файл изменился с момента кэширования")
            print(f"   Запустите анализ с --force-refresh для обновления кэша")
        else:
            print(f"\n✅ CSV файл не изменился - кэш актуален")
    else:
        print(f"\n⚠️  CSV файл не найден: {csv_path}")


def clear_group_cache(cache_db: QueryCacheDatabase, group_name: str):
    """Очистка кэша группы"""
    if not cache_db.group_exists(group_name):
        print(f"❌ Группа '{group_name}' не найдена в кэше")
        return
    
    # Подтверждение
    response = input(f"⚠️  Удалить кэш для группы '{group_name}'? (yes/no): ")
    if response.lower() not in ['yes', 'y', 'да']:
        print("❌ Отменено")
        return
    
    cache_db.clear_group_cache(group_name)
    print(f"✅ Кэш группы '{group_name}' очищен")


def clear_all_cache(cache_db: QueryCacheDatabase):
    """Полная очистка кэша"""
    groups = cache_db.get_all_groups()
    
    if not groups:
        print("📭 Кэш уже пустой")
        return
    
    print(f"⚠️  Будут удалены кэши для {len(groups)} групп:")
    for group in groups:
        print(f"   - {group}")
    
    response = input("\nУдалить весь кэш? (yes/no): ")
    if response.lower() not in ['yes', 'y', 'да']:
        print("❌ Отменено")
        return
    
    for group in groups:
        cache_db.clear_group_cache(group)
    
    print(f"✅ Кэш полностью очищен ({len(groups)} групп)")


def show_cache_stats(cache_db: QueryCacheDatabase):
    """Статистика по кэшу"""
    groups = cache_db.get_all_groups()
    db_size = cache_db.get_database_size()
    
    print("📊 Статистика кэша")
    print("=" * 80)
    print(f"Файл БД:            {cache_db.db_path}")
    print(f"Размер БД:          {db_size / 1024 / 1024:.2f} MB")
    print(f"Групп в кэше:       {len(groups)}")
    
    if groups:
        total_queries = 0
        total_duplicates = 0
        
        for group_name in groups:
            stats = cache_db.get_group_stats(group_name)
            if stats:
                total_queries += stats['unique_queries']
                total_duplicates += stats['duplicates_removed']
        
        print(f"Всего запросов:     {total_queries:,}")
        print(f"Удалено дублей:     {total_duplicates:,}")
        
        if total_queries > 0:
            avg_size = (db_size / total_queries) if total_queries > 0 else 0
            print(f"Средний размер:     {avg_size:.2f} bytes/запрос")


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(
        description='Управление кэшем обработанных запросов',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Примеры использования:
  # Список всех закэшированных групп
  python manage_query_cache.py --list

  # Информация о конкретной группе
  python manage_query_cache.py --info скуд

  # Очистка кэша группы
  python manage_query_cache.py --clear скуд

  # Очистка всего кэша
  python manage_query_cache.py --clear-all

  # Статистика по кэшу
  python manage_query_cache.py --stats
        '''
    )
    
    parser.add_argument('--list', action='store_true',
                      help='Список закэшированных групп')
    parser.add_argument('--info', metavar='GROUP',
                      help='Подробная информация о группе')
    parser.add_argument('--clear', metavar='GROUP',
                      help='Очистить кэш группы')
    parser.add_argument('--clear-all', action='store_true',
                      help='Очистить весь кэш')
    parser.add_argument('--stats', action='store_true',
                      help='Статистика по кэшу')
    parser.add_argument('--db', metavar='PATH',
                      help='Путь к БД кэша (по умолчанию output/query_cache.db)')
    
    args = parser.parse_args()
    
    # Если нет аргументов - показываем help
    if not any([args.list, args.info, args.clear, args.clear_all, args.stats]):
        parser.print_help()
        return
    
    # Инициализация БД
    db_path = Path(args.db) if args.db else None
    cache_db = QueryCacheDatabase(db_path)
    
    # Выполнение команд
    if args.list:
        list_cached_groups(cache_db)
    
    if args.info:
        show_group_details(cache_db, args.info)
    
    if args.clear:
        clear_group_cache(cache_db, args.clear)
    
    if args.clear_all:
        clear_all_cache(cache_db)
    
    if args.stats:
        show_cache_stats(cache_db)


if __name__ == '__main__':
    main()

