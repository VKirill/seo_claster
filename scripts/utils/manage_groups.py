"""
Скрипт управления группами запросов
"""

import sys
from pathlib import Path
from seo_analyzer.core.query_groups import QueryGroupManager, GroupDatabaseManager


def list_groups():
    """Показать все группы"""
    manager = QueryGroupManager()
    groups = manager.discover_groups()
    
    if not groups:
        print("⚠️  Группы не найдены в semantika/")
        return
    
    print("=" * 80)
    print(f"📁 Доступные группы запросов ({len(groups)}):")
    print("=" * 80)
    print()
    
    groups_info = manager.list_groups()
    
    for info in groups_info:
        status = "✅" if info['file_exists'] else "❌"
        db_status = "💾" if info['db_exists'] else "  "
        queries = info['queries_count'] or "?"
        
        print(f"{status} {db_status} {info['name']:<20} ({queries} запросов)")
        print(f"      Файл: {info['input_file']}")
        print(f"      Output: {info['output_dir']}")
        print()


def show_global_stats():
    """Показать статистику глобальной БД"""
    import sqlite3
    
    db_path = GroupDatabaseManager.GLOBAL_DB_PATH
    
    if not db_path.exists():
        print("⚠️  Глобальная БД не найдена")
        print(f"   Путь: {db_path}")
        return
    
    print("=" * 80)
    print("📊 Глобальная статистика доменов")
    print("=" * 80)
    print()
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Общее количество доменов
        cursor.execute("SELECT COUNT(*) FROM domain_global_stats")
        total_domains = cursor.fetchone()[0]
        
        # Коммерческие домены
        cursor.execute("""
            SELECT COUNT(*) FROM domain_global_stats 
            WHERE is_commercial = 1
        """)
        commercial_count = cursor.fetchone()[0]
        
        # Информационные домены
        cursor.execute("""
            SELECT COUNT(*) FROM domain_global_stats 
            WHERE is_commercial = 0
        """)
        info_count = cursor.fetchone()[0]
        
        print(f"Всего доменов: {total_domains}")
        print(f"Коммерческих: {commercial_count}")
        print(f"Информационных: {info_count}")
        print()
        
        # Топ-10 коммерческих доменов
        print("Топ-10 коммерческих доменов:")
        cursor.execute("""
            SELECT 
                domain, 
                total_queries, 
                commercial_ratio,
                groups_count
            FROM domain_global_stats
            WHERE is_commercial = 1
            ORDER BY total_queries DESC
            LIMIT 10
        """)
        
        for row in cursor.fetchall():
            domain, queries, ratio, groups = row
            print(f"  {domain:<30} {queries:>5} запросов, {ratio*100:>5.1f}% коммерц., {groups} групп")
        
        print()
        
        # Топ-10 информационных доменов
        print("Топ-10 информационных доменов:")
        cursor.execute("""
            SELECT 
                domain, 
                total_queries, 
                commercial_ratio,
                groups_count
            FROM domain_global_stats
            WHERE is_commercial = 0
            ORDER BY total_queries DESC
            LIMIT 10
        """)
        
        for row in cursor.fetchall():
            domain, queries, ratio, groups = row
            print(f"  {domain:<30} {queries:>5} запросов, {ratio*100:>5.1f}% коммерц., {groups} групп")


def show_domain_info(domain: str):
    """Показать информацию о домене"""
    import sqlite3
    
    db_path = GroupDatabaseManager.GLOBAL_DB_PATH
    
    if not db_path.exists():
        print("⚠️  Глобальная БД не найдена")
        return
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Агрегированные данные
        cursor.execute("""
            SELECT * FROM domain_global_stats
            WHERE domain = ?
        """, (domain,))
        
        agg_row = cursor.fetchone()
        
        if not agg_row:
            print(f"⚠️  Домен '{domain}' не найден в глобальной БД")
            return
        
        print("=" * 80)
        print(f"📊 Информация о домене: {domain}")
        print("=" * 80)
        print()
        
        print("Агрегированная статистика:")
        print(f"  Классификация: {'Коммерческий' if agg_row['is_commercial'] else 'Информационный'}")
        print(f"  Коммерциализация: {agg_row['commercial_ratio']*100:.1f}%")
        print(f"  Confidence: {agg_row['confidence_score']:.2f}")
        print(f"  Всего запросов: {agg_row['total_queries']}")
        print(f"  Коммерческих: {agg_row['total_commercial']}")
        print(f"  Информационных: {agg_row['total_informational']}")
        print(f"  Групп: {agg_row['groups_count']}")
        print()
        
        # Статистика по группам
        print("Статистика по группам:")
        cursor.execute("""
            SELECT * FROM domain_group_stats
            WHERE domain = ?
            ORDER BY total_queries DESC
        """, (domain,))
        
        for row in cursor.fetchall():
            print(f"  {row['query_group']:<20} "
                  f"{row['total_queries']:>4} запросов "
                  f"(К: {row['commercial_count']}, И: {row['informational_count']})")


def main():
    """Главная функция"""
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python manage_groups.py list              # Показать все группы")
        print("  python manage_groups.py stats             # Глобальная статистика доменов")
        print("  python manage_groups.py domain <domain>   # Информация о домене")
        return
    
    command = sys.argv[1]
    
    if command == "list":
        list_groups()
    elif command == "stats":
        show_global_stats()
    elif command == "domain" and len(sys.argv) >= 3:
        show_domain_info(sys.argv[2])
    else:
        print(f"❌ Неизвестная команда: {command}")


if __name__ == "__main__":
    main()

