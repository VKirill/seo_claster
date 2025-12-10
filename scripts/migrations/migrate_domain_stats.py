"""
Миграция данных из global_domain_stats.db в serp_data.db

Этот скрипт переносит данные из старой БД в новую единую структуру.
"""

import sqlite3
from pathlib import Path


def migrate_domain_stats():
    """Перенос данных из global_domain_stats.db в serp_data.db"""
    
    old_db = Path("output/global_domain_stats.db")
    new_db = Path("output/serp_data.db")
    
    if not old_db.exists():
        print(f"⚠️  Старая БД не найдена: {old_db}")
        print("   Нечего мигрировать")
        return
    
    if not new_db.exists():
        print(f"⚠️  Новая БД не найдена: {new_db}")
        print("   Запустите анализ чтобы создать БД")
        return
    
    print("=" * 80)
    print("🔄 МИГРАЦИЯ ДАННЫХ ДОМЕНОВ")
    print("=" * 80)
    print()
    print(f"Из: {old_db}")
    print(f"В:  {new_db}")
    print()
    
    # Подключаемся к обеим БД
    old_conn = sqlite3.connect(old_db)
    new_conn = sqlite3.connect(new_db)
    
    try:
        old_cursor = old_conn.cursor()
        new_cursor = new_conn.cursor()
        
        # 1. Миграция domain_stats -> domain_group_stats
        print("📊 Миграция статистики доменов по группам...")
        
        old_cursor.execute("SELECT * FROM domain_stats")
        rows = old_cursor.fetchall()
        
        print(f"   Найдено записей: {len(rows)}")
        
        migrated = 0
        for row in rows:
            # row = (id, domain, query_group, commercial_count, informational_count, 
            #        total_queries, first_seen, last_updated)
            try:
                new_cursor.execute("""
                    INSERT OR REPLACE INTO domain_group_stats
                        (domain, query_group, commercial_count, informational_count,
                         total_queries, first_seen, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
                migrated += 1
            except Exception as e:
                print(f"   ⚠️  Ошибка при миграции {row[1]}: {e}")
        
        print(f"   ✓ Перенесено: {migrated} записей")
        print()
        
        # 2. Миграция domain_aggregated_stats -> domain_global_stats
        print("📊 Миграция агрегированной статистики...")
        
        old_cursor.execute("SELECT * FROM domain_aggregated_stats")
        rows = old_cursor.fetchall()
        
        print(f"   Найдено записей: {len(rows)}")
        
        migrated = 0
        for row in rows:
            # row = (domain, total_commercial, total_informational, total_queries,
            #        groups_count, is_commercial, commercial_ratio, confidence_score, last_updated)
            try:
                new_cursor.execute("""
                    INSERT OR REPLACE INTO domain_global_stats
                        (domain, total_commercial, total_informational, total_queries,
                         groups_count, is_commercial, commercial_ratio, confidence_score, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
                migrated += 1
            except Exception as e:
                print(f"   ⚠️  Ошибка при миграции {row[0]}: {e}")
        
        print(f"   ✓ Перенесено: {migrated} записей")
        print()
        
        # Коммит изменений
        new_conn.commit()
        
        print("=" * 80)
        print("✅ МИГРАЦИЯ ЗАВЕРШЕНА")
        print("=" * 80)
        print()
        
        # Показываем статистику новой БД
        print("📊 Статистика новой БД:")
        new_cursor.execute("SELECT COUNT(*) FROM domain_group_stats")
        print(f"   domain_group_stats: {new_cursor.fetchone()[0]} записей")
        
        new_cursor.execute("SELECT COUNT(*) FROM domain_global_stats")
        print(f"   domain_global_stats: {new_cursor.fetchone()[0]} записей")
        print()
        
        # Предлагаем удалить старую БД
        print("💡 Теперь можно удалить старую БД:")
        print(f"   del {old_db}")
        print()
        
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        old_conn.close()
        new_conn.close()


if __name__ == "__main__":
    migrate_domain_stats()

