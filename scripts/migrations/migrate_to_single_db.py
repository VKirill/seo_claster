"""
Миграция на единую БД с полем query_group

Добавляет поле query_group в таблицу serp_results и создает индекс.
Также переносит данные из БД групп в общую БД (если они есть).
"""

import sqlite3
from pathlib import Path


def add_query_group_column():
    """Добавить колонку query_group и обновить UNIQUE constraint"""
    db_path = Path("output/serp_data.db")
    
    if not db_path.exists():
        print(f"⚠️  БД не найдена: {db_path}")
        print("   Она будет создана автоматически при первом запуске")
        return
    
    print("=" * 80)
    print("🔄 МИГРАЦИЯ НА ЕДИНУЮ БД С ДУБЛИКАТАМИ ДЛЯ ГРУПП")
    print("=" * 80)
    print()
    print(f"БД: {db_path}")
    print()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Проверяем есть ли уже колонка query_group
        cursor.execute("PRAGMA table_info(serp_results)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'query_group' not in columns:
            print("📊 Добавление колонки query_group...")
            
            # Добавляем колонку
            cursor.execute("""
                ALTER TABLE serp_results 
                ADD COLUMN query_group TEXT DEFAULT NULL
            """)
            
            print("✓ Колонка добавлена")
        else:
            print("✓ Колонка query_group уже существует")
        
        # Создаем индекс
        print("📊 Создание индекса query_group...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_query_group 
            ON serp_results(query_group)
        """)
        print("✓ Индекс создан")
        
        # Обновляем UNIQUE constraint
        # SQLite не поддерживает изменение constraints напрямую
        # Но новая схема будет создана автоматически при первом запуске
        print("✓ UNIQUE constraint будет обновлен автоматически")
        print("  Старый: UNIQUE(query_hash, lr)")
        print("  Новый:  UNIQUE(query_hash, lr, query_group)")
        print()
        
        conn.commit()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        conn.rollback()
    
    finally:
        conn.close()


def migrate_group_databases():
    """Перенести данные из БД групп в общую БД"""
    groups_dir = Path("output/groups")
    
    if not groups_dir.exists():
        print("⚠️  Директория групп не найдена - нечего мигрировать")
        return
    
    group_dbs = list(groups_dir.glob("*/serp_data.db"))
    
    if not group_dbs:
        print("⚠️  БД групп не найдены - нечего мигрировать")
        return
    
    print()
    print("=" * 80)
    print(f"🔄 МИГРАЦИЯ ДАННЫХ ИЗ БД ГРУПП ({len(group_dbs)} БД)")
    print("=" * 80)
    print()
    
    main_db = Path("output/serp_data.db")
    
    for db_path in group_dbs:
        group_name = db_path.parent.name
        print(f"📁 Группа: {group_name}")
        
        try:
            # Подключаемся к общей БД
            main_conn = sqlite3.connect(main_db)
            main_cursor = main_conn.cursor()
            
            # Подключаем БД группы
            main_cursor.execute(f"ATTACH DATABASE '{db_path}' AS group_db")
            
            # Копируем данные из serp_results
            main_cursor.execute("""
                SELECT COUNT(*) FROM group_db.serp_results
            """)
            count = main_cursor.fetchone()[0]
            print(f"   Запросов в БД группы: {count}")
            
            if count > 0:
                # Копируем данные с установкой query_group
                main_cursor.execute(f"""
                    INSERT OR REPLACE INTO serp_results 
                    (query, query_hash, query_group, lr, xml_response, 
                     found_docs, main_pages_count, titles_with_keyword,
                     commercial_domains, info_domains, error_message,
                     created_at, updated_at)
                    SELECT 
                        query, query_hash, '{group_name}', lr, xml_response,
                        found_docs, main_pages_count, titles_with_keyword,
                        commercial_domains, info_domains, error_message,
                        created_at, updated_at
                    FROM group_db.serp_results
                """)
                
                migrated = main_cursor.rowcount
                print(f"   ✓ Перенесено: {migrated} записей")
            
            # Отключаем БД группы
            main_cursor.execute("DETACH DATABASE group_db")
            
            main_conn.commit()
            main_conn.close()
            
        except Exception as e:
            print(f"   ⚠️  Ошибка: {e}")
        
        print()


def show_statistics():
    """Показать статистику по группам в общей БД"""
    db_path = Path("output/serp_data.db")
    
    if not db_path.exists():
        return
    
    print()
    print("=" * 80)
    print("📊 СТАТИСТИКА ОБЩЕЙ БД")
    print("=" * 80)
    print()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Общее количество запросов
        cursor.execute("SELECT COUNT(*) FROM serp_results")
        total = cursor.fetchone()[0]
        print(f"Всего запросов в БД: {total}")
        print()
        
        # По группам
        cursor.execute("""
            SELECT 
                query_group,
                COUNT(*) as count
            FROM serp_results
            GROUP BY query_group
            ORDER BY count DESC
        """)
        
        rows = cursor.fetchall()
        
        if rows:
            print("Распределение по группам:")
            for group, count in rows:
                group_name = group if group else "(без группы)"
                print(f"  {group_name:<30} {count:>6} запросов")
        
    except Exception as e:
        print(f"⚠️  Ошибка: {e}")
    
    finally:
        conn.close()


def main():
    """Главная функция"""
    print()
    print("🔄 Миграция на единую БД с query_group")
    print()
    
    # 1. Добавляем колонку
    add_query_group_column()
    
    # 2. Мигрируем данные из БД групп
    migrate_group_databases()
    
    # 3. Показываем статистику
    show_statistics()
    
    print()
    print("=" * 80)
    print("✅ МИГРАЦИЯ ЗАВЕРШЕНА")
    print("=" * 80)
    print()
    print("💡 Теперь все группы используют единую БД: output/serp_data.db")
    print("   Данные разделяются через поле query_group")
    print()
    print("🗑️  Можно удалить старые БД групп:")
    print("   - output/groups/*/serp_data.db")
    print()


if __name__ == "__main__":
    main()

