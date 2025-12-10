"""
Миграция query_cache.db: Добавление колонок интента

Этот скрипт добавляет колонки для кэширования интента в существующую БД query_cache.db
Запускайте если хотите обновить старую БД без полного пересоздания.
"""

import sqlite3
from pathlib import Path


def migrate_query_cache():
    """Добавляет колонки интента в query_cache.db"""
    db_path = Path("output/query_cache.db")
    
    if not db_path.exists():
        print("❌ БД output/query_cache.db не найдена")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Проверяем есть ли уже колонки
    cursor.execute("PRAGMA table_info(cached_queries)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if 'main_intent' in columns:
        print("✓ Колонки интента уже существуют в БД")
        conn.close()
        return
    
    print("🔄 Добавляем колонки интента в query_cache.db...")
    
    try:
        cursor.execute("ALTER TABLE cached_queries ADD COLUMN main_intent TEXT")
        cursor.execute("ALTER TABLE cached_queries ADD COLUMN commercial_score REAL DEFAULT 0.0")
        cursor.execute("ALTER TABLE cached_queries ADD COLUMN informational_score REAL DEFAULT 0.0")
        cursor.execute("ALTER TABLE cached_queries ADD COLUMN navigational_score REAL DEFAULT 0.0")
        
        conn.commit()
        print("✓ Колонки успешно добавлены!")
        print()
        print("Теперь при следующем запуске:")
        print("1. Интент будет классифицирован заново")
        print("2. Результаты сохранятся в БД")
        print("3. Последующие запуски будут использовать кэш")
    
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")
    
    finally:
        conn.close()


if __name__ == "__main__":
    print("=" * 80)
    print("Миграция query_cache.db: Добавление кэширования интента")
    print("=" * 80)
    print()
    
    migrate_query_cache()

