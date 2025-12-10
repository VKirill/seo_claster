"""
Миграция данных Yandex Direct из старой БД в единую serp_data.db

Переносит данные из yandex_direct_cache.db → serp_data.db
"""

import sqlite3
import os
from pathlib import Path


def migrate_direct_cache():
    """Миграция данных Direct в единую БД."""
    
    old_db = "yandex_direct_cache.db"
    new_db = "serp_data.db"
    
    # Проверяем наличие старой БД
    if not Path(old_db).exists():
        print(f"✓ Старая БД {old_db} не найдена, миграция не требуется")
        return
    
    print(f"📦 Миграция данных Yandex Direct: {old_db} → {new_db}")
    print("-" * 60)
    
    # Подключаемся к обеим БД
    old_conn = sqlite3.connect(old_db)
    new_conn = sqlite3.connect(new_db)
    
    old_cursor = old_conn.cursor()
    new_cursor = new_conn.cursor()
    
    # Создаём таблицу в новой БД если её нет
    new_cursor.execute("""
        CREATE TABLE IF NOT EXISTS direct_forecasts (
            phrase TEXT PRIMARY KEY,
            geo_id INTEGER,
            shows INTEGER,
            clicks INTEGER,
            ctr REAL,
            premium_ctr REAL,
            min_cpc REAL,
            avg_cpc REAL,
            max_cpc REAL,
            recommended_cpc REAL,
            competition_level TEXT,
            first_place_bid REAL,
            first_place_price REAL,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    
    # Создаём индексы
    new_cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_phrase 
        ON direct_forecasts(phrase)
    """)
    new_cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_updated_at 
        ON direct_forecasts(updated_at)
    """)
    
    # Получаем данные из старой БД
    old_cursor.execute("SELECT COUNT(*) FROM direct_forecasts")
    total = old_cursor.fetchone()[0]
    
    if total == 0:
        print("✓ Нет данных для миграции")
        old_conn.close()
        new_conn.close()
        return
    
    print(f"📊 Найдено записей: {total}")
    
    # Копируем данные
    old_cursor.execute("""
        SELECT phrase, geo_id, shows, clicks, ctr, premium_ctr,
               min_cpc, avg_cpc, max_cpc, recommended_cpc,
               competition_level, first_place_bid, first_place_price,
               created_at, updated_at
        FROM direct_forecasts
    """)
    
    migrated = 0
    skipped = 0
    
    for row in old_cursor.fetchall():
        try:
            new_cursor.execute("""
                INSERT OR REPLACE INTO direct_forecasts 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
            migrated += 1
        except Exception as e:
            print(f"⚠️  Ошибка при миграции записи: {e}")
            skipped += 1
    
    new_conn.commit()
    
    print(f"✅ Миграция завершена:")
    print(f"  • Перенесено: {migrated}")
    if skipped > 0:
        print(f"  • Пропущено: {skipped}")
    
    # Закрываем соединения
    old_conn.close()
    new_conn.close()
    
    # Информируем об успешной миграции
    print()
    print(f"ℹ️  Старый файл {old_db} можно удалить вручную")
    print(f"   Все данные теперь в единой БД: {new_db}")


if __name__ == "__main__":
    migrate_direct_cache()

