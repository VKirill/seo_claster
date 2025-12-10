"""
Очистка битых SERP данных из БД
Удаляет записи с ошибкой "is_commercial_domain is not defined"

⚠️ DEPRECATED: Этот скрипт использует устаревшую БД serp_data.db
Все данные теперь хранятся в master_queries.db (MasterQueryDatabase)
"""

import sys

print("⚠️  ВНИМАНИЕ: Этот скрипт устарел!")
print("   serp_data.db больше не используется.")
print("   Все данные теперь в master_queries.db")
sys.exit(1)

import sqlite3
from pathlib import Path

def fix_serp_database():
    """Удаляет битые записи из БД"""
    
    db_path = "output/serp_data.db"
    if not Path(db_path).exists():
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Считаем битые записи
    cursor.execute("""
        SELECT COUNT(*) as cnt
        FROM serp_results
        WHERE error_message LIKE '%is_commercial_domain%'
    """)
    
    count_before = cursor.fetchone()[0]
    print(f"🔍 Найдено битых записей: {count_before}")
    
    if count_before == 0:
        print("✅ Битых записей не найдено")
        conn.close()
        return
    
    # Удаляем битые записи
    cursor.execute("""
        DELETE FROM serp_results
        WHERE error_message LIKE '%is_commercial_domain%'
    """)
    
    conn.commit()
    
    print(f"✅ Удалено битых записей: {cursor.rowcount}")
    
    # Показываем оставшиеся записи
    cursor.execute("""
        SELECT COUNT(*) as cnt
        FROM serp_results
    """)
    
    total = cursor.fetchone()[0]
    print(f"📊 Осталось записей в БД: {total}")
    
    conn.close()

if __name__ == "__main__":
    fix_serp_database()

