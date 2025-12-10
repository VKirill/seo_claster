"""Удаляет тестовый запрос из БД для пересбора с новой логикой"""

import sqlite3
from pathlib import Path

db_path = "output/serp_data.db"
if Path(db_path).exists():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM serp_results WHERE LOWER(query) = 'опс скуд'")
    conn.commit()
    
    print(f"✅ Удалено записей: {cursor.rowcount}")
    conn.close()
else:
    print(f"❌ БД не найдена")

