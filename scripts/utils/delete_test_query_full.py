"""Удаляет тестовый запрос из БД полностью (включая documents)"""

import sqlite3
from pathlib import Path

db_path = "output/serp_data.db"
if Path(db_path).exists():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Сначала найдём ID
    cursor.execute("SELECT id FROM serp_results WHERE LOWER(query) = 'опс скуд'")
    row = cursor.fetchone()
    
    if row:
        serp_id = row[0]
        
        # Удаляем documents
        cursor.execute("DELETE FROM serp_documents WHERE serp_result_id = ?", (serp_id,))
        print(f"✅ Удалено documents: {cursor.rowcount}")
        
        # Удаляем serp_result
        cursor.execute("DELETE FROM serp_results WHERE id = ?", (serp_id,))
        print(f"✅ Удалено serp_results: {cursor.rowcount}")
        
        conn.commit()
    else:
        print("❓ Запрос не найден в БД")
    
    conn.close()
else:
    print(f"❌ БД не найдена")

