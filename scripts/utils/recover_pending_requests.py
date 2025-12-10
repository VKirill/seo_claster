"""
Утилита для восстановления незавершённых SERP запросов

Использование:
  python recover_pending_requests.py
  
Описание:
  Находит все запросы со статусом 'pending' (есть req_id, но нет результатов)
  и пытается получить результаты от XMLStock по req_id

⚠️ DEPRECATED: Этот скрипт использует устаревшую БД serp_data.db
Все данные теперь хранятся в master_queries.db (MasterQueryDatabase)
Используйте MasterQueryDatabase.get_pending_serp_queries() для получения pending запросов.
"""

import sys

print("⚠️  ВНИМАНИЕ: Этот скрипт устарел!")
print("   serp_data.db больше не используется.")
print("   Все данные теперь в master_queries.db")
print("   Используйте MasterQueryDatabase.get_pending_serp_queries()")
sys.exit(1)

import asyncio
import aiohttp
from pathlib import Path
import sqlite3
from datetime import datetime

import config_local


async def recover_pending():
    """Восстановить незавершённые запросы"""
    
    db_path = Path("output/serp_data.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Находим все pending запросы
    cursor.execute("""
        SELECT id, query, req_id, query_group, lr 
        FROM serp_results
        WHERE status = 'pending' AND req_id IS NOT NULL
    """)
    
    pending = cursor.fetchall()
    
    print(f"🔍 Найдено незавершённых запросов: {len(pending)}")
    
    if len(pending) == 0:
        print("✅ Нет незавершённых запросов")
        conn.close()
        return
    
    # Парсим API ключ
    if ':' in config_local.XMLSTOCK_API_KEY:
        user, key = config_local.XMLSTOCK_API_KEY.split(':', 1)
    else:
        user = key = config_local.XMLSTOCK_API_KEY
    
    url = "https://xmlstock.com/yandex/xml/"
    
    recovered = 0
    still_pending = 0
    errors = 0
    
    async with aiohttp.ClientSession() as session:
        for row_id, query, req_id, group, lr in pending:
            print(f"\n🔄 [{query[:50]}...] req_id={req_id}")
            
            params = {
                'user': user,
                'key': key,
                'req_id': req_id
            }
            
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    xml_text = await response.text()
                    
                    # Проверяем статус
                    if 'code="202"' in xml_text or 'не обработан' in xml_text:
                        print(f"  ⏳ Ещё не готов (202)")
                        still_pending += 1
                        continue
                    
                    if '<error' in xml_text:
                        print(f"  ❌ Ошибка: {xml_text[:100]}")
                        cursor.execute("""
                            UPDATE serp_results 
                            SET status = 'failed', error_message = ?, updated_at = ?
                            WHERE id = ?
                        """, (xml_text[:500], datetime.now(), row_id))
                        errors += 1
                        continue
                    
                    # Успешно получили результат!
                    print(f"  ✅ Результат получен!")
                    
                    # Обновляем запись
                    cursor.execute("""
                        UPDATE serp_results 
                        SET xml_response = ?, status = 'completed', updated_at = ?
                        WHERE id = ?
                    """, (xml_text, datetime.now(), row_id))
                    
                    recovered += 1
                    
            except Exception as e:
                print(f"  ⚠️ Ошибка: {e}")
                errors += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n📊 Итого:")
    print(f"  ✅ Восстановлено: {recovered}")
    print(f"  ⏳ Ещё pending: {still_pending}")
    print(f"  ❌ Ошибок: {errors}")


if __name__ == '__main__':
    asyncio.run(recover_pending())

