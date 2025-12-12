"""
Сброс застрявших запросов в статусе 'processing'
Нужен когда req_id устарели (ошибка 203)
"""

import sqlite3
import sys
from datetime import datetime, timedelta

def reset_stuck_processing(group_name: str, hours_old: int = 1):
    """
    Сбросить запросы в processing старше N часов
    
    Args:
        group_name: Название группы
        hours_old: Сколько часов должно пройти (по умолчанию 1)
    """
    
    db_path = "output/master_queries.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("="*80)
    print("🔄 СБРОС ЗАСТРЯВШИХ ЗАПРОСОВ")
    print("="*80)
    print(f"Группа: {group_name}")
    print(f"Сбрасываем запросы старше: {hours_old} час(ов)")
    print()
    
    # Находим застрявшие запросы
    cutoff_time = datetime.now() - timedelta(hours=hours_old)
    
    cursor.execute('''
        SELECT COUNT(*) 
        FROM master_queries 
        WHERE group_name = ? 
          AND serp_status = 'processing'
          AND serp_updated_at < ?
    ''', (group_name, cutoff_time.isoformat()))
    
    stuck_count = cursor.fetchone()[0]
    
    if stuck_count == 0:
        print(f"✅ Нет застрявших запросов (старше {hours_old}ч)")
        conn.close()
        return
    
    print(f"⚠️  Найдено застрявших запросов: {stuck_count}")
    print()
    
    # Показываем примеры
    cursor.execute('''
        SELECT keyword, serp_req_id, serp_updated_at
        FROM master_queries 
        WHERE group_name = ? 
          AND serp_status = 'processing'
          AND serp_updated_at < ?
        LIMIT 5
    ''', (group_name, cutoff_time.isoformat()))
    
    print("📋 Примеры застрявших запросов:")
    print("-"*80)
    for keyword, req_id, updated in cursor.fetchall():
        keyword_short = keyword[:50] + "..." if len(keyword) > 50 else keyword
        req_id_short = req_id[:15] if req_id else "N/A"
        print(f"  {keyword_short:55} | {req_id_short} | {updated}")
    
    print("-"*80)
    print()
    
    # Запрос подтверждения
    response = input(f"Сбросить {stuck_count} запросов в статус 'pending'? (yes/no): ")
    
    if response.lower() not in ['yes', 'y', 'да']:
        print("❌ Отменено")
        conn.close()
        return
    
    # Сбрасываем статус
    cursor.execute('''
        UPDATE master_queries
        SET 
            serp_status = 'pending',
            serp_req_id = NULL,
            serp_error_message = 'Reset: req_id expired (error 203)',
            serp_updated_at = CURRENT_TIMESTAMP
        WHERE group_name = ? 
          AND serp_status = 'processing'
          AND serp_updated_at < ?
    ''', (group_name, cutoff_time.isoformat()))
    
    conn.commit()
    
    reset_count = cursor.rowcount
    
    print()
    print("="*80)
    print(f"✅ СБРОШЕНО: {reset_count} запросов")
    print(f"   Статус: processing → pending")
    print(f"   req_id: очищены")
    print(f"   Теперь можно запустить скрипт заново")
    print("="*80)
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python reset_stuck_processing.py <группа> [часов]")
        print()
        print("Пример:")
        print("  python reset_stuck_processing.py николай_чудотворец")
        print("  python reset_stuck_processing.py николай_чудотворец 2  # старше 2 часов")
        sys.exit(1)
    
    group_name = sys.argv[1]
    hours_old = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    
    reset_stuck_processing(group_name, hours_old)






