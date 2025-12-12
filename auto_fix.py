"""
Автоматический сброс застрявших запросов БЕЗ подтверждения
"""

import sqlite3
import sys
from datetime import datetime, timedelta

def auto_fix(group_name: str):
    """Автоматически сбросить застрявшие запросы"""
    
    db_path = "output/master_queries.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("="*80)
    print("🔧 АВТОМАТИЧЕСКОЕ ИСПРАВЛЕНИЕ")
    print("="*80)
    print(f"Группа: {group_name}\n")
    
    # Сбрасываем processing старше 1 часа
    cutoff_time = datetime.now() - timedelta(hours=1)
    
    cursor.execute('''
        UPDATE master_queries
        SET 
            serp_status = 'pending',
            serp_req_id = NULL,
            serp_error_message = 'Auto-reset: req_id expired',
            serp_updated_at = CURRENT_TIMESTAMP
        WHERE group_name = ? 
          AND serp_status = 'processing'
          AND serp_updated_at < ?
    ''', (group_name, cutoff_time.isoformat()))
    
    reset_count = cursor.rowcount
    conn.commit()
    
    if reset_count > 0:
        print(f"✅ Сброшено: {reset_count} запросов (processing → pending)")
    
    # Статистика
    cursor.execute('''
        SELECT serp_status, COUNT(*) as count
        FROM master_queries 
        WHERE group_name = ?
        GROUP BY serp_status
    ''', (group_name,))
    
    print("\n📊 Статистика:")
    print("-"*80)
    for status, count in cursor.fetchall():
        status_display = status if status else 'pending'
        print(f"  {status_display:15} {count:6}")
    
    # Без URLs
    cursor.execute('''
        SELECT COUNT(*) 
        FROM master_queries 
        WHERE group_name = ? 
          AND (serp_top_urls IS NULL OR serp_top_urls = '' OR serp_top_urls = '[]')
    ''', (group_name,))
    
    without_urls = cursor.fetchone()[0]
    print(f"\n📋 БЕЗ SERP URL: {without_urls}")
    print("="*80)
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python auto_fix.py <группа>")
        sys.exit(1)
    
    auto_fix(sys.argv[1])





