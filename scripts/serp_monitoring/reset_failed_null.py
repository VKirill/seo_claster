"""Сброс failed запросов с NULL ошибкой в pending"""

import sqlite3
import sys

def reset_failed_null(group_name: str):
    """Сбросить failed с NULL ошибкой обратно в pending"""
    
    db_path = "output/master_queries.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("="*80)
    print("🔄 СБРОС FAILED С NULL ОШИБКОЙ")
    print("="*80)
    print(f"Группа: {group_name}\n")
    
    # Подсчёт
    cursor.execute('''
        SELECT COUNT(*)
        FROM master_queries 
        WHERE group_name = ? 
          AND serp_status = 'failed'
          AND (serp_error_message IS NULL OR serp_error_message = '')
    ''', (group_name,))
    
    count = cursor.fetchone()[0]
    
    if count == 0:
        print("✅ Нет failed с NULL ошибкой")
        conn.close()
        return
    
    print(f"📋 Найдено failed с NULL: {count}")
    print(f"   Это старые записи без информации об ошибке")
    print(f"   Действие: сброс в 'pending' для повторной отправки\n")
    
    # Сбрасываем
    cursor.execute('''
        UPDATE master_queries
        SET 
            serp_status = 'pending',
            serp_req_id = NULL,
            serp_error_message = NULL,
            serp_updated_at = CURRENT_TIMESTAMP
        WHERE group_name = ? 
          AND serp_status = 'failed'
          AND (serp_error_message IS NULL OR serp_error_message = '')
    ''', (group_name,))
    
    reset_count = cursor.rowcount
    conn.commit()
    
    print(f"✅ Сброшено: {reset_count} запросов (failed → pending)")
    
    # Статистика после
    cursor.execute('''
        SELECT serp_status, COUNT(*) as count
        FROM master_queries 
        WHERE group_name = ?
        GROUP BY serp_status
        ORDER BY count DESC
    ''', (group_name,))
    
    print("\n📊 Статистика после сброса:")
    print("-"*80)
    
    total = 0
    for status, cnt in cursor.fetchall():
        status_display = status if status else 'pending'
        total += cnt
        
        if status == 'completed':
            icon = "✅"
        elif status == 'pending':
            icon = "⏸️"
        elif status == 'processing':
            icon = "⏳"
        elif status == 'failed':
            icon = "❌"
        else:
            icon = "❓"
        
        print(f"  {icon} {status_display:15} {cnt:6} ({cnt/total*100:5.1f}%)")
    
    print("-"*80)
    print(f"  📝 ВСЕГО:          {total:6}")
    
    # БЕЗ URLs
    cursor.execute('''
        SELECT COUNT(*) 
        FROM master_queries 
        WHERE group_name = ? 
          AND (serp_top_urls IS NULL OR serp_top_urls = '' OR serp_top_urls = '[]')
    ''', (group_name,))
    
    without_urls = cursor.fetchone()[0]
    
    print(f"  📋 БЕЗ SERP URL:   {without_urls:6}")
    print("="*80)
    print()
    
    print(f"🚀 СЛЕДУЮЩИЙ ШАГ:")
    print(f"   Запустите основной скрипт для обработки {without_urls} запросов:")
    print()
    print(f"   python main.py {group_name}")
    print()
    print("="*80)
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python reset_failed_null.py <группа>")
        print()
        print("Пример:")
        print("  python reset_failed_null.py николай_чудотворец")
        sys.exit(1)
    
    reset_failed_null(sys.argv[1])

