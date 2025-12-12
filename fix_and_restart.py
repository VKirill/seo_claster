"""
Автоматический сброс застрявших запросов и перезапуск
"""

import sqlite3
import sys
from datetime import datetime, timedelta

def fix_and_restart(group_name: str):
    """
    Сбросить застрявшие запросы автоматически
    """
    
    db_path = "output/master_queries.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("="*80)
    print("🔧 АВТОМАТИЧЕСКОЕ ИСПРАВЛЕНИЕ")
    print("="*80)
    print(f"Группа: {group_name}")
    print()
    
    # 1. Сбрасываем processing старше 1 часа (req_id истекли - ошибка 203)
    cutoff_time = datetime.now() - timedelta(hours=1)
    
    cursor.execute('''
        SELECT COUNT(*) 
        FROM master_queries 
        WHERE group_name = ? 
          AND serp_status = 'processing'
          AND serp_updated_at < ?
    ''', (group_name, cutoff_time.isoformat()))
    
    stuck_processing = cursor.fetchone()[0]
    
    if stuck_processing > 0:
        print(f"📋 Найдено застрявших в 'processing': {stuck_processing}")
        print(f"   Причина: req_id истекли (ошибка 203 от XMLStock)")
        print(f"   Действие: сброс в 'pending' для перезапроса")
        
        cursor.execute('''
            UPDATE master_queries
            SET 
                serp_status = 'pending',
                serp_req_id = NULL,
                serp_error_message = 'Auto-reset: req_id expired (error 203)',
                serp_updated_at = CURRENT_TIMESTAMP
            WHERE group_name = ? 
              AND serp_status = 'processing'
              AND serp_updated_at < ?
        ''', (group_name, cutoff_time.isoformat()))
        
        print(f"   ✅ Сброшено: {cursor.rowcount} запросов")
        print()
    
    # 2. Сбрасываем failed БЕЗ req_id (ошибки отправки, можно переотправить)
    cursor.execute('''
        SELECT COUNT(*) 
        FROM master_queries 
        WHERE group_name = ? 
          AND serp_status = 'failed'
          AND (serp_req_id IS NULL OR serp_req_id = '')
    ''', (group_name,))
    
    failed_no_reqid = cursor.fetchone()[0]
    
    if failed_no_reqid > 0:
        print(f"📋 Найдено 'failed' без req_id: {failed_no_reqid}")
        print(f"   Причина: ошибки при отправке запроса")
        print(f"   Действие: сброс в 'pending' для повторной попытки")
        
        cursor.execute('''
            UPDATE master_queries
            SET 
                serp_status = 'pending',
                serp_error_message = NULL,
                serp_updated_at = CURRENT_TIMESTAMP
            WHERE group_name = ? 
              AND serp_status = 'failed'
              AND (serp_req_id IS NULL OR serp_req_id = '')
        ''', (group_name,))
        
        print(f"   ✅ Сброшено: {cursor.rowcount} запросов")
        print()
    
    conn.commit()
    
    # 3. Статистика после исправления
    cursor.execute('''
        SELECT 
            serp_status,
            COUNT(*) as count
        FROM master_queries 
        WHERE group_name = ?
        GROUP BY serp_status
        ORDER BY count DESC
    ''', (group_name,))
    
    print("="*80)
    print("📊 СТАТИСТИКА ПОСЛЕ ИСПРАВЛЕНИЯ:")
    print("-"*80)
    
    total = 0
    for status, count in cursor.fetchall():
        status_display = status if status else 'pending'
        total += count
        
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
        
        print(f"  {icon} {status_display:15} {count:6} ({count/total*100:5.1f}%)")
    
    print("-"*80)
    print(f"  📝 ВСЕГО:          {total:6}")
    print("="*80)
    print()
    
    # 4. Проверяем сколько осталось обработать
    cursor.execute('''
        SELECT COUNT(*) 
        FROM master_queries 
        WHERE group_name = ? 
          AND (serp_top_urls IS NULL OR serp_top_urls = '' OR serp_top_urls = '[]' OR LENGTH(serp_top_urls) <= 2)
    ''', (group_name,))
    
    without_urls = cursor.fetchone()[0]
    
    print(f"📊 Запросов БЕЗ SERP URL: {without_urls}")
    print()
    
    if without_urls > 0:
        print("🚀 СЛЕДУЮЩИЙ ШАГ:")
        print(f"   Запустите основной скрипт для обработки {without_urls} запросов:")
        print()
        print(f"   python main.py {group_name}")
        print()
        print("💡 ВАЖНО:")
        print("   • Скрипт будет обрабатывать по 50 запросов за раз")
        print("   • Каждый батч: отправка → получение → сохранение → следующий батч")
        print("   • req_id не накапливаются, результаты сразу сохраняются в БД")
        print(f"   • Время обработки: ~{without_urls/50*12/60:.0f} минут (при 50 запросов/12 сек)")
    else:
        print("✅ ВСЕ ЗАПРОСЫ ОБРАБОТАНЫ!")
    
    print("="*80)
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python fix_and_restart.py <группа>")
        print()
        print("Пример:")
        print("  python fix_and_restart.py николай_чудотворец")
        sys.exit(1)
    
    group_name = sys.argv[1]
    fix_and_restart(group_name)





