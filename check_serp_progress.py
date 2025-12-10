"""
Проверка прогресса заполнения SERP данных
"""

import sqlite3
import sys
from datetime import datetime

def check_serp_progress(group_name: str = None):
    """
    Проверить прогресс заполнения SERP данных
    
    Args:
        group_name: Название группы (опционально)
    """
    
    db_path = "output/master_queries.db"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("="*80)
        print("📊 ПРОГРЕСС ЗАПОЛНЕНИЯ SERP ДАННЫХ")
        print("="*80)
        print(f"⏰ Время проверки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Общая статистика
        if group_name:
            where_clause = "WHERE group_name = ?"
            params = (group_name,)
            print(f"📁 Группа: {group_name}")
        else:
            where_clause = ""
            params = ()
            print(f"📁 Группа: ВСЕ")
        
        print()
        
        # Всего запросов
        cursor.execute(f'''
            SELECT COUNT(*) FROM master_queries {where_clause}
        ''', params)
        total_queries = cursor.fetchone()[0]
        
        # Статусы SERP
        cursor.execute(f'''
            SELECT 
                serp_status,
                COUNT(*) as count
            FROM master_queries 
            {where_clause}
            GROUP BY serp_status
            ORDER BY count DESC
        ''', params)
        
        statuses = cursor.fetchall()
        
        print("📊 СТАТИСТИКА ПО СТАТУСАМ:")
        print("-" * 60)
        
        completed = 0
        processing = 0
        error = 0
        pending = 0
        
        for status, count in statuses:
            percentage = count / total_queries * 100 if total_queries > 0 else 0
            status_display = status if status else 'NULL/pending'
            
            if status == 'completed':
                completed = count
                icon = "✅"
            elif status == 'processing':
                processing = count
                icon = "⏳"
            elif status == 'error':
                error = count
                icon = "❌"
            else:
                pending = count
                icon = "⏸️"
            
            print(f"  {icon} {status_display:15} {count:6} ({percentage:5.1f}%)")
        
        print("-" * 60)
        print(f"  📝 ВСЕГО:          {total_queries:6}")
        print()
        
        # Проверка наличия URL (правильная проверка с учётом '[]')
        cursor.execute(f'''
            SELECT COUNT(*) 
            FROM master_queries 
            {where_clause}
            AND (serp_top_urls IS NULL OR serp_top_urls = '' OR serp_top_urls = '[]' OR LENGTH(serp_top_urls) <= 2)
        ''', params)
        without_urls = cursor.fetchone()[0]
        
        cursor.execute(f'''
            SELECT COUNT(*) 
            FROM master_queries 
            {where_clause}
            AND serp_top_urls IS NOT NULL 
            AND serp_top_urls != '' 
            AND serp_top_urls != '[]'
            AND LENGTH(serp_top_urls) > 2
        ''', params)
        with_urls = cursor.fetchone()[0]
        
        print("🔗 НАЛИЧИЕ SERP URL:")
        print("-" * 60)
        print(f"  ✅ С URL:          {with_urls:6} ({with_urls/total_queries*100:5.1f}%)")
        print(f"  ❌ БЕЗ URL:        {without_urls:6} ({without_urls/total_queries*100:5.1f}%)")
        print("-" * 60)
        print()
        
        # Проверка req_id
        cursor.execute(f'''
            SELECT COUNT(*) 
            FROM master_queries 
            {where_clause}
            AND serp_req_id IS NOT NULL AND serp_req_id != ''
        ''', params)
        with_req_id = cursor.fetchone()[0]
        
        cursor.execute(f'''
            SELECT COUNT(*) 
            FROM master_queries 
            {where_clause}
            AND (serp_req_id IS NULL OR serp_req_id = '')
        ''', params)
        without_req_id = cursor.fetchone()[0]
        
        print("🔑 НАЛИЧИЕ REQ_ID:")
        print("-" * 60)
        print(f"  ✅ С req_id:       {with_req_id:6} ({with_req_id/total_queries*100:5.1f}%)")
        print(f"  ❌ БЕЗ req_id:     {without_req_id:6} ({without_req_id/total_queries*100:5.1f}%)")
        print("-" * 60)
        print()
        
        # Прогресс заполнения
        progress_percentage = with_urls / total_queries * 100 if total_queries > 0 else 0
        
        print("📈 ПРОГРЕСС ЗАПОЛНЕНИЯ:")
        print("-" * 60)
        bar_width = 50
        filled = int(bar_width * progress_percentage / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        print(f"  [{bar}] {progress_percentage:.1f}%")
        print()
        print(f"  Заполнено:  {with_urls:6} / {total_queries}")
        print(f"  Осталось:   {without_urls:6}")
        print("-" * 60)
        print()
        
        # Последние обновления
        cursor.execute(f'''
            SELECT 
                keyword,
                serp_status,
                serp_req_id,
                serp_updated_at
            FROM master_queries 
            {where_clause}
            AND serp_updated_at IS NOT NULL
            ORDER BY serp_updated_at DESC
            LIMIT 5
        ''', params)
        
        recent = cursor.fetchall()
        
        if recent:
            print("🕐 ПОСЛЕДНИЕ ОБНОВЛЕНИЯ:")
            print("-" * 60)
            for keyword, status, req_id, updated_at in recent:
                keyword_short = keyword[:40] + "..." if len(keyword) > 40 else keyword
                req_id_short = req_id[:15] + "..." if req_id and len(req_id) > 15 else (req_id or "N/A")
                print(f"  {keyword_short:45} | {status:10} | {req_id_short}")
            print("-" * 60)
            print()
        
        conn.close()
        
        # Итоговый статус
        print("="*80)
        if progress_percentage >= 100:
            print("✅ ЗАПОЛНЕНИЕ ЗАВЕРШЕНО НА 100%")
        elif progress_percentage >= 90:
            print(f"🎯 ПОЧТИ ЗАВЕРШЕНО: {progress_percentage:.1f}% (осталось {without_urls} запросов)")
        elif progress_percentage >= 50:
            print(f"⏳ ПРОЦЕСС ИДЁТ: {progress_percentage:.1f}% (осталось {without_urls} запросов)")
        elif progress_percentage > 0:
            print(f"🚀 НАЧАЛО ОБРАБОТКИ: {progress_percentage:.1f}% (осталось {without_urls} запросов)")
        else:
            print("⏸️  ОБРАБОТКА НЕ НАЧАТА")
        print("="*80)
        
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            print("❌ Таблица master_queries не существует")
            print("   Запустите сначала основной скрипт для создания БД")
        else:
            print(f"❌ Ошибка БД: {e}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    group_name = sys.argv[1] if len(sys.argv) > 1 else None
    
    if group_name:
        print(f"\n💡 Проверка группы: {group_name}")
        print(f"   Для проверки всех групп запустите без параметров\n")
    else:
        print(f"\n💡 Проверка всех групп")
        print(f"   Для проверки конкретной группы: python check_serp_progress.py <группа>\n")
    
    check_serp_progress(group_name)

