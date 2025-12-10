"""
Обновление snippet на extended_text из XML для более полного описания
"""

import sqlite3
import json
from pathlib import Path


def copy_passages_to_snippet(group_name: str):
    """
    Скопировать passages в snippet где snippet пустой
    
    Args:
        group_name: Название группы
    """
    master_db_path = Path("output/master_queries.db")
    
    if not master_db_path.exists():
        print(f"❌ Master база данных не найдена: {master_db_path}")
        return
    
    print(f"📊 Копирование passages → snippet для группы '{group_name}'...")
    print(f"   Master DB: {master_db_path}")
    print()
    
    conn = sqlite3.connect(master_db_path)
    cursor = conn.cursor()
    
    try:
        # Получаем все запросы группы
        cursor.execute('''
            SELECT keyword, serp_top_urls
            FROM master_queries
            WHERE group_name = ?
            AND serp_status = 'completed'
            AND serp_top_urls IS NOT NULL
        ''', (group_name,))
        
        queries = cursor.fetchall()
        total = len(queries)
        
        if total == 0:
            print(f"⚠️  Нет запросов для обработки")
            return
        
        print(f"✓ Найдено {total} запросов")
        print()
        
        updated_count = 0
        already_filled_count = 0
        
        for idx, (keyword, serp_top_urls_json) in enumerate(queries, 1):
            try:
                data = json.loads(serp_top_urls_json)
                modified = False
                
                for doc in data:
                    # Если snippet пустой, но passages есть
                    if not doc.get('snippet') and doc.get('passages'):
                        doc['snippet'] = doc['passages']
                        modified = True
                
                if modified:
                    # Сохраняем обновлённый JSON
                    updated_json = json.dumps(data, ensure_ascii=False)
                    cursor.execute('''
                        UPDATE master_queries
                        SET serp_top_urls = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE group_name = ? AND keyword = ?
                    ''', (updated_json, group_name, keyword))
                    
                    updated_count += 1
                    
                    if idx <= 5 or idx % 100 == 0:
                        print(f"   [{idx}/{total}] ✓ {keyword[:50]}")
                        if data:
                            snippet_preview = data[0]['snippet'][:80]
                            print(f"      Snippet: {snippet_preview}...")
                else:
                    already_filled_count += 1
            
            except Exception as e:
                print(f"   [{idx}/{total}] ❌ Ошибка: {keyword[:50]} - {e}")
        
        conn.commit()
        
        print()
        print("=" * 80)
        print(f"✅ Обработка завершена!")
        print(f"   Обновлено: {updated_count}")
        print(f"   Уже заполнено: {already_filled_count}")
        print(f"   Всего: {total}")
        
    finally:
        conn.close()


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Использование: python copy_passages_to_snippet.py <group_name>")
        print("Пример: python copy_passages_to_snippet.py скуд")
        sys.exit(1)
    
    group_name = sys.argv[1]
    copy_passages_to_snippet(group_name)

