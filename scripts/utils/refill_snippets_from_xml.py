"""
Перезагрузка snippet и passages из XML для исправления пустых полей
"""

import sqlite3
import json
from pathlib import Path
from typing import Dict, Any
import xml.etree.ElementTree as ET

# Импортируем enricher для правильной обработки XML
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from seo_analyzer.core.serp_enricher.enricher import SERPDataEnricher


def refill_snippets_for_group(group_name: str):
    """
    Перезагрузить snippet и passages из XML в master_queries.db
    
    Args:
        group_name: Название группы (например, 'скуд')
    """
    serp_db_path = Path("data/databases/serp_data.db")
    master_db_path = Path("output/master_queries.db")
    
    if not serp_db_path.exists():
        print(f"❌ SERP база данных не найдена: {serp_db_path}")
        return
    
    if not master_db_path.exists():
        print(f"❌ Master база данных не найдена: {master_db_path}")
        return
    
    print(f"📊 Перезагрузка snippet и passages для группы '{group_name}'...")
    print(f"   SERP DB: {serp_db_path}")
    print(f"   Master DB: {master_db_path}")
    print()
    
    # Подключаемся к обеим базам
    serp_conn = sqlite3.connect(serp_db_path)
    master_conn = sqlite3.connect(master_db_path)
    
    try:
        # Создаём enricher для обработки XML
        enricher = SERPDataEnricher()
        
        # Получаем все запросы группы из master_queries
        master_cursor = master_conn.cursor()
        master_cursor.execute('''
            SELECT keyword, serp_top_urls
            FROM master_queries
            WHERE group_name = ?
            AND serp_status = 'completed'
        ''', (group_name,))
        
        queries = master_cursor.fetchall()
        total = len(queries)
        
        if total == 0:
            print(f"⚠️  Нет завершённых SERP запросов для группы '{group_name}'")
            return
        
        print(f"✓ Найдено {total} запросов с SERP данными")
        print()
        
        # Счётчики
        updated_count = 0
        skipped_count = 0
        error_count = 0
        
        for idx, (keyword, serp_top_urls_json) in enumerate(queries, 1):
            # Проверяем есть ли уже snippet в JSON
            if serp_top_urls_json:
                try:
                    current_data = json.loads(serp_top_urls_json)
                    # Проверяем первый документ
                    if current_data and len(current_data) > 0:
                        first_doc = current_data[0]
                        # Если snippet уже есть - пропускаем
                        if first_doc.get('snippet') and first_doc.get('passages'):
                            skipped_count += 1
                            if idx % 100 == 0:
                                print(f"   [{idx}/{total}] Пропущено (данные уже есть): {keyword[:50]}")
                            continue
                except:
                    pass
            
            # Получаем XML из serp_data.db
            serp_cursor = serp_conn.cursor()
            serp_cursor.execute('''
                SELECT xml_response
                FROM serp_results
                WHERE query = ? AND query_group = ?
                LIMIT 1
            ''', (keyword, group_name))
            
            row = serp_cursor.fetchone()
            
            if not row or not row[0]:
                # XML не найден - пропускаем
                skipped_count += 1
                continue
            
            xml_response = row[0]
            
            try:
                # Обрабатываем XML через enricher (с новым кодом fallback)
                enriched = enricher.enrich_from_serp(xml_response, keyword)
                documents = enriched.get('documents', [])
                
                if not documents:
                    skipped_count += 1
                    continue
                
                # Формируем обновлённый JSON для serp_top_urls
                top_urls = []
                for i, doc in enumerate(documents[:20], 1):
                    top_urls.append({
                        'position': i,
                        'url': doc.get('url', ''),
                        'domain': doc.get('domain', ''),
                        'title': doc.get('title', ''),
                        'snippet': doc.get('snippet', ''),
                        'passages': doc.get('passages', ''),
                        'is_commercial': doc.get('is_commercial', False)
                    })
                
                top_urls_json = json.dumps(top_urls, ensure_ascii=False)
                
                # Обновляем master_queries
                master_cursor.execute('''
                    UPDATE master_queries
                    SET serp_top_urls = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE group_name = ? AND keyword = ?
                ''', (top_urls_json, group_name, keyword))
                
                updated_count += 1
                
                # Логирование прогресса
                if idx % 10 == 0 or idx <= 5:
                    snippet_preview = top_urls[0]['snippet'][:60] if top_urls[0]['snippet'] else '(пусто)'
                    passages_preview = top_urls[0]['passages'][:60] if top_urls[0]['passages'] else '(пусто)'
                    print(f"   [{idx}/{total}] ✓ {keyword[:40]}")
                    print(f"      Snippet: {snippet_preview}...")
                    print(f"      Passages: {passages_preview}...")
            
            except Exception as e:
                error_count += 1
                print(f"   [{idx}/{total}] ❌ Ошибка: {keyword[:50]} - {e}")
        
        # Сохраняем изменения
        master_conn.commit()
        
        print()
        print("=" * 80)
        print(f"✅ Обработка завершена!")
        print(f"   Обновлено: {updated_count}")
        print(f"   Пропущено (данные уже есть): {skipped_count}")
        print(f"   Ошибок: {error_count}")
        print(f"   Всего: {total}")
        
    finally:
        serp_conn.close()
        master_conn.close()


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Использование: python refill_snippets_from_xml.py <group_name>")
        print("Пример: python refill_snippets_from_xml.py скуд")
        sys.exit(1)
    
    group_name = sys.argv[1]
    refill_snippets_for_group(group_name)

