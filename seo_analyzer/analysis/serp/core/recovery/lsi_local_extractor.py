"""
Локальное извлечение LSI фраз из имеющихся данных
"""

import json
import sqlite3
from typing import List, Tuple

from seo_analyzer.core.lsi_extractor import LSIExtractor


class LSILocalExtractor:
    """Локальное извлечение LSI"""
    
    def __init__(self, db_path: str):
        """
        Args:
            db_path: Путь к Master DB
        """
        self.db_path = db_path
        self.lsi_extractor = LSIExtractor()
    
    def extract_lsi_for_queries(self, queries_with_full_data: List[Tuple]) -> int:
        """
        Извлечь LSI фразы из имеющихся данных без API запросов
        
        Args:
            queries_with_full_data: Список запросов с полными данными
            
        Returns:
            Количество обновленных запросов
        """
        if not queries_with_full_data:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        updated_count = 0
        
        try:
            for keyword, top_urls_json, req_id, query_group in queries_with_full_data:
                try:
                    if isinstance(top_urls_json, str):
                        top_urls = json.loads(top_urls_json) if top_urls_json.strip() else []
                    else:
                        top_urls = top_urls_json if top_urls_json else []
                    
                    if not top_urls:
                        continue
                    
                    documents = []
                    has_title_data = False
                    
                    for item in top_urls:
                        if isinstance(item, dict):
                            if item.get('title'):
                                has_title_data = True
                            documents.append({
                                'title': item.get('title', ''),
                                'snippet': item.get('snippet', ''),
                                'passages': item.get('passages', ''),
                                'url': item.get('url', ''),
                                'domain': item.get('domain', ''),
                                'is_commercial': item.get('is_commercial', False)
                            })
                    
                    if has_title_data and documents:
                        lsi_phrases = self.lsi_extractor.extract_from_serp_documents(documents, keyword)
                        
                        if lsi_phrases:
                            top_urls_updated = []
                            for i, doc in enumerate(documents[:20], 1):
                                top_urls_updated.append({
                                    'position': i,
                                    'url': doc.get('url', ''),
                                    'domain': doc.get('domain', ''),
                                    'title': doc.get('title', ''),
                                    'snippet': doc.get('snippet', ''),
                                    'passages': doc.get('passages', ''),
                                    'is_commercial': doc.get('is_commercial', False)
                                })
                            
                            top_urls_json_updated = json.dumps(top_urls_updated, ensure_ascii=False)
                            lsi_json = json.dumps(lsi_phrases, ensure_ascii=False)
                            
                            cursor.execute('''
                                UPDATE master_queries
                                SET serp_top_urls = ?, serp_lsi_phrases = ?
                                WHERE group_name = ? AND keyword = ?
                            ''', (top_urls_json_updated, lsi_json, query_group, keyword))
                            
                            query_short = keyword[:50] + "..." if len(keyword) > 50 else keyword
                            urls_count = len(documents)
                            lsi_count = len(lsi_phrases)
                            print(f"     ✓ '{query_short}': {urls_count} URLs, {lsi_count} LSI фраз")
                            
                            updated_count += 1
                            if updated_count % 10 == 0:
                                conn.commit()
                except Exception as e:
                    print(f"   ⚠️  Ошибка обработки '{keyword[:50]}...': {e}")
                    continue
            
            conn.commit()
            return updated_count
        finally:
            conn.close()

