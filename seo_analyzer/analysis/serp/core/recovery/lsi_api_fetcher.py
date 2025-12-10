"""
Получение LSI фраз через API
"""

import asyncio
import json
from typing import List, Tuple
from datetime import datetime

from ...async_batch_client import AsyncBatchSERPClient, PendingRequest
from seo_analyzer.core.serp_data_enricher import SERPDataEnricher
from seo_analyzer.core.lsi_extractor import LSIExtractor


class LSIApiFetcher:
    """Получение LSI через API"""
    
    def __init__(self, api_key: str, lr: int, db_path: str):
        """
        Args:
            api_key: API ключ
            lr: Регион поиска
            db_path: Путь к Master DB
        """
        self.api_key = api_key
        self.lr = lr
        self.db_path = db_path
    
    async def fetch_lsi_for_queries(self, queries_with_req_id: List[Tuple]) -> int:
        """
        Получить LSI фразы для запросов через API
        
        Args:
            queries_with_req_id: Список запросов с req_id для получения через API
            
        Returns:
            Количество обновленных запросов
        """
        if not queries_with_req_id:
            print("   ⚠️  LSIApiFetcher: список запросов пуст")
            return 0
        
        print(f"   🔍 LSIApiFetcher: начинаем обработку {len(queries_with_req_id)} запросов")
        # Проверяем примеры req_id
        if queries_with_req_id:
            sample_req_id = queries_with_req_id[0][2] if len(queries_with_req_id[0]) > 2 else None
            print(f"   🔍 LSIApiFetcher: пример req_id: '{sample_req_id}'")
        
        batch_client = AsyncBatchSERPClient(
            api_key=self.api_key,
            lr=self.lr,
            max_concurrent_send=10,
            max_concurrent_fetch=50,
            initial_delay=5,
            retry_delay=5,
            max_attempts=20,
            requests_per_second=90.0
        )
        
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            pending_requests = []
            for keyword, _, req_id, _ in queries_with_req_id:
                if not req_id or not req_id.strip():
                    print(f"   ⚠️  Пропущен запрос '{keyword[:50]}...': req_id пустой")
                    continue
                pending_requests.append(
                    PendingRequest(query=keyword, req_id=req_id, sent_at=datetime.now())
                )
            
            if not pending_requests:
                print("   ⚠️  LSIApiFetcher: нет валидных req_id для обработки")
                return 0
            
            print(f"   🔍 LSIApiFetcher: создано {len(pending_requests)} PendingRequest объектов")
            
            fetch_semaphore = asyncio.Semaphore(50)
            updated_count = 0
            
            enricher = SERPDataEnricher()
            lsi_extractor = LSIExtractor()
            
            # Создаем словарь для быстрого доступа к данным запроса
            query_data_map = {}
            for keyword, _, req_id, query_group in queries_with_req_id:
                query_data_map[keyword] = (req_id, query_group)
            
            async def process_single_lsi_query(pending: PendingRequest):
                """Обработать один запрос для получения LSI"""
                nonlocal updated_count
                
                async with fetch_semaphore:
                    result = await batch_client.result_fetcher.fetch_result_by_req_id(
                        pending, fetch_semaphore, None
                    )
                    
                    if isinstance(result, Exception):
                        print(f"   ⚠️  Ошибка для '{pending.query[:50]}...': {result}")
                        return
                    
                    if isinstance(result, dict) and result.get('status') == 'completed':
                        xml_text = result.get('xml_response')
                        if xml_text:
                            enriched = enricher.enrich_from_serp(xml_text, pending.query)
                            lsi_phrases = lsi_extractor.extract_from_serp_documents(
                                enriched['documents'], pending.query
                            )
                            
                            top_urls_new = []
                            for idx, doc in enumerate(enriched['documents'][:20], 1):
                                top_urls_new.append({
                                    'position': idx,
                                    'url': doc.get('url', ''),
                                    'domain': doc.get('domain', ''),
                                    'title': doc.get('title', ''),
                                    'snippet': doc.get('snippet', ''),
                                    'passages': doc.get('passages', ''),
                                    'is_commercial': doc.get('is_commercial', False)
                                })
                            
                            top_urls_json_new = json.dumps(top_urls_new, ensure_ascii=False)
                            lsi_json = json.dumps(lsi_phrases, ensure_ascii=False) if lsi_phrases else '[]'
                            
                            req_id, query_group = query_data_map.get(pending.query, (None, None))
                            if query_group:
                                cursor.execute('''
                                    UPDATE master_queries
                                    SET serp_top_urls = ?, serp_lsi_phrases = ?
                                    WHERE group_name = ? AND keyword = ?
                                ''', (top_urls_json_new, lsi_json, query_group, pending.query))
                                
                                query_short = pending.query[:50] + "..." if len(pending.query) > 50 else pending.query
                                urls_count = len(enriched['documents'])
                                lsi_count = len(lsi_phrases)
                                print(f"     ✓ '{query_short}': {urls_count} URLs, {lsi_count} LSI фраз")
                                
                                updated_count += 1
                                if updated_count % 10 == 0:
                                    conn.commit()
            
            # Запускаем обработку всех запросов параллельно (streaming режим)
            tasks = [asyncio.create_task(process_single_lsi_query(pending)) for pending in pending_requests]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            conn.commit()
            print(f"   ✓ LSIApiFetcher: обработано {updated_count} запросов из {len(pending_requests)}")
            if updated_count == 0 and len(pending_requests) > 0:
                print(f"   ⚠️  LSIApiFetcher: ни один запрос не был обновлен!")
                print(f"   🔍 Проверьте, что req_id валидны и данные доступны через API")
            return updated_count
        except Exception as e:
            print(f"   ❌ LSIApiFetcher: ошибка при обработке: {e}")
            import traceback
            traceback.print_exc()
            return 0
        finally:
            conn.close()
            await batch_client.close()

