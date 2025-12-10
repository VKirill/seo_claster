"""
Восстановление незавершённых запросов по req_id
"""

import asyncio
from typing import List, Dict, Any
from datetime import datetime

from ...async_batch_client import AsyncBatchSERPClient, PendingRequest
from seo_analyzer.core.serp_data_enricher import SERPDataEnricher
from seo_analyzer.core.lsi_extractor import LSIExtractor


class PendingQueriesRecoverer:
    """Восстановитель незавершённых запросов"""
    
    def __init__(self, api_key: str, lr: int, master_db_handler):
        """
        Args:
            api_key: API ключ
            lr: Регион поиска
            master_db_handler: Обработчик Master DB
        """
        self.api_key = api_key
        self.lr = lr
        self.master_db_handler = master_db_handler
    
    async def recover(self, pending_queries: List[Dict[str, Any]]) -> int:
        """
        Восстановить незавершённые запросы
        
        Args:
            pending_queries: Список незавершённых запросов
            
        Returns:
            Количество восстановленных запросов
        """
        if not pending_queries:
            return 0
        
        recoverable_by_req_id = [
            item for item in pending_queries 
            if item.get('req_id') and not item.get('needs_new_request', False)
        ]
        
        pending_requests = [
            PendingRequest(query=item['query'], req_id=item['req_id'], sent_at=datetime.now())
            for item in recoverable_by_req_id
        ]
        
        if not pending_requests:
            return 0
        
        batch_client = AsyncBatchSERPClient(
            api_key=self.api_key,
            lr=self.lr,
            max_concurrent_send=10,
            max_concurrent_fetch=20,
            initial_delay=5,
            retry_delay=10,
            max_attempts=50,
            requests_per_second=90.0
        )
        
        try:
            fetch_semaphore = asyncio.Semaphore(50)
            recovered_count = 0
            
            # Создаем словарь для быстрого доступа к группе
            group_map = {}
            for idx, item in enumerate(recoverable_by_req_id):
                group_map[item['query']] = item['group']
            
            enricher = SERPDataEnricher()
            lsi_extractor = LSIExtractor()
            
            async def process_single_recovery(pending: PendingRequest):
                """Обработать один запрос для восстановления"""
                nonlocal recovered_count
                
                async with fetch_semaphore:
                    result = await batch_client.result_fetcher.fetch_result_by_req_id(
                        pending, fetch_semaphore, None
                    )
                    
                    if isinstance(result, dict) and result.get('status') == 'completed':
                        xml_text = result.get('xml_response')
                        if xml_text:
                            enriched = enricher.enrich_from_serp(xml_text, pending.query)
                            lsi_phrases = lsi_extractor.extract_from_serp_documents(
                                enriched['documents'], pending.query
                            )
                            
                            original_group = group_map.get(pending.query)
                            if self.master_db_handler.master_db and original_group:
                                try:
                                    self.master_db_handler.master_db.update_serp_status(
                                        group_name=original_group,
                                        keyword=pending.query,
                                        status='completed',
                                        req_id=pending.req_id
                                    )
                                    self.master_db_handler.master_db.update_serp_metrics(
                                        group_name=original_group,
                                        keyword=pending.query,
                                        metrics=enriched['metrics'],
                                        documents=enriched['documents'],
                                        lsi_phrases=lsi_phrases
                                    )
                                    
                                    query_short = pending.query[:50] + "..." if len(pending.query) > 50 else pending.query
                                    urls_count = len(enriched['documents'])
                                    lsi_count = len(lsi_phrases)
                                    print(f"     ✓ '{query_short}': {urls_count} URLs, {lsi_count} LSI фраз")
                                    
                                    recovered_count += 1
                                except Exception as e:
                                    print(f"   ⚠️  Ошибка сохранения для '{pending.query[:50]}...': {e}")
            
            # Запускаем обработку всех запросов параллельно (streaming режим)
            tasks = [asyncio.create_task(process_single_recovery(pending)) for pending in pending_requests]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            return recovered_count
        finally:
            await batch_client.close()

