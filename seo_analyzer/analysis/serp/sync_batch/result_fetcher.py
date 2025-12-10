"""
Получение результатов синхронных запросов через executor
"""

import sys
import importlib.util
from pathlib import Path
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor
import asyncio


class SyncResultFetcher:
    """Получатель результатов синхронных запросов"""
    
    @staticmethod
    async def fetch_result(
        req_id: str,
        user: str,
        key: str,
        url: str,
        executor: ThreadPoolExecutor
    ) -> Dict[str, Any]:
        """
        Получить результат синхронного запроса через executor
        
        Args:
            req_id: ID запроса
            user: Пользователь API
            key: Ключ API
            url: URL API
            executor: ThreadPoolExecutor
            
        Returns:
            Результат запроса
        """
        backup_path = Path(__file__).parent.parent / 'sync_batch_client.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("sync_batch_client_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            temp_instance = module.SyncBatchSERPClient(
                api_key=f"{user}:{key}"
            )
            temp_instance._executor = executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                executor,
                temp_instance._fetch_request_sync,
                req_id
            )
        else:
            return {'error': 'Backup file not found'}

