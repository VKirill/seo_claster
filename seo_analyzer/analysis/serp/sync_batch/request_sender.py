"""
Отправка синхронных запросов через executor
"""

import sys
import importlib.util
from pathlib import Path
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor


class SyncRequestSender:
    """Отправитель синхронных запросов"""
    
    @staticmethod
    async def send_request(
        query: str,
        user: str,
        key: str,
        lr: int,
        url: str,
        executor: ThreadPoolExecutor
    ) -> Dict[str, Any]:
        """
        Отправить синхронный запрос через executor
        
        Args:
            query: Запрос
            user: Пользователь API
            key: Ключ API
            lr: Регион
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
                api_key=f"{user}:{key}",
                lr=lr
            )
            temp_instance._executor = executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                executor,
                temp_instance._send_request_sync,
                query
            )
        else:
            return {'error': 'Backup file not found'}

