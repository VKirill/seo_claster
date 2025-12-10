"""
Модули для синхронной батчевой обработки SERP запросов
"""

from .request_sender import SyncRequestSender
from .result_fetcher import SyncResultFetcher
from .executor_manager import ExecutorManager

__all__ = ['SyncRequestSender', 'SyncResultFetcher', 'ExecutorManager']

