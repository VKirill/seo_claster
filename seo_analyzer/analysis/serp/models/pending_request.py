"""
Модель отложенного запроса
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PendingRequest:
    """Отложенный запрос"""
    query: str
    req_id: str
    sent_at: datetime
    attempts: int = 0
    last_error: Optional[str] = None

