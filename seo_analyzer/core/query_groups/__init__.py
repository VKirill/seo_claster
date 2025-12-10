"""
Query Groups Module
Управление группами запросов с разделением БД и output
"""

from .group_manager import QueryGroupManager
from .group_config import QueryGroup
from .group_database import GroupDatabaseManager

__all__ = [
    'QueryGroupManager',
    'QueryGroup', 
    'GroupDatabaseManager',
]

