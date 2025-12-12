"""
Query Groups Module
Управление группами запросов с разделением БД и output
"""

from .group_manager import QueryGroupManager, normalize_group_name
from .group_config import QueryGroup
from .group_database import GroupDatabaseManager

__all__ = [
    'QueryGroupManager',
    'QueryGroup', 
    'GroupDatabaseManager',
    'normalize_group_name',
]

