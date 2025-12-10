"""SERP Database модули"""

# SERPDatabase удалён - используйте MasterQueryDatabase для всех данных
from .queries import get_serp_query, get_documents_query, get_lsi_query
from .migrations import init_database_schema

__all__ = [
    'get_serp_query',
    'get_documents_query',
    'get_lsi_query',
    'init_database_schema',
]

