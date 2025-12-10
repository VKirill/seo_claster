"""Совместимый фасад конфигураций (разделен на модули <150 строк)."""

from .config_paths import (
    PROJECT_ROOT,
    KEYWORD_GROUP_DIR,
    KEYWORDS_STOP_DIR,
    OUTPUT_DIR,
    get_output_dir,
)
from .config_dictionaries import KEYWORD_DICTIONARIES, GEO_DICTIONARIES
from .clustering_config import CLUSTERING_CONFIG
from .serp_config import SERP_CONFIG
from .lsi_config import LSI_CONFIG
from .excel_hierarchy_config import EXCEL_CONFIG, HIERARCHY_CONFIG
from .run_config import RUN_CONFIG

# Неиспользуемые ранее конфиги убраны:
# - INTENT_THRESHOLDS
# - FUNNEL_PATTERNS, QUERY_PATTERNS
# - PAGE_TYPES, MODIFIERS
# - PERFORMANCE_CONFIG, EXPORT_CONFIG, KEI_CONFIG
# - get_keyword_group_path / get_stopwords_path
# Для локальных оверрайдов используйте config_local.py (см. шаблоны в нем).

__all__ = [
    "PROJECT_ROOT",
    "KEYWORD_GROUP_DIR",
    "KEYWORDS_STOP_DIR",
    "OUTPUT_DIR",
    "get_output_dir",
    "KEYWORD_DICTIONARIES",
    "GEO_DICTIONARIES",
    "CLUSTERING_CONFIG",
    "SERP_CONFIG",
    "LSI_CONFIG",
    "EXCEL_CONFIG",
    "HIERARCHY_CONFIG",
    "RUN_CONFIG",
]