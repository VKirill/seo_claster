"""Конфигурации экспорта Excel и анализа иерархии."""

from typing import Any, Dict

from .config_overrides import load_local_module, merge_overrides

_LOCAL = load_local_module()

_BASE_EXCEL_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "include_charts": False,
    "include_pivot": False,
    "group_by_clusters": True,
    "conditional_formatting": True,
    "freeze_panes": True,
    "auto_filter": True,
}

_BASE_HIERARCHY_CONFIG: Dict[str, Any] = {
    "enabled": False,
    "deepseek_api_key": "",
    "max_urls_per_query": 3,
    "use_clusters": True,
    "timeout": 10,
    "use_database": True,
    "filter_stop_domains": True,
    # Настройки сбора breadcrumbs
    "collect_breadcrumbs": False,  # Собирать ли breadcrumbs (скачивание страниц)
    "use_breadcrumbs": False,      # Использовать ли breadcrumbs для иерархии
}

_EXCEL_OVERRIDES = getattr(_LOCAL, "EXCEL_CONFIG_OVERRIDES", None) if _LOCAL else None
_HIERARCHY_OVERRIDES = getattr(_LOCAL, "HIERARCHY_CONFIG_OVERRIDES", None) if _LOCAL else None

EXCEL_CONFIG: Dict[str, Any] = merge_overrides(_BASE_EXCEL_CONFIG, _EXCEL_OVERRIDES)
HIERARCHY_CONFIG: Dict[str, Any] = merge_overrides(_BASE_HIERARCHY_CONFIG, _HIERARCHY_OVERRIDES)

__all__ = ["EXCEL_CONFIG", "HIERARCHY_CONFIG"]

