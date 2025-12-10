"""Конфигурация для работы с SERP xmlstock."""

from typing import Any, Dict

from .config_overrides import load_local_module, merge_overrides

_LOCAL = load_local_module()

_BASE_SERP_CONFIG: Dict[str, Any] = {
    "enabled": False,
    "cache_ttl_days": 30,
    "max_queries_to_analyze": 500,
    "api": {
        "base_url": "https://xmlstock.com/yandex/xml/",
        "lr": 213,
        "groupby": "attr=d.mode=deep.groups-on-page=20.docs-in-group=1",
        "maxpassages": 2,
        "filter": "moderate",
        "timeout": 10,
        "retry_delay": 5,
        "max_retries": 5,
        "max_concurrent": 50,
    },
}

_LOCAL_OVERRIDES = getattr(_LOCAL, "SERP_CONFIG_OVERRIDES", None) if _LOCAL else None
SERP_CONFIG: Dict[str, Any] = merge_overrides(_BASE_SERP_CONFIG, _LOCAL_OVERRIDES)

__all__ = ["SERP_CONFIG"]

