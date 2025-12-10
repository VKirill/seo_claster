"""Конфигурация LSI агрегатора."""

from typing import Any, Dict

from .config_overrides import load_local_module, merge_overrides

_LOCAL = load_local_module()

_BASE_LSI_CONFIG: Dict[str, Any] = {
    "ngram_range": (2, 4),
    "top_n_per_query": 10,
    "top_n_per_cluster": 10,
    "min_phrase_frequency": 2,
    "use_lemmatization": True,
}

_LOCAL_OVERRIDES = getattr(_LOCAL, "LSI_CONFIG_OVERRIDES", None) if _LOCAL else None
LSI_CONFIG: Dict[str, Any] = merge_overrides(_BASE_LSI_CONFIG, _LOCAL_OVERRIDES)

__all__ = ["LSI_CONFIG"]

