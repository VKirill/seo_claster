"""Базовые настройки запуска pipeline с поддержкой локальных оверрайдов."""

from typing import Any, Dict

from .config_overrides import load_local_module, merge_overrides

_LOCAL = load_local_module()

_BASE_RUN_CONFIG: Dict[str, Any] = {
    # Общие флаги
    "input_file": "semantika/скуд.csv",
    "skip_embeddings": False,
    "enable_graph": False,
    "skip_topics": False,
    "skip_hierarchical": False,
    "skip_forms": False,
    "export_brands": False,
    # Фильтры частотности
    "min_frequency": 1,
    "max_frequency_ratio": 51.0,
    # SERP
    "serp_similarity_threshold": 7,
    "serp_top_positions": 20,
    "max_cluster_size": 100,
    "serp_mode": "balanced",
    "use_legacy_serp": False,
    "maxmin": True,  # Итеративная кластеризация от большего к меньшему по умолчанию
    # Excel/LSI/темы
    "skip_excel": False,
    "excel_with_charts": False,
    "soft_clustering": False,
    # Word-match
    "enable_word_match": False,
    "word_match_strength": 2,
    "word_match_min_size": 2,
    "word_match_strengthen": True,
}

_LOCAL_OVERRIDES = getattr(_LOCAL, "RUN_CONFIG_OVERRIDES", None) if _LOCAL else None
RUN_CONFIG: Dict[str, Any] = merge_overrides(_BASE_RUN_CONFIG, _LOCAL_OVERRIDES)

__all__ = ["RUN_CONFIG"]

