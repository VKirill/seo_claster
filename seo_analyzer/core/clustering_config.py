"""Конфигурация кластеризации, используемая в pipeline.

Все значения можно частично перекрыть через CLUSTERING_CONFIG_OVERRIDES
в config_local.py. Ниже указано влияние основных блоков:
- graph: параметры построения графа по embeddings (используется, если включить --enable-graph)
- embeddings: модель и батч для генерации эмбеддингов (нужны графу)
- word_match: правила группировки по совпадениям слов (--enable-word-match)
- serp_advanced: правила SERP-кластеризации (используются по умолчанию)
"""

from typing import Any, Dict

from .config_overrides import load_local_module, merge_overrides

_LOCAL = load_local_module()

# Убраны неиспользуемые блоки (tfidf/kmeans/dbscan/hierarchical/topic_modeling)
_BASE_CLUSTERING_CONFIG: Dict[str, Any] = {
    # Параметры графа на embeddings
    "graph": {
        # Порог похожести ребра (0..1), выше — меньше ребер, быстрее, но разреженнее
        "similarity_threshold": 0.5,
        # Минимальный вес ребра, мелкие связи отбрасываются
        "min_edge_weight": 0.3,
        # Resolution для Louvain, >1 больше кластеров, <1 крупнее
        "community_resolution": 1.0,
    },
    # Настройки sentence-transformers
    "embeddings": {
        # Модель для эмбеддингов запросов
        "model_name": "paraphrase-multilingual-MiniLM-L12-v2",
        # Размер батча при кодировании (баланс скорость/память)
        "batch_size": 32,
        # Показывать прогресс бар при кодировании
        "show_progress_bar": True,
    },
    # Группировка по совпадениям слов (аналог KeyCollector)
    "word_match": {
        # Мин. совпадений слов, чтобы соединить запросы
        "min_match_strength": 7,
        # Мин. размер группы
        "min_group_size": 1,
        # Усиливать связи внутри найденных групп (перераспределение)
        "strengthen_links": True,
        # Исключать стоп-слова при сравнении
        "exclude_stopwords": True,
        # Использовать лемматизацию для сопоставления
        "use_lemmatization": True,
    },
    # Продвинутая SERP кластеризация (по умолчанию)
    "serp_advanced": {
        # Мин. общих URL между запросами, чтобы считать их связанными
        "min_common_urls": 7,
        # Сколько позиций SERP анализируем
        "top_positions": 20,
        # Макс. размер кластера (больше — дробится)
        "max_cluster_size": 50,
        # Режим: strict/balanced/soft — жесткость требований
        "mode": "strict",
        # Делать семантическую проверку кластеров
        "semantic_check": True,
        # Мин. связность кластера в balanced/soft
        "min_cluster_cohesion": 0.8,
    },
    # Пост-обработка кластеров (деление больших, прикрепление одиночек)
    "postprocess": {
        # Макс. размер кластера при пост-обработке (дефолт для --max-cluster-size)
        "max_cluster_size": 12,
        # Шаг повышения порога при делении больших кластеров
        "threshold_step": 1,
    },
}

_LOCAL_OVERRIDES = getattr(_LOCAL, "CLUSTERING_CONFIG_OVERRIDES", None) if _LOCAL else None
CLUSTERING_CONFIG: Dict[str, Any] = merge_overrides(_BASE_CLUSTERING_CONFIG, _LOCAL_OVERRIDES)

__all__ = ["CLUSTERING_CONFIG"]

