"""
Пост-обработка кластеров: делит слишком большие группы и пытается
присоединить одиночные запросы, если у них есть схожие SERP URL.

⚠️ ВАЖНО: При прикреплении одиночек ПРОВЕРЯЕМ ГЕОГРАФИЮ!
Запросы с гео НЕ должны попадать в кластеры без гео и наоборот.
"""

from typing import Dict, List, Set, Optional

import pandas as pd

from seo_analyzer.core.config import CLUSTERING_CONFIG
from .postprocessing import ClusterSplitter, SingletonReattacher

# Получаем дефолтное значение из конфига
_DEFAULT_MAX_CLUSTER_SIZE = CLUSTERING_CONFIG.get("postprocess", {}).get("max_cluster_size", 12)


class ClusterPostprocessor:
    """
    Перераспределяет кластеры после основной SERP-кластеризации.

    Логика:
    1) Если в кластере больше max_cluster_size запросов, постепенно
       повышаем порог общих URL и делим кластер на компоненты связности.
    2) Одиночные запросы повторно пробуем прикрепить к существующим
       кластерам, если найдено достаточное пересечение URL.
    3) ⚠️ ВАЖНО: При прикреплении одиночек ПРОВЕРЯЕМ ГЕОГРАФИЮ!
       Запросы с гео НЕ должны попадать в кластеры без гео и наоборот.
    """

    def __init__(
        self,
        base_threshold: int,
        top_positions: int,
        max_cluster_size: int = _DEFAULT_MAX_CLUSTER_SIZE,
        threshold_step: int = 1,
        max_threshold: int | None = None,
        max_compare: int = 15,
        geo_dicts: Optional[Dict[str, Set[str]]] = None,
        skip_singleton_reattach: bool = False,
    ):
        """
        Args:
            base_threshold: исходный порог общих URL.
            top_positions: глубина SERP, используемая при сравнении.
            max_cluster_size: допустимый размер кластера.
            threshold_step: шаг повышения порога при делении.
            max_threshold: верхняя граница порога (по умолчанию base+10).
            max_compare: сколько запросов в кластере сравнивать при
                         повторном присоединении одиночек.
            geo_dicts: Словари с географическими названиями для проверки гео.
            skip_singleton_reattach: Пропустить этап прикрепления одиночек (ускорение).
        """
        self.base_threshold = max(1, base_threshold)
        self.top_positions = max(1, top_positions)
        self.max_cluster_size = max_cluster_size
        self.skip_singleton_reattach = skip_singleton_reattach
        self._stats: Dict[str, int] = {}
        
        # Инициализируем компоненты
        self.splitter = ClusterSplitter(
            base_threshold=base_threshold,
            top_positions=top_positions,
            max_cluster_size=max_cluster_size,
            threshold_step=threshold_step,
            max_threshold=max_threshold,
        )
        
        # Reattacher нужен только если не пропускаем прикрепление
        self.reattacher = None
        if not skip_singleton_reattach:
            self.reattacher = SingletonReattacher(
                base_threshold=base_threshold,
                top_positions=top_positions,
                max_cluster_size=max_cluster_size,
                max_compare=max_compare,
                geo_dicts=geo_dicts,
            )

    def _normalize_urls(self, value) -> List[str]:
        """Нормализует SERP URL из DataFrame."""
        if value is None:
            return []
        if isinstance(value, list):
            urls = value
        else:
            try:
                if pd.isna(value):
                    return []
            except Exception:
                pass
            urls = str(value).split("|")
        
        cleaned: List[str] = []
        for url in urls[: self.top_positions]:
            if not url:
                continue
            if isinstance(url, str):
                cleaned.append(url.strip())
        return cleaned

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Выполняет пост-обработку кластеров.
        
        1. Собирает кластеры из DataFrame
        2. Делит большие кластеры (> max_cluster_size)
        3. Прикрепляет одиночные запросы с проверкой географии
        4. Обновляет DataFrame с новыми кластерами
        """
        # Шаг 1: Собираем кластеры из DataFrame
        clusters: Dict[int, List[str]] = {}
        query_urls: Dict[str, List[str]] = {}
        
        for _, row in df.iterrows():
            cid = int(row["semantic_cluster_id"])
            q = row["keyword"]
            clusters.setdefault(cid, []).append(q)
            query_urls[q] = self._normalize_urls(row.get("serp_urls"))

        # Шаг 2: Делим большие кластеры
        split_clusters: List[List[str]] = []
        for _, queries in clusters.items():
            split_clusters.extend(self.splitter.split_cluster(queries, query_urls))

        # Шаг 3: Прикрепляем одиночные запросы (с проверкой географии!)
        if self.skip_singleton_reattach or self.reattacher is None:
            # ОПТИМИЗАЦИЯ: Пропускаем прикрепление одиночек для ускорения
            final_clusters = split_clusters
        else:
            final_clusters = self.reattacher.reattach_singletons(split_clusters, query_urls)

        # Шаг 4: Обновляем DataFrame
        freq = df.set_index("keyword")["frequency_world"].to_dict()
        query_to_cluster: Dict[str, int] = {}
        cluster_names: Dict[int, str] = {}
        
        for new_cid, queries in enumerate(final_clusters):
            for q in queries:
                query_to_cluster[q] = new_cid
            # Главный запрос кластера = запрос с максимальной частотностью
            main = max(queries, key=lambda x: freq.get(x, 0))
            cluster_names[new_cid] = main

        df = df.copy()
        df["semantic_cluster_id"] = df["keyword"].map(query_to_cluster)
        df["cluster_name"] = df["semantic_cluster_id"].map(cluster_names)

        # Статистика
        sizes = [len(qs) for qs in final_clusters]
        self._stats = {
            "total_clusters": len(final_clusters),
            "max_cluster_size": max(sizes) if sizes else 0,
            "singleton_clusters": sum(1 for s in sizes if s == 1),
        }
        
        return df

    def get_stats(self) -> Dict[str, int]:
        """Возвращает статистику пост-обработки."""
        return self._stats
