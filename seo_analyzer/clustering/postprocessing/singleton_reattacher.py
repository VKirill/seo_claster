"""
Прикрепление одиночных запросов к существующим кластерам.

⚠️ ВАЖНО: Проверяет географическую совместимость!
Запросы с гео НЕ должны попадать в кластеры без гео и наоборот.

ОПТИМИЗАЦИЯ: Использует set-based операции, кэширование и ранний выход.
"""

from functools import lru_cache
from typing import Dict, List, Optional, Sequence, Set

from seo_analyzer.core.config import CLUSTERING_CONFIG

# Получаем дефолтное значение из конфига
_DEFAULT_MAX_CLUSTER_SIZE = CLUSTERING_CONFIG.get("postprocess", {}).get("max_cluster_size", 12)


class SingletonReattacher:
    """
    Прикрепляет одиночные запросы к существующим кластерам.
    
    КЛЮЧЕВАЯ ОСОБЕННОСТЬ: Проверяет географию перед прикреплением!
    Одиночные запросы с гео могут прикрепляться только к кластерам с той же гео,
    а запросы без гео - только к кластерам без гео.
    """

    def __init__(
        self,
        base_threshold: int,
        top_positions: int,
        max_cluster_size: int = _DEFAULT_MAX_CLUSTER_SIZE,
        max_compare: int = 15,
        geo_dicts: Optional[Dict[str, Set[str]]] = None,
    ):
        """
        Args:
            base_threshold: минимальный порог общих URL для прикрепления.
            top_positions: глубина SERP, используемая при сравнении.
            max_cluster_size: максимальный размер кластера.
            max_compare: сколько запросов в кластере сравнивать.
            geo_dicts: Словари с географическими названиями для проверки гео.
        """
        self.base_threshold = max(1, base_threshold)
        self.top_positions = max(1, top_positions)
        self.max_cluster_size = max_cluster_size
        self.max_compare = max_compare
        
        # Инициализируем семантический чекер для проверки гео
        from ..semantic_checker import SemanticClusterChecker
        self.semantic_checker = SemanticClusterChecker(geo_dicts=geo_dicts) if geo_dicts else None

    @lru_cache(maxsize=10000)
    def _similarity_cached(self, urls_a: tuple, urls_b: tuple) -> int:
        """
        Кэшируемая версия _similarity для ускорения повторных вычислений.
        
        ОПТИМИЗАЦИЯ: Использует tuple для хэширования и кэширования результатов.
        """
        if not urls_a or not urls_b:
            return 0
        return len(set(urls_a) & set(urls_b))
    
    def _similarity(self, urls_a: Sequence[str], urls_b: Sequence[str]) -> int:
        """
        Рассчитывает количество общих URL между двумя запросами.
        
        ОПТИМИЗАЦИЯ: Использует кэшированную версию для ускорения.
        """
        if not urls_a or not urls_b:
            return 0
        
        # Конвертируем в tuple для кэширования
        tuple_a = tuple(urls_a[: self.top_positions])
        tuple_b = tuple(urls_b[: self.top_positions])
        
        return self._similarity_cached(tuple_a, tuple_b)

    def reattach_singletons(
        self, clusters: List[List[str]], query_urls: Dict[str, List[str]]
    ) -> List[List[str]]:
        """
        Прикрепляет одиночные запросы к существующим кластерам.
        
        ⚠️ ВАЖНО: Проверяет географическую совместимость!
        Запросы с гео НЕ должны попадать в кластеры без гео и наоборот.
        
        ОПТИМИЗАЦИЯ: Предварительно кэшируем URL sets для быстрого сравнения.
        """
        singles = [c[0] for c in clusters if len(c) == 1]
        non_single_clusters = [c for c in clusters if len(c) > 1]
        
        # ОПТИМИЗАЦИЯ: Предварительно конвертируем все URL в sets (O(n) вместо O(n³))
        url_sets: Dict[str, Set[str]] = {}
        all_queries = singles + [q for cluster in non_single_clusters for q in cluster]
        for q in all_queries:
            urls = query_urls.get(q, [])
            url_sets[q] = set(urls[: self.top_positions]) if urls else set()
        
        # Извлекаем географию для всех запросов (если чекер доступен)
        query_geo_dict = {}
        if self.semantic_checker:
            for single in singles:
                query_geo_dict[single] = self.semantic_checker.extract_geo(single)
            for cluster in non_single_clusters:
                for query in cluster:
                    if query not in query_geo_dict:
                        query_geo_dict[query] = self.semantic_checker.extract_geo(query)
        
        # Прикрепляем каждую одиночку к лучшему кластеру
        for single in singles:
            best_cid = None
            best_score = 0
            set_single = url_sets.get(single, set())
            single_geo = query_geo_dict.get(single)
            
            # ОПТИМИЗАЦИЯ: Пропускаем пустые одиночки
            if not set_single:
                non_single_clusters.append([single])
                continue
            
            for idx, cluster in enumerate(non_single_clusters):
                # Пропускаем переполненные кластеры
                if len(cluster) >= self.max_cluster_size:
                    continue
                
                # 🌍 ПРОВЕРКА ГЕОГРАФИИ: одиночка должен иметь ТУ ЖЕ географию что и кластер
                if self.semantic_checker:
                    cluster_geo = query_geo_dict.get(cluster[0])  # География кластера = география первого запроса
                    
                    # Если география не совпадает - ПРОПУСКАЕМ этот кластер
                    if single_geo != cluster_geo:
                        continue
                
                # ОПТИМИЗАЦИЯ: Быстрое сравнение через set intersection
                score = 0
                for member in cluster[: self.max_compare]:
                    set_member = url_sets.get(member, set())
                    if not set_member:  # Пропускаем пустые члены
                        continue
                    
                    # Быстрое пересечение множеств
                    intersection_size = len(set_single & set_member)
                    if intersection_size > score:
                        score = intersection_size
                    
                    # ОПТИМИЗАЦИЯ: Ранний выход - нашли достаточное совпадение
                    if score >= self.base_threshold:
                        break
                
                # Сохраняем лучший вариант
                if score > best_score and score >= self.base_threshold:
                    best_score = score
                    best_cid = idx
                    
                    # ОПТИМИЗАЦИЯ: Если нашли идеальное совпадение - прекращаем поиск
                    if score == len(set_single):
                        break
            
            # Прикрепляем к лучшему кластеру или создаем новый
            if best_cid is not None:
                non_single_clusters[best_cid].append(single)
            else:
                non_single_clusters.append([single])
        
        return non_single_clusters
