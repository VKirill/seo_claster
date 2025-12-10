"""
Деление слишком больших кластеров на подкластеры.

Если кластер превышает max_cluster_size, постепенно повышаем порог
общих URL и делим кластер на компоненты связности.

ОПТИМИЗАЦИЯ: Использует set-based операции и кэширование для ускорения.
"""

from functools import lru_cache
from typing import Dict, List, Sequence, Set

from seo_analyzer.core.config import CLUSTERING_CONFIG

# Получаем дефолтное значение из конфига
_DEFAULT_MAX_CLUSTER_SIZE = CLUSTERING_CONFIG.get("postprocess", {}).get("max_cluster_size", 12)


class ClusterSplitter:
    """Делит большие кластеры на подкластеры по компонентам связности."""

    def __init__(
        self,
        base_threshold: int,
        top_positions: int,
        max_cluster_size: int = _DEFAULT_MAX_CLUSTER_SIZE,
        threshold_step: int = 1,
        max_threshold: int | None = None,
    ):
        """
        Args:
            base_threshold: исходный порог общих URL.
            top_positions: глубина SERP, используемая при сравнении.
            max_cluster_size: допустимый размер кластера.
            threshold_step: шаг повышения порога при делении.
            max_threshold: верхняя граница порога (по умолчанию base+10).
        """
        self.base_threshold = max(1, base_threshold)
        self.top_positions = max(1, top_positions)
        self.max_cluster_size = max_cluster_size
        self.threshold_step = max(1, threshold_step)
        self.max_threshold = max_threshold or min(self.top_positions, self.base_threshold + 10)

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

    def _components(
        self, queries: List[str], query_urls: Dict[str, List[str]], threshold: int
    ) -> List[List[str]]:
        """
        Находит компоненты связности в графе запросов.
        
        Два запроса связаны если у них >= threshold общих URL.
        
        ОПТИМИЗАЦИЯ: Предварительно конвертируем URL в sets для быстрого пересечения.
        """
        graph: Dict[str, Set[str]] = {q: set() for q in queries}
        
        # ОПТИМИЗАЦИЯ: Предварительно конвертируем все URL в sets (O(n) вместо O(n²))
        url_sets: Dict[str, Set[str]] = {}
        for q in queries:
            urls = query_urls.get(q, [])
            url_sets[q] = set(urls[: self.top_positions]) if urls else set()
        
        # Строим граф используя быстрое пересечение множеств
        for i, q1 in enumerate(queries):
            set1 = url_sets[q1]
            if not set1:  # Пропускаем пустые запросы
                continue
            
            for q2 in queries[i + 1 :]:
                set2 = url_sets[q2]
                if not set2:  # Пропускаем пустые запросы
                    continue
                
                # Быстрое пересечение множеств вместо повторных вызовов _similarity
                if len(set1 & set2) >= threshold:
                    graph[q1].add(q2)
                    graph[q2].add(q1)
        
        # Находим компоненты связности (DFS)
        components: List[List[str]] = []
        visited: Set[str] = set()
        
        for start in queries:
            if start in visited:
                continue
            
            stack = [start]
            comp: List[str] = []
            
            while stack:
                node = stack.pop()
                if node in visited:
                    continue
                visited.add(node)
                comp.append(node)
                stack.extend(graph[node] - visited)
            
            components.append(comp)
        
        return components

    def split_cluster(
        self, queries: List[str], query_urls: Dict[str, List[str]]
    ) -> List[List[str]]:
        """
        Делит большой кластер на подкластеры.
        
        Постепенно повышает порог общих URL до тех пор, пока
        не получится разделить кластер на части <= max_cluster_size.
        """
        if len(queries) <= self.max_cluster_size:
            return [queries]
        
        threshold = self.base_threshold + self.threshold_step
        best_split = [queries]
        
        # Пытаемся найти хорошее разделение
        while threshold <= self.max_threshold:
            parts = self._components(queries, query_urls, threshold)
            largest = max(len(c) for c in parts)
            
            # Сохраняем лучшее разделение
            if largest < len(best_split[0]):
                best_split = parts
            
            # Если получилось разделить - отлично!
            if largest <= self.max_cluster_size and len(parts) > 1:
                return parts
            
            threshold += self.threshold_step
        
        # Если получилось разделить на допустимые размеры - возвращаем
        if max(len(c) for c in best_split) <= self.max_cluster_size:
            return best_split
        
        # Иначе принудительно делим по размеру
        forced: List[List[str]] = []
        for comp in best_split:
            if len(comp) <= self.max_cluster_size:
                forced.append(comp)
            else:
                # Принудительное деление на куски по max_cluster_size
                for i in range(0, len(comp), self.max_cluster_size):
                    forced.append(comp[i : i + self.max_cluster_size])
        
        return forced

