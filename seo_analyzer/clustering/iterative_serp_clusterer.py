"""
Итеративная SERP кластеризация от большего к меньшему порогу
Сначала формируются кластеры с максимальными связями (20 URL), затем постепенно снижается порог до 4 URL
"""

from typing import List, Dict, Set, Tuple, Optional
import pandas as pd
from collections import defaultdict

from .semantic_checker import SemanticClusterChecker
from .fast_similarity import FastSimilarityCalculator


class IterativeSERPClusterer:
    """
    Итеративная SERP кластеризация от большего к меньшему порогу
    
    Алгоритм:
    1. Начинаем с максимального порога (20 общих URL)
    2. На каждой итерации пытаемся добавить запросы в существующие кластеры
       или создать новые кластеры с текущим порогом
    3. Постепенно снижаем порог до минимума (4 общих URL)
    4. На каждой итерации обрабатываем только необработанные запросы
    
    Это гарантирует, что сначала формируются самые сильные связи,
    а затем к ним присоединяются запросы с меньшими связями.
    """
    
    def __init__(
        self,
        min_threshold: int = 4,
        max_threshold: int = 20,
        top_positions: int = 20,
        max_cluster_size: int = 100,
        semantic_check: bool = True,
        geo_dicts: Dict[str, Set[str]] = None,
        verbose: bool = False
    ):
        """
        Args:
            min_threshold: Минимальный порог общих URL (по умолчанию 4)
            max_threshold: Максимальный порог общих URL (по умолчанию 20)
            top_positions: Глубина анализа SERP (по умолчанию 20)
            max_cluster_size: Максимальный размер кластера (по умолчанию 100)
            semantic_check: Проверять семантическую схожесть запросов
            geo_dicts: Словари с географическими названиями для проверки
            verbose: Выводить отладочную информацию
        """
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold
        self.top_positions = top_positions
        self.max_cluster_size = max_cluster_size
        self.semantic_check = semantic_check
        self.verbose = verbose
        
        self.clusters = {}  # query -> cluster_id
        self.cluster_queries = defaultdict(list)  # cluster_id -> [queries]
        
        # Семантический чекер для проверки совместимости
        self.semantic_checker = SemanticClusterChecker(geo_dicts=geo_dicts) if semantic_check else None
        
        # Быстрый калькулятор схожести
        self.fast_similarity = FastSimilarityCalculator(top_positions=top_positions)
    
    def _normalize_url(self, url: str) -> str:
        """Нормализует URL для сравнения"""
        if not url:
            return ""
        url = url.replace("https://", "").replace("http://", "")
        url = url.replace("www.", "")
        url = url.split("?")[0].split("#")[0]
        return url.rstrip("/").lower()
    
    def _extract_urls_from_serp(self, serp_data) -> List[str]:
        """Извлекает список URL из SERP данных"""
        if not serp_data:
            return []
        
        urls = []
        if isinstance(serp_data, list):
            for item in serp_data:
                if isinstance(item, dict):
                    url = item.get('url', '') or item.get('link', '')
                elif isinstance(item, str):
                    url = item
                else:
                    continue
                if url:
                    normalized = self._normalize_url(url)
                    if normalized:
                        urls.append(normalized)
        elif isinstance(serp_data, str):
            # Попытка распарсить JSON строку
            import json
            try:
                data = json.loads(serp_data)
                if isinstance(data, list):
                    return self._extract_urls_from_serp(data)
            except:
                pass
        
        return urls[:self.top_positions]
    
    def _calculate_url_overlap(self, urls1: List[str], urls2: List[str]) -> int:
        """Вычисляет количество общих URL между двумя списками"""
        set1 = set(urls1[:self.top_positions])
        set2 = set(urls2[:self.top_positions])
        return len(set1 & set2)
    
    def _calculate_url_ids_overlap(self, url_ids1: Set[int], url_ids2: Set[int]) -> int:
        """Вычисляет количество общих URL между двумя множествами числовых ID (быстрая версия)"""
        return len(url_ids1 & url_ids2)
    
    def _can_add_to_cluster(
        self,
        query: str,
        cluster_queries: List[str],
        query_urls_dict: Dict[str, List[str]],
        threshold: int
    ) -> bool:
        """
        Проверяет может ли запрос быть добавлен в кластер
        БЕЗ транзитивного замыкания - требуется прямая связь со ВСЕМИ запросами в кластере
        
        ВАЖНО: Если кластер состоит из двух запросов со связью >= threshold * 2,
        то новый запрос может быть добавлен только если у него тоже есть связь >= threshold * 2
        с обоими запросами в кластере (защита от добавления слабых связей в сильные кластеры).
        """
        if not cluster_queries:
            return True
        
        query_urls = query_urls_dict.get(query, [])
        if not query_urls:
            return False
        
        strong_bond_threshold = threshold * 2
        
        # Специальная проверка для кластеров из двух запросов с сильной связью
        if len(cluster_queries) == 2:
            cluster_query1_urls = query_urls_dict.get(cluster_queries[0], [])
            cluster_query2_urls = query_urls_dict.get(cluster_queries[1], [])
            cluster_bond = self._calculate_url_overlap(cluster_query1_urls, cluster_query2_urls)
            
            # Если связь между запросами в кластере очень сильная (>= strong_bond_threshold),
            # то новый запрос может быть добавлен только если у него тоже очень сильная связь
            if cluster_bond >= strong_bond_threshold:
                overlap1 = self._calculate_url_overlap(query_urls, cluster_query1_urls)
                overlap2 = self._calculate_url_overlap(query_urls, cluster_query2_urls)
                
                # Оба должны быть >= strong_bond_threshold
                if overlap1 < strong_bond_threshold or overlap2 < strong_bond_threshold:
                    return False
        
        # Проверяем связь со ВСЕМИ запросами в кластере
        for cluster_query in cluster_queries:
            cluster_query_urls = query_urls_dict.get(cluster_query, [])
            overlap = self._calculate_url_overlap(query_urls, cluster_query_urls)
            
            # Если хотя бы с одним запросом недостаточно общих URL - отказ
            if overlap < threshold:
                return False
            
            # Семантическая проверка (если включена)
            if self.semantic_checker:
                compatible, reason = self.semantic_checker.are_queries_compatible(
                    query, cluster_query, check_geo=True
                )
                if not compatible:
                    return False
        
        # Все проверки пройдены - запрос связан со ВСЕМИ запросами в кластере
        return True
    
    def _can_add_to_cluster_fast(
        self,
        query: str,
        cluster_queries: List[str],
        query_url_ids_dict: Dict[str, Set[int]],
        threshold: int
    ) -> bool:
        """
        Быстрая версия проверки добавления запроса в кластер (использует числовые ID URL)
        
        Проверяет может ли запрос быть добавлен в кластер
        БЕЗ транзитивного замыкания - требуется прямая связь со ВСЕМИ запросами в кластере
        
        ВАЖНО: Если кластер состоит из двух запросов со связью >= threshold * 2,
        то новый запрос может быть добавлен только если у него тоже есть связь >= threshold * 2
        с обоими запросами в кластере (защита от добавления слабых связей в сильные кластеры).
        """
        if not cluster_queries:
            return True
        
        query_url_ids = query_url_ids_dict.get(query)
        if not query_url_ids:
            return False
        
        strong_bond_threshold = threshold * 2
        
        # Специальная проверка для кластеров из двух запросов с сильной связью
        if len(cluster_queries) == 2:
            cluster_query1_url_ids = query_url_ids_dict.get(cluster_queries[0])
            cluster_query2_url_ids = query_url_ids_dict.get(cluster_queries[1])
            
            if cluster_query1_url_ids and cluster_query2_url_ids:
                cluster_bond = self._calculate_url_ids_overlap(cluster_query1_url_ids, cluster_query2_url_ids)
                
                # Если связь между запросами в кластере очень сильная (>= strong_bond_threshold),
                # то новый запрос может быть добавлен только если у него тоже очень сильная связь
                if cluster_bond >= strong_bond_threshold:
                    overlap1 = self._calculate_url_ids_overlap(query_url_ids, cluster_query1_url_ids)
                    overlap2 = self._calculate_url_ids_overlap(query_url_ids, cluster_query2_url_ids)
                    
                    # Оба должны быть >= strong_bond_threshold
                    if overlap1 < strong_bond_threshold or overlap2 < strong_bond_threshold:
                        return False
        
        # Проверяем связь со ВСЕМИ запросами в кластере
        for cluster_query in cluster_queries:
            cluster_query_url_ids = query_url_ids_dict.get(cluster_query)
            if not cluster_query_url_ids:
                return False
            
            overlap = self._calculate_url_ids_overlap(query_url_ids, cluster_query_url_ids)
            
            # Если хотя бы с одним запросом недостаточно общих URL - отказ
            if overlap < threshold:
                return False
            
            # Семантическая проверка (если включена)
            if self.semantic_checker:
                compatible, reason = self.semantic_checker.are_queries_compatible(
                    query, cluster_query, check_geo=True
                )
                if not compatible:
                    return False
        
        # Все проверки пройдены - запрос связан со ВСЕМИ запросами в кластере
        return True
    
    async def cluster_by_serp(
        self,
        df: pd.DataFrame,
        serp_column: str = 'serp_urls',
        geo_processor=None
    ) -> pd.DataFrame:
        """
        Выполняет итеративную кластеризацию запросов по SERP
        
        Args:
            df: DataFrame с запросами
            serp_column: Название колонки с SERP данными
            geo_processor: Процессор географии (опционально)
        
        Returns:
            DataFrame с добавленными колонками cluster_id и cluster_name
        """
        if len(df) == 0:
            return df
        
        # Ждем завершения geo_processor если он есть
        if geo_processor is not None:
            await geo_processor.get_result()
        
        # Извлекаем URL для каждого запроса
        query_urls_dict = {}
        queries = df['keyword'].tolist()
        
        for idx, row in df.iterrows():
            query = row['keyword']
            serp_data = row.get(serp_column)
            urls = self._extract_urls_from_serp(serp_data)
            if urls:
                query_urls_dict[query] = urls
        
        if not query_urls_dict:
            if self.verbose:
                print("⚠️  Нет SERP данных для кластеризации")
            df['semantic_cluster_id'] = -1
            df['cluster_name'] = df['keyword']
            return df
        
        if self.verbose:
            print(f"📥 Загружено URL для {len(query_urls_dict)} запросов")
        
        # ОПТИМИЗАЦИЯ: Создаем числовые ID для всех уникальных URL
        # Это ускоряет сравнение в 5-10 раз (сравнение int вместо строк)
        url_to_id = {}  # normalized_url -> int
        url_id_counter = 0
        
        for query, urls in query_urls_dict.items():
            for url in urls[:self.top_positions]:
                if url not in url_to_id:
                    url_to_id[url] = url_id_counter
                    url_id_counter += 1
        
        if self.verbose:
            print(f"🔢 Создано {len(url_to_id)} уникальных URL ID")
        
        # Преобразуем списки URL в множества числовых ID для быстрого сравнения
        query_url_ids_dict = {}  # query -> Set[int]
        for query, urls in query_urls_dict.items():
            url_ids = {url_to_id[url] for url in urls[:self.top_positions] if url in url_to_id}
            query_url_ids_dict[query] = url_ids
        
        # Итеративная кластеризация
        clusters = []
        processed = set()  # Запросы, которые уже попали в кластеры
        query_to_cluster = {}  # query -> cluster_idx
        
        # Итерации от максимального порога к минимальному
        for threshold in range(self.max_threshold, self.min_threshold - 1, -1):
            if self.verbose:
                unprocessed_count = len(queries) - len(processed)
                if unprocessed_count > 0:
                    print(f"\n🔍 Итерация: порог = {threshold} общих URL (необработано: {unprocessed_count})")
            
            # Обрабатываем только необработанные запросы
            unprocessed_queries = [q for q in queries if q not in processed and q in query_urls_dict]
            
            if not unprocessed_queries:
                if self.verbose:
                    print(f"   ✓ Все запросы обработаны, переход к следующему порогу")
                continue
            
            # Находим пары запросов с текущим порогом общих URL
            # ОПТИМИЗАЦИЯ: Инвертированный индекс - находим только кандидатов с общими URL
            # Вместо O(n²) сравнений делаем O(n × k), где k - среднее количество URL (~20-30)
            
            # Строим инвертированный индекс: URL ID → список запросов
            url_id_to_queries = defaultdict(set)
            for query in unprocessed_queries:
                url_ids = query_url_ids_dict.get(query)
                if url_ids:
                    for url_id in url_ids:
                        url_id_to_queries[url_id].add(query)
            
            # Находим пары через индекс (только кандидаты с общими URL)
            pairs = []
            seen_pairs = set()  # Избегаем дубликатов
            
            for query1 in unprocessed_queries:
                query1_url_ids = query_url_ids_dict.get(query1)
                if not query1_url_ids:
                    continue
                
                # Находим кандидатов - запросы с общими URL
                candidate_counts = defaultdict(int)
                for url_id in query1_url_ids:
                    for candidate in url_id_to_queries[url_id]:
                        if candidate != query1 and candidate > query1:  # Избегаем дубликатов
                            candidate_counts[candidate] += 1
                
                # Проверяем только кандидатов с достаточным количеством общих URL
                for query2, common_urls_count in candidate_counts.items():
                    if common_urls_count < threshold:
                        continue
                    
                    # Проверяем точное пересечение (может быть больше чем common_urls_count)
                    query2_url_ids = query_url_ids_dict.get(query2)
                    if not query2_url_ids:
                        continue
                    
                    # БЫСТРОЕ пересечение множеств чисел
                    overlap = self._calculate_url_ids_overlap(query1_url_ids, query2_url_ids)
                    if overlap >= threshold:
                        pair_key = (query1, query2) if query1 < query2 else (query2, query1)
                        if pair_key not in seen_pairs:
                            seen_pairs.add(pair_key)
                            pairs.append((query1, query2, overlap))
            
            # Сортируем пары по убыванию общих URL
            pairs.sort(key=lambda x: x[2], reverse=True)
            
            if self.verbose and pairs:
                print(f"   📊 Найдено пар с >= {threshold} общих URL: {len(pairs)}")
            
            # Обрабатываем пары, начиная с самых сильных связей
            for query1, query2, overlap in pairs:
                # Пропускаем, если оба запроса уже обработаны
                if query1 in processed and query2 in processed:
                    continue
                
                # Если оба запроса не обработаны - создаем новый кластер
                if query1 not in processed and query2 not in processed:
                    # ВАЖНО: Проверяем гео-совместимость перед созданием кластера
                    # Гео-запросы должны кластеризоваться отдельно от не-гео запросов
                    if self.semantic_checker:
                        compatible, reason = self.semantic_checker.are_queries_compatible(
                            query1, query2, check_geo=True
                        )
                        if not compatible:
                            # Гео-несовместимы - пропускаем эту пару
                            if self.verbose:
                                print(f"   ⚠️  Пропущена пара (гео-несовместимость): '{query1}' + '{query2}' ({reason})")
                            continue
                    
                    new_cluster = [query1, query2]
                    clusters.append(new_cluster)
                    cluster_idx = len(clusters) - 1
                    query_to_cluster[query1] = cluster_idx
                    query_to_cluster[query2] = cluster_idx
                    processed.add(query1)
                    processed.add(query2)
                    
                    if self.verbose:
                        print(f"   ✅ Создан кластер {cluster_idx + 1}: '{query1}' + '{query2}' ({overlap} URL)")
                    continue
                
                # Если один запрос обработан, а другой нет - пытаемся добавить в кластер
                if query1 in processed and query2 not in processed:
                    cluster_idx = query_to_cluster[query1]
                    cluster = clusters[cluster_idx]
                    
                    # Проверяем размер кластера
                    if len(cluster) >= self.max_cluster_size:
                        continue
                    
                    # ОПТИМИЗАЦИЯ: Используем быструю версию с числовыми ID
                    if self._can_add_to_cluster_fast(query2, cluster, query_url_ids_dict, threshold):
                        cluster.append(query2)
                        query_to_cluster[query2] = cluster_idx
                        processed.add(query2)
                        if self.verbose:
                            print(f"   ✅ Добавлен в кластер {cluster_idx + 1}: '{query2}' (связь с '{query1}': {overlap} URL)")
                
                elif query2 in processed and query1 not in processed:
                    cluster_idx = query_to_cluster[query2]
                    cluster = clusters[cluster_idx]
                    
                    # Проверяем размер кластера
                    if len(cluster) >= self.max_cluster_size:
                        continue
                    
                    # ОПТИМИЗАЦИЯ: Используем быструю версию с числовыми ID
                    if self._can_add_to_cluster_fast(query1, cluster, query_url_ids_dict, threshold):
                        cluster.append(query1)
                        query_to_cluster[query1] = cluster_idx
                        processed.add(query1)
                        if self.verbose:
                            print(f"   ✅ Добавлен в кластер {cluster_idx + 1}: '{query1}' (связь с '{query2}': {overlap} URL)")
        
        # Добавляем оставшиеся запросы как отдельные кластеры
        for query in queries:
            if query not in processed and query in query_urls_dict:
                clusters.append([query])
                query_to_cluster[query] = len(clusters) - 1
                processed.add(query)
        
        # Создаем mapping для DataFrame
        cluster_id_map = {}
        cluster_name_map = {}
        
        for cluster_idx, cluster in enumerate(clusters):
            # Имя кластера - первый запрос (или самый частотный)
            cluster_name = cluster[0]
            for query in cluster:
                cluster_id_map[query] = cluster_idx
                cluster_name_map[query] = cluster_name
        
        # Добавляем колонки в DataFrame
        df['semantic_cluster_id'] = df['keyword'].map(cluster_id_map).fillna(-1).astype(int)
        df['cluster_name'] = df['keyword'].map(cluster_name_map).fillna(df['keyword'])
        
        # Сохраняем статистику
        self.clusters = cluster_id_map
        self.cluster_queries = {i: cluster for i, cluster in enumerate(clusters)}
        
        if self.verbose:
            print(f"\n✓ Создано кластеров: {len(clusters)}")
        
        return df
    
    def get_cluster_stats(self) -> Dict:
        """Возвращает статистику кластеризации"""
        if not self.cluster_queries:
            return {}
        
        cluster_sizes = [len(cluster) for cluster in self.cluster_queries.values()]
        
        return {
            'total_clusters': len(self.cluster_queries),
            'avg_cluster_size': sum(cluster_sizes) / len(cluster_sizes) if cluster_sizes else 0,
            'min_cluster_size': min(cluster_sizes) if cluster_sizes else 0,
            'max_cluster_size': max(cluster_sizes) if cluster_sizes else 0,
            'singleton_clusters': sum(1 for size in cluster_sizes if size == 1)
        }
    
    def get_cluster_url_overlaps(self, df: pd.DataFrame, serp_column: str = 'serp_urls') -> Dict[int, List[Dict]]:
        """
        Возвращает точное количество общих URL для каждой пары запросов в каждом кластере
        
        Args:
            df: DataFrame с запросами и SERP данными
            serp_column: Название колонки с SERP данными
        
        Returns:
            Dict[cluster_id -> List[Dict]] где каждый Dict содержит:
            {
                'query1': str,
                'query2': str,
                'overlap': int
            }
        """
        result = {}
        
        for cluster_id, cluster_queries in self.cluster_queries.items():
            if len(cluster_queries) <= 1:
                continue
            
            overlaps = []
            
            # Получаем URL для каждого запроса в кластере
            query_urls_dict = {}
            for query in cluster_queries:
                row = df[df['keyword'] == query]
                if not row.empty:
                    serp_data = row.iloc[0].get(serp_column)
                    urls = self._extract_urls_from_serp(serp_data)
                    if urls:
                        query_urls_dict[query] = urls
            
            # Вычисляем overlaps для всех пар запросов
            queries_list = list(cluster_queries)
            for i, query1 in enumerate(queries_list):
                for query2 in queries_list[i+1:]:
                    if query1 in query_urls_dict and query2 in query_urls_dict:
                        overlap = self._calculate_url_overlap(
                            query_urls_dict[query1],
                            query_urls_dict[query2]
                        )
                        overlaps.append({
                            'query1': query1,
                            'query2': query2,
                            'overlap': overlap
                        })
            
            if overlaps:
                result[cluster_id] = overlaps
        
        return result

