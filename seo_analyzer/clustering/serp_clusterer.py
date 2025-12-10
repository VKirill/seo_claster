"""
SOFT-кластеризация запросов на основе пересечения URL в SERP
"""
from typing import List, Dict, Set, Tuple
import pandas as pd
from collections import defaultdict


class SERPClusterer:
    """
    Мягкая (SOFT) кластеризация по схожести результатов поисковой выдачи
    
    Принцип: если два запроса имеют N+ общих URL в топ-30, они в одном кластере
    """
    
    def __init__(self, min_common_urls: int = 7, top_positions: int = 30, max_cluster_size: int = 100, strict_mode: bool = False):
        """
        Args:
            min_common_urls: Минимум общих URL для группировки (по умолчанию 7)
            top_positions: Глубина анализа - сколько позиций учитывать (по умолчанию 30)
            max_cluster_size: Максимальный размер кластера (по умолчанию 100)
            strict_mode: Строгий режим - требовать схожести ВСЕХ запросов в кластере (по умолчанию False)
        """
        self.min_common_urls = min_common_urls
        self.top_positions = top_positions
        self.max_cluster_size = max_cluster_size
        self.strict_mode = strict_mode
        self.clusters = {}  # query -> cluster_id
        self.cluster_queries = defaultdict(list)  # cluster_id -> [queries]
        
    def _split_large_cluster(self, queries: List[str], query_urls: Dict[str, Set[str]]) -> List[List[str]]:
        """
        Разбивает слишком большой кластер на подкластеры
        
        Args:
            queries: Список запросов в большом кластере
            query_urls: Словарь query -> set(urls)
            
        Returns:
            Список подкластеров
        """
        # Используем агломеративную кластеризацию для разбиения
        # Сортируем по частотности (если доступно) или алфавиту
        sorted_queries = sorted(queries)
        
        subclusters = []
        current_subcluster = [sorted_queries[0]]
        
        for query in sorted_queries[1:]:
            # Проверяем схожесть с текущим подкластером
            # Требуем схожести хотя бы с одним запросом из подкластера
            has_similarity = False
            for member in current_subcluster:
                common = query_urls[query] & query_urls[member]
                if len(common) >= self.min_common_urls:
                    has_similarity = True
                    break
            
            if has_similarity and len(current_subcluster) < self.max_cluster_size:
                current_subcluster.append(query)
            else:
                # Начинаем новый подкластер
                if len(current_subcluster) > 0:
                    subclusters.append(current_subcluster)
                current_subcluster = [query]
        
        # Добавляем последний подкластер
        if len(current_subcluster) > 0:
            subclusters.append(current_subcluster)
        
        return subclusters
    
    def extract_serp_urls(self, serp_data: any) -> Set[str]:
        """
        Извлекает список URL из SERP данных
        
        Args:
            serp_data: SERP данные (может быть список URL/строк)
            
        Returns:
            Множество нормализованных URL из топ-N позиций
        """
        # Проверка на пустоту/NaN
        if serp_data is None:
            return set()
        
        # Для скалярных значений используем pd.isna
        if not isinstance(serp_data, (list, tuple)):
            if pd.isna(serp_data):
                return set()
            # Если это не список и не NaN, но и не строка - пустой результат
            if not isinstance(serp_data, str):
                return set()
        
        # Для списков проверяем длину
        if isinstance(serp_data, (list, tuple)) and len(serp_data) == 0:
            return set()
        
        # Если это список URL (строк)
        if isinstance(serp_data, list):
            urls = []
            for url in serp_data[:self.top_positions]:
                if url and isinstance(url, str):
                    norm_url = self._normalize_url(url)
                    if norm_url:
                        urls.append(norm_url)
            return set(urls)
        
        # Если это строка с доменами через запятую
        if isinstance(serp_data, str):
            urls = [self._normalize_url(d.strip()) for d in serp_data.split(',')]
            return set(urls[:self.top_positions])
        
        return set()
    
    def _normalize_url(self, url: str) -> str:
        """Нормализует URL (убирает протокол, www, но оставляет путь)"""
        if not url:
            return ""
        
        # Убираем протокол
        url = url.replace('https://', '').replace('http://', '')
        
        # Берём до первого пробела (на всякий случай)
        url = url.split(' ')[0]
        
        # Убираем www
        url = url.replace('www.', '')
        
        # Убираем trailing slash
        if url.endswith('/'):
            url = url[:-1]
        
        return url.lower()
    
    def calculate_similarity(self, urls1: Set[str], urls2: Set[str]) -> float:
        """
        Рассчитывает коэффициент схожести между двумя наборами URL
        
        Args:
            urls1: Первый набор URL
            urls2: Второй набор URL
            
        Returns:
            Коэффициент от 0 до 1 (процент пересечения)
        """
        if not urls1 or not urls2:
            return 0.0
        
        common = len(urls1 & urls2)  # Пересечение
        total = len(urls1 | urls2)   # Объединение
        
        return common / total if total > 0 else 0.0
    
    def cluster_by_serp(
        self,
        df: pd.DataFrame,
        serp_column: str = 'serp_main_pages'
    ) -> pd.DataFrame:
        """
        Выполняет SOFT-кластеризацию запросов по SERP
        
        Args:
            df: DataFrame с запросами и SERP данными
            serp_column: Название колонки с SERP данными
            
        Returns:
            DataFrame с добавленными колонками semantic_cluster_id и cluster_name
        """
        print(f"🔄 SOFT-кластеризация по SERP (порог: {self.min_common_urls} общих URL из топ-{self.top_positions})...")
        
        # Проверяем наличие SERP данных
        if serp_column not in df.columns:
            print(f"⚠️  Колонка '{serp_column}' не найдена. Пропускаем кластеризацию.")
            df['semantic_cluster_id'] = -1
            df['cluster_name'] = df['keyword']
            return df
        
        # Извлекаем URL из SERP для каждого запроса
        print("  📊 Извлечение URL из SERP...")
        query_urls = {}
        for idx, row in df.iterrows():
            query = row['keyword']
            serp_data = row[serp_column]
            urls = self.extract_serp_urls(serp_data)
            query_urls[query] = urls
        
        # Фильтруем запросы без SERP данных
        queries_with_serp = [q for q, urls in query_urls.items() if len(urls) > 0]
        print(f"  ✓ Запросов с SERP данными: {len(queries_with_serp)} из {len(df)}")
        
        if len(queries_with_serp) == 0:
            print("⚠️  Нет запросов с SERP данными. Кластеризация невозможна.")
            df['semantic_cluster_id'] = -1
            df['cluster_name'] = df['keyword']
            return df
        
        # Строим граф схожести через ИНВЕРТИРОВАННЫЙ ИНДЕКС (оптимизация для больших датасетов)
        print("  🔗 Построение графа схожести...")
        
        # Шаг 1: Строим инвертированный индекс (URL -> список запросов)
        url_to_queries = defaultdict(set)
        
        for query in queries_with_serp:
            for url in list(query_urls[query])[:self.top_positions]:  # Только топ-N URL
                url_to_queries[url].add(query)
        
        # Шаг 2: Создаем граф связей только для реальных кандидатов
        graph = defaultdict(set)
        
        for query1 in queries_with_serp:
            # Находим кандидатов - запросы с общими URL
            candidates = set()
            for url in list(query_urls[query1])[:self.top_positions]:
                candidates.update(url_to_queries[url])
            
            # Убираем сам запрос из кандидатов
            candidates.discard(query1)
            
            # Проверяем схожесть только с кандидатами
            for query2 in candidates:
                if query1 < query2:  # Избегаем дублирования (A-B и B-A)
                    common_urls = query_urls[query1] & query_urls[query2]
                    
                    if len(common_urls) >= self.min_common_urls:
                        # Добавляем ребро в граф (двунаправленное)
                        graph[query1].add(query2)
                        graph[query2].add(query1)
        
        # Шаг 2: Поиск компонент связности через итеративный DFS
        visited = set()
        cluster_id = 0
        
        def dfs_iterative(start_node):
            """Итеративный обход в глубину для поиска компоненты связности"""
            component = []
            stack = [start_node]
            
            while stack:
                node = stack.pop()
                
                if node in visited:
                    continue
                
                visited.add(node)
                component.append(node)
                
                # Добавляем всех соседей в стек
                for neighbor in graph.get(node, []):
                    if neighbor not in visited:
                        stack.append(neighbor)
            
            return component
        
        # Шаг 3: Обходим все запросы и находим компоненты
        for query in queries_with_serp:
            if query not in visited:
                # Новая компонента связности = новый кластер
                component = dfs_iterative(query)
                
                # Если кластер слишком большой - разбиваем его на подкластеры
                if len(component) > self.max_cluster_size:
                    # Разбиваем большой кластер на подкластеры по схожести
                    subclusters = self._split_large_cluster(component, query_urls)
                    for subcluster in subclusters:
                        for member in subcluster:
                            self.clusters[member] = cluster_id
                            self.cluster_queries[cluster_id].append(member)
                        cluster_id += 1
                else:
                    # Сохраняем кластер как есть
                    for member in component:
                        self.clusters[member] = cluster_id
                        self.cluster_queries[cluster_id].append(member)
                    cluster_id += 1
        
        # Запросы без SERP → каждый получает свой уникальный ID
        for idx, row in df.iterrows():
            query = row['keyword']
            if query not in self.clusters:
                self.clusters[query] = cluster_id
                self.cluster_queries[cluster_id].append(query)
                cluster_id += 1
        
        total_clusters = len(self.cluster_queries)
        print(f"  ✓ Создано кластеров: {total_clusters}")
        
        # Добавляем в DataFrame
        df['semantic_cluster_id'] = df['keyword'].map(self.clusters)
        
        # Определяем имя кластера (запрос с максимальной частотностью)
        cluster_names = {}
        for cid, queries in self.cluster_queries.items():
            cluster_df = df[df['keyword'].isin(queries)]
            if len(cluster_df) > 0:
                main_query = cluster_df.nlargest(1, 'frequency_world')['keyword'].iloc[0]
                cluster_names[cid] = main_query
        
        # Для каждого кластера присваиваем имя
        df['cluster_name'] = df['semantic_cluster_id'].map(
            lambda x: cluster_names.get(x, '')
        )
        
        # Для пустых имён берём сам запрос
        df.loc[df['cluster_name'] == '', 'cluster_name'] = df.loc[df['cluster_name'] == '', 'keyword']
        
        # Статистика
        multi_query_clusters = sum(1 for queries in self.cluster_queries.values() if len(queries) > 1)
        single_query_clusters = sum(1 for queries in self.cluster_queries.values() if len(queries) == 1)
        
        # Статистика по размерам кластеров
        cluster_sizes = [len(queries) for queries in self.cluster_queries.values()]
        max_size = max(cluster_sizes) if cluster_sizes else 0
        avg_size = sum(cluster_sizes) / len(cluster_sizes) if cluster_sizes else 0
        
        print(f"  ✓ Кластеризовано: {len(df)} запросов в {total_clusters} кластеров")
        print(f"  • Кластеры с 2+ запросами: {multi_query_clusters}")
        print(f"  • Одиночные кластеры: {single_query_clusters}")
        print(f"  • Максимальный размер кластера: {max_size} запросов")
        print(f"  • Средний размер кластера: {avg_size:.1f} запросов")
        
        # Предупреждение о слишком больших кластерах
        large_clusters = sum(1 for size in cluster_sizes if size > 100)
        if large_clusters > 0:
            print(f"  ⚠️  Найдено {large_clusters} кластеров с >100 запросами!")
            print(f"     Рекомендуется увеличить --serp-similarity-threshold (текущий: {self.min_common_urls})")
        
        return df
    
    def get_cluster_info(self, cluster_id: int, df: pd.DataFrame, serp_column: str = 'serp_main_pages') -> Dict:
        """
        Получить информацию о кластере
        
        Args:
            cluster_id: ID кластера
            df: DataFrame с данными
            serp_column: Колонка с SERP данными
            
        Returns:
            Словарь с информацией о кластере
        """
        cluster_df = df[df['semantic_cluster_id'] == cluster_id]
        
        if len(cluster_df) == 0:
            return {}
        
        # Сбор статистики по URL
        all_urls = []
        url_sets = []
        
        if serp_column in df.columns:
            for _, row in cluster_df.iterrows():
                urls = self.extract_serp_urls(row[serp_column])
                if urls:
                    url_sets.append(urls)
                    all_urls.extend(list(urls))
        
        # Общие URL (пересечение всех запросов)
        common_urls = set.intersection(*url_sets) if url_sets else set()
        
        # Топ популярных URL
        from collections import Counter
        popular_urls = []
        if all_urls:
            counter = Counter(all_urls)
            # Топ-10 или все, что встречаются больше чем в 1 запросе
            limit = 10
            min_count = 2 if len(cluster_df) > 1 else 1
            
            popular_urls = [
                {'url': url, 'count': count} 
                for url, count in counter.most_common(limit)
                if count >= min_count
            ]
        
        result = {
            'cluster_id': cluster_id,
            'size': len(cluster_df),
            'queries': cluster_df['keyword'].tolist(),
            'common_urls': list(common_urls),
            'popular_urls': popular_urls
        }
        
        # Добавляем метрики если есть
        if 'frequency_world' in cluster_df.columns:
            result['total_frequency'] = cluster_df['frequency_world'].sum()
            result['main_query'] = cluster_df.nlargest(1, 'frequency_world')['keyword'].iloc[0]
            
        if 'serp_docs_count' in cluster_df.columns:
            result['avg_serp_docs'] = cluster_df['serp_docs_count'].mean()
            
        return result

