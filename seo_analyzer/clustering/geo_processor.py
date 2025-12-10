"""Асинхронный процессор географии запросов"""

import asyncio
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass


@dataclass
class GeoProcessingResult:
    """Результат обработки географии запросов"""
    query_geo_dict: Dict[str, Optional[str]]
    geo_groups: Dict[str, List[str]]
    geo_stats: Dict[str, int]


class AsyncGeoProcessor:
    """
    Асинхронный процессор для определения географии запросов.
    
    Запускается в фоне сразу после дедупликации, чтобы к моменту 
    кластеризации результаты уже были готовы.
    """
    
    def __init__(self, semantic_checker=None, max_workers: int = 4):
        """
        Args:
            semantic_checker: SemanticClusterChecker с методом extract_geo()
            max_workers: Количество потоков для параллельной обработки
        """
        self.semantic_checker = semantic_checker
        self.max_workers = max_workers
        self._processing_task = None
        self._result: Optional[GeoProcessingResult] = None
    
    def start_processing(self, queries: List[str]) -> asyncio.Task:
        """
        Запускает обработку географии в фоновом режиме.
        
        Args:
            queries: Список запросов для обработки
            
        Returns:
            asyncio.Task с обработкой
        """
        self._processing_task = asyncio.create_task(
            self._process_geo_async(queries)
        )
        return self._processing_task
    
    async def _process_geo_async(self, queries: List[str]) -> GeoProcessingResult:
        """
        Асинхронная обработка географии запросов с распараллеливанием.
        
        Args:
            queries: Список запросов
            
        Returns:
            GeoProcessingResult с результатами обработки
        """
        if not self.semantic_checker:
            # Если нет semantic_checker, возвращаем пустые результаты
            return GeoProcessingResult(
                query_geo_dict={q: None for q in queries},
                geo_groups={'__no_geo__': queries},
                geo_stats={'total_segments': 0, 'no_geo': len(queries)}
            )
        
        # Разбиваем на батчи для параллельной обработки
        batch_size = max(100, len(queries) // self.max_workers)
        batches = [
            queries[i:i + batch_size] 
            for i in range(0, len(queries), batch_size)
        ]
        
        # Обрабатываем батчи параллельно в разных потоках
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            batch_tasks = [
                loop.run_in_executor(
                    executor,
                    self._process_batch,
                    batch
                )
                for batch in batches
            ]
            
            batch_results = await asyncio.gather(*batch_tasks)
        
        # Объединяем результаты батчей
        query_geo_dict = {}
        for batch_result in batch_results:
            query_geo_dict.update(batch_result)
        
        # Группируем по географии
        geo_groups = self._group_by_geography(query_geo_dict)
        
        # Собираем статистику
        geo_stats = self._calculate_stats(geo_groups)
        
        result = GeoProcessingResult(
            query_geo_dict=query_geo_dict,
            geo_groups=geo_groups,
            geo_stats=geo_stats
        )
        
        self._result = result
        return result
    
    def _process_batch(self, queries: List[str]) -> Dict[str, Optional[str]]:
        """
        Обрабатывает батч запросов синхронно (вызывается в отдельном потоке).
        
        Args:
            queries: Список запросов для обработки
            
        Returns:
            Словарь {query: geo}
        """
        result = {}
        for query in queries:
            try:
                geo = self.semantic_checker.extract_geo(query)
                result[query] = geo
            except Exception:
                # В случае ошибки считаем что географии нет
                result[query] = None
        return result
    
    def _group_by_geography(
        self, 
        query_geo_dict: Dict[str, Optional[str]]
    ) -> Dict[str, List[str]]:
        """
        Группирует запросы по географическим сегментам.
        
        Args:
            query_geo_dict: Словарь {query: geo}
            
        Returns:
            Словарь {geo: [queries]}
        """
        geo_groups = {'__no_geo__': []}
        
        for query, geo in query_geo_dict.items():
            if geo:
                if geo not in geo_groups:
                    geo_groups[geo] = []
                geo_groups[geo].append(query)
            else:
                geo_groups['__no_geo__'].append(query)
        
        return geo_groups
    
    def _calculate_stats(self, geo_groups: Dict[str, List[str]]) -> Dict[str, int]:
        """
        Рассчитывает статистику по географическим сегментам.
        
        Args:
            geo_groups: Словарь {geo: [queries]}
            
        Returns:
            Словарь со статистикой
        """
        geo_count = len([g for g in geo_groups.keys() if g != '__no_geo__'])
        no_geo_count = len(geo_groups.get('__no_geo__', []))
        
        return {
            'total_segments': geo_count,
            'no_geo': no_geo_count,
            'geo_groups': geo_groups
        }
    
    async def get_result(self) -> GeoProcessingResult:
        """
        Получает результат обработки (ждет завершения если еще не готово).
        
        Returns:
            GeoProcessingResult с результатами
        """
        if self._result is not None:
            return self._result
        
        if self._processing_task is None:
            raise RuntimeError("Обработка не была запущена")
        
        # Ждем завершения обработки
        return await self._processing_task
    
    def is_ready(self) -> bool:
        """
        Проверяет готовность результатов.
        
        Returns:
            True если обработка завершена
        """
        return self._result is not None or (
            self._processing_task is not None and 
            self._processing_task.done()
        )

