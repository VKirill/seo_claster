"""Агрегация breadcrumbs из SERP данных с сохранением в БД"""

from typing import List, Dict
from pathlib import Path
import pandas as pd

from .page_data_extractor import PageDataExtractor
from .breadcrumb_selector import BreadcrumbSelector
from .domain_filter import DomainFilter
from ..core.page_content_database import PageContentDatabase


class BreadcrumbAggregator:
    """Собирает breadcrumbs и контент, сохраняет в БД"""
    
    def __init__(
        self, 
        max_urls_per_query: int = 3,
        db_path: Path = None,
        stop_domains_file: Path = None
    ):
        """
        Инициализация
        
        Args:
            max_urls_per_query: Макс URL для сканирования (по умолчанию 3 для ТОП-3)
            db_path: Путь к БД
            stop_domains_file: Файл со стоп-доменами
        """
        self.max_urls_per_query = max_urls_per_query
        self.extractor = PageDataExtractor()
        self.selector = BreadcrumbSelector()
        
        # БД для кэша
        if db_path:
            self.database = PageContentDatabase(db_path)
        else:
            self.database = None
        
        # Фильтр доменов
        if stop_domains_file and stop_domains_file.exists():
            self.domain_filter = DomainFilter(stop_domains_file)
        else:
            self.domain_filter = None
    
    def extract_from_dataframe(self, df: pd.DataFrame) -> Dict[str, List[List[str]]]:
        """
        Извлечь breadcrumbs из DataFrame с SERP данными
        
        Args:
            df: DataFrame с колонкой 'serp_urls'
            
        Returns:
            Словарь {query: [[breadcrumbs1], [breadcrumbs2], ...]}
        """
        results = {}
        total = len(df)
        
        print(f"\n🔍 Извлечение breadcrumbs и контента из {total} запросов...")
        print(f"  Макс URL на запрос: {self.max_urls_per_query} (ТОП-{self.max_urls_per_query})")
        
        for idx, row in df.iterrows():
            if (idx + 1) % 10 == 0:
                print(f"  Обработано: {idx + 1}/{total}")
            
            query = row.get('keyword', '')
            serp_urls = row.get('serp_urls', [])
            
            if not isinstance(serp_urls, list) or not serp_urls:
                continue
            
            # Фильтруем по стоп-доменам
            if self.domain_filter:
                serp_urls = self.domain_filter.filter_urls(serp_urls)
            
            # Берем только ТОП-N
            breadcrumbs_list = self._extract_from_urls(
                serp_urls[:self.max_urls_per_query],
                query
            )
            
            if breadcrumbs_list:
                results[query] = breadcrumbs_list
        
        print(f"✓ Извлечено breadcrumbs для {len(results)} запросов")
        
        if self.database:
            print(f"✓ Данные сохранены в БД: {self.database.db_path}")
        
        return results
    
    def _extract_from_urls(self, urls: List[str], query: str) -> List[List[str]]:
        """
        Извлечь breadcrumbs и контент из списка URL
        
        Args:
            urls: Список URL для сканирования
            query: Поисковый запрос
            
        Returns:
            Список breadcrumbs
        """
        breadcrumbs_list = []
        
        for position, url in enumerate(urls, start=1):
            # Проверяем БД кэш
            if self.database:
                cached = self.database.get_page_data(url)
                if cached:
                    if cached['breadcrumbs']:
                        breadcrumbs_list.append(cached['breadcrumbs'])
                    continue
            
            # Извлекаем данные
            page_data = self.extractor.extract_from_url(url)
            
            if not page_data:
                continue
            
            # Сохраняем в БД
            if self.database:
                self.database.save_page_data(
                    url=page_data['url'],
                    domain=page_data['domain'],
                    query=query,
                    position=position,
                    content_data=page_data['content'],
                    breadcrumbs=page_data['breadcrumbs']
                )
            
            # Добавляем breadcrumbs
            if page_data['breadcrumbs'] and len(page_data['breadcrumbs']) >= 2:
                breadcrumbs_list.append(page_data['breadcrumbs'])
        
        return breadcrumbs_list
    
    def deduplicate_breadcrumbs(self, breadcrumbs_dict: Dict) -> Dict:
        """Удалить дубликаты breadcrumbs"""
        return self.selector.deduplicate_breadcrumbs(breadcrumbs_dict)
    
    def get_unique_hierarchies(self, deduplicated: Dict) -> set:
        """Получить уникальные иерархии"""
        return self.selector.get_unique_hierarchies(deduplicated)
    
    def get_hierarchy_stats(self, deduplicated: Dict) -> Dict:
        """Статистика по иерархиям"""
        return self.selector.get_hierarchy_stats(deduplicated)
