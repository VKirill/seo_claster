"""Извлечение breadcrumbs (хлебных крошек) из веб-страниц"""

from typing import List, Optional
import requests
from bs4 import BeautifulSoup
from .breadcrumb_methods import BreadcrumbExtractionMethods


class BreadcrumbExtractor:
    """Извлекает breadcrumbs из HTML страниц"""
    
    def __init__(self, timeout: int = 10):
        """
        Инициализация
        
        Args:
            timeout: Таймаут для HTTP запросов в секундах
        """
        self.timeout = timeout
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.methods = BreadcrumbExtractionMethods()
    
    def extract_from_url(self, url: str) -> Optional[List[str]]:
        """
        Извлечь breadcrumbs с URL
        
        Args:
            url: URL страницы
            
        Returns:
            Список элементов breadcrumbs или None
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            return self._extract_breadcrumbs(soup, url)
            
        except Exception as e:
            print(f"⚠️  Ошибка загрузки {url}: {e}")
            return None
    
    def _extract_breadcrumbs(self, soup: BeautifulSoup, url: str) -> Optional[List[str]]:
        """
        Извлечь breadcrumbs из HTML
        
        Args:
            soup: BeautifulSoup объект
            url: URL страницы (для контекста)
            
        Returns:
            Список элементов breadcrumbs
        """
        # Метод 1: Schema.org BreadcrumbList
        breadcrumbs = self.methods.extract_schema_org(soup)
        if breadcrumbs:
            return breadcrumbs
        
        # Метод 2: Микроразметка (data-* атрибуты)
        breadcrumbs = self.methods.extract_microdata(soup)
        if breadcrumbs:
            return breadcrumbs
        
        # Метод 3: Стандартные CSS классы
        breadcrumbs = self.methods.extract_by_class(soup)
        if breadcrumbs:
            return breadcrumbs
        
        # Метод 4: Навигационные элементы
        breadcrumbs = self.methods.extract_from_nav(soup)
        if breadcrumbs:
            return breadcrumbs
        
        return None
