"""Комплексное извлечение данных со страницы"""

from typing import Dict, Optional
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

from .breadcrumb_methods import BreadcrumbExtractionMethods
from .page_content_extractor import PageContentExtractor


class PageDataExtractor:
    """Извлекает breadcrumbs и контент одновременно"""
    
    def __init__(self, timeout: int = 10):
        """
        Инициализация
        
        Args:
            timeout: Таймаут для HTTP запросов
        """
        self.timeout = timeout
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.breadcrumb_methods = BreadcrumbExtractionMethods()
        self.content_extractor = PageContentExtractor()
    
    def extract_from_url(self, url: str) -> Optional[Dict]:
        """
        Извлечь все данные с URL
        
        Args:
            url: URL страницы
            
        Returns:
            Словарь с breadcrumbs и контентом или None
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            
            # Извлекаем breadcrumbs
            breadcrumbs = self._extract_breadcrumbs(soup)
            
            # Извлекаем контент
            content_data = self.content_extractor.extract_text_from_html(html)
            
            # Парсим домен
            domain = urlparse(url).netloc
            
            return {
                'url': url,
                'domain': domain,
                'breadcrumbs': breadcrumbs,
                'content': content_data
            }
            
        except Exception as e:
            print(f"⚠️  Ошибка загрузки {url}: {e}")
            return None
    
    def _extract_breadcrumbs(self, soup: BeautifulSoup):
        """Извлечь breadcrumbs всеми доступными методами"""
        # Метод 1: Schema.org
        breadcrumbs = self.breadcrumb_methods.extract_schema_org(soup)
        if breadcrumbs:
            return breadcrumbs
        
        # Метод 2: Микроразметка
        breadcrumbs = self.breadcrumb_methods.extract_microdata(soup)
        if breadcrumbs:
            return breadcrumbs
        
        # Метод 3: CSS классы
        breadcrumbs = self.breadcrumb_methods.extract_by_class(soup)
        if breadcrumbs:
            return breadcrumbs
        
        # Метод 4: Nav элементы
        breadcrumbs = self.breadcrumb_methods.extract_from_nav(soup)
        if breadcrumbs:
            return breadcrumbs
        
        return None





