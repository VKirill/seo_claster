"""Методы извлечения breadcrumbs из HTML"""

import re
from typing import List, Optional
from bs4 import BeautifulSoup


class BreadcrumbExtractionMethods:
    """Различные методы извлечения breadcrumbs"""
    
    @staticmethod
    def extract_schema_org(soup: BeautifulSoup) -> Optional[List[str]]:
        """Извлечь breadcrumbs из Schema.org разметки"""
        breadcrumbs = []
        
        # JSON-LD
        scripts = soup.find_all('script', {'type': 'application/ld+json'})
        for script in scripts:
            try:
                import json
                data = json.loads(script.string)
                
                # Может быть массив или один объект
                if isinstance(data, list):
                    for item in data:
                        if item.get('@type') == 'BreadcrumbList':
                            breadcrumbs = BreadcrumbExtractionMethods._parse_breadcrumb_list(item)
                            if breadcrumbs:
                                return breadcrumbs
                elif isinstance(data, dict) and data.get('@type') == 'BreadcrumbList':
                    breadcrumbs = BreadcrumbExtractionMethods._parse_breadcrumb_list(data)
                    if breadcrumbs:
                        return breadcrumbs
            except:
                continue
        
        # Microdata атрибуты
        breadcrumb_lists = soup.find_all(itemtype="http://schema.org/BreadcrumbList")
        for bc_list in breadcrumb_lists:
            items = bc_list.find_all(itemtype="http://schema.org/ListItem")
            for item in items:
                name_elem = item.find(itemprop="name")
                if name_elem:
                    breadcrumbs.append(name_elem.get_text(strip=True))
            
            if breadcrumbs:
                return breadcrumbs
        
        return None
    
    @staticmethod
    def _parse_breadcrumb_list(data: dict) -> List[str]:
        """Парсинг BreadcrumbList из JSON-LD"""
        breadcrumbs = []
        items = data.get('itemListElement', [])
        
        for item in items:
            if isinstance(item, dict):
                name = item.get('name') or item.get('item', {}).get('name')
                if name:
                    breadcrumbs.append(name)
        
        return breadcrumbs
    
    @staticmethod
    def extract_microdata(soup: BeautifulSoup) -> Optional[List[str]]:
        """Извлечь breadcrumbs из микроразметки"""
        breadcrumbs = []
        
        # Ищем элементы с data-breadcrumb
        elements = soup.find_all(attrs={'data-breadcrumb': True})
        for elem in elements:
            text = elem.get_text(strip=True)
            if text:
                breadcrumbs.append(text)
        
        if breadcrumbs:
            return breadcrumbs
        
        return None
    
    @staticmethod
    def extract_by_class(soup: BeautifulSoup) -> Optional[List[str]]:
        """Извлечь breadcrumbs по CSS классам"""
        breadcrumbs = []
        
        # Популярные классы для breadcrumbs
        breadcrumb_classes = [
            'breadcrumb', 'breadcrumbs', 'bread-crumbs',
            'nav-breadcrumb', 'page-breadcrumb', 
            'path', 'crumbs', 'location'
        ]
        
        for class_name in breadcrumb_classes:
            elements = soup.find_all(class_=re.compile(class_name, re.I))
            
            for element in elements:
                # Ищем ссылки или элементы списка
                links = element.find_all('a')
                if links:
                    breadcrumbs = [link.get_text(strip=True) for link in links]
                else:
                    # Пробуем li элементы
                    items = element.find_all('li')
                    if items:
                        breadcrumbs = [item.get_text(strip=True) for item in items]
                    else:
                        # Разделяем по разделителям
                        text = element.get_text()
                        breadcrumbs = BreadcrumbExtractionMethods._split_breadcrumb_text(text)
                
                if len(breadcrumbs) >= 2:  # Минимум 2 элемента
                    return BreadcrumbExtractionMethods._clean_breadcrumbs(breadcrumbs)
        
        return None
    
    @staticmethod
    def extract_from_nav(soup: BeautifulSoup) -> Optional[List[str]]:
        """Извлечь breadcrumbs из nav элементов"""
        breadcrumbs = []
        
        # Ищем nav с aria-label="breadcrumb"
        nav_elements = soup.find_all('nav', {'aria-label': re.compile('breadcrumb', re.I)})
        
        for nav in nav_elements:
            links = nav.find_all('a')
            if links:
                breadcrumbs = [link.get_text(strip=True) for link in links]
                
                # Добавляем текущую страницу (не ссылка)
                current = nav.find(class_=re.compile('active|current', re.I))
                if current:
                    breadcrumbs.append(current.get_text(strip=True))
                
                if len(breadcrumbs) >= 2:
                    return BreadcrumbExtractionMethods._clean_breadcrumbs(breadcrumbs)
        
        return None
    
    @staticmethod
    def _split_breadcrumb_text(text: str) -> List[str]:
        """Разделить текст breadcrumbs по разделителям"""
        separators = [' > ', ' / ', ' » ', ' › ', ' | ', '›', '»', '/', '>']
        
        for sep in separators:
            if sep in text:
                parts = [p.strip() for p in text.split(sep)]
                if len(parts) >= 2:
                    return parts
        
        return []
    
    @staticmethod
    def _clean_breadcrumbs(breadcrumbs: List[str]) -> List[str]:
        """Очистить список breadcrumbs"""
        cleaned = []
        
        for crumb in breadcrumbs:
            # Удаляем пустые и очень короткие
            crumb = crumb.strip()
            if len(crumb) < 2:
                continue
            
            # Удаляем "Главная", "Home" в начале
            if crumb.lower() in ['главная', 'home', 'домой', 'начало']:
                continue
            
            cleaned.append(crumb)
        
        return cleaned if len(cleaned) >= 2 else []





