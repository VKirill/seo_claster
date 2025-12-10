"""
Нормализация URL для кластеризации
"""

import re
from typing import List
import pandas as pd


class URLNormalizer:
    """Нормализатор URL"""
    
    @staticmethod
    def normalize_url(url: str) -> str:
        """
        Нормализует URL для сравнения (убирает параметры, якоря, trailing slash)
        
        Args:
            url: Полный URL
            
        Returns:
            Нормализованный URL без протокола www параметров
        """
        if not url:
            return ""
        
        # Убираем протокол
        url = re.sub(r'^https?://', '', url)
        url = re.sub(r'^www\.', '', url)
        
        # Убираем параметры запроса и якоря
        url = url.split('?')[0].split('#')[0]
        
        # Убираем trailing slash
        url = url.rstrip('/')
        
        return url.lower()
    
    @staticmethod
    def extract_domain(url: str) -> str:
        """Извлекает домен из URL"""
        if not url:
            return ""
        
        # Убираем протокол
        url = re.sub(r'^https?://', '', url)
        url = re.sub(r'^www\.', '', url)
        
        # Берем только домен
        domain = url.split('/')[0]
        
        return domain.lower()
    
    @staticmethod
    def extract_serp_urls(serp_data) -> List[str]:
        """
        Извлекает и нормализует URL из SERP данных
        
        Args:
            serp_data: Список URL или строка с URL разделёнными |
            
        Returns:
            Список нормализованных URL (БЕЗ доменов - полные пути!)
        """
        # Проверка на NaN/None/пустое значение
        if serp_data is None:
            return []
        
        # Если это уже список - используем его
        if isinstance(serp_data, list):
            urls = []
            for url in serp_data:
                if url:  # Пропускаем пустые
                    normalized = URLNormalizer.normalize_url(str(url).strip())
                    if normalized:
                        urls.append(normalized)
            return urls
        
        # Проверка на pandas NaN
        try:
            if pd.isna(serp_data):
                return []
        except (TypeError, ValueError):
            pass
        
        # Если пустая строка
        if not serp_data:
            return []
        
        # Если строка - разделяем по |
        url_list = str(serp_data).split('|')
        urls = []
        
        for url in url_list:
            normalized = URLNormalizer.normalize_url(url.strip())
            if normalized:
                urls.append(normalized)
        
        return urls

