"""
Нормализация SERP данных в единый формат
"""

import json
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse


class SERPDataNormalizer:
    """Нормализатор SERP данных в единый формат"""
    
    @staticmethod
    def normalize_serp_urls(serp_data: Any) -> List[Dict[str, Any]]:
        """
        Нормализует SERP данные в единый формат
        
        Поддерживает:
        - Список URL строк: ["url1", "url2"] -> [{position, url, domain, ...}, ...]
        - Список словарей: [{url, ...}, ...] -> [{position, url, domain, ...}, ...]
        - JSON строка: '["url1", "url2"]' -> [{position, url, domain, ...}, ...]
        
        Args:
            serp_data: SERP данные в любом формате
            
        Returns:
            Список нормализованных словарей с полями:
            - position: int
            - url: str
            - domain: str
            - title: str (пустая строка если нет)
            - snippet: str (пустая строка если нет)
            - extended_text: str (пустая строка если нет)
            - passages: str (пустая строка если нет)
            - is_commercial: bool (False если нет)
        """
        # Обработка None и NULL значений
        if serp_data is None:
            return []
        
        # Проверка на pandas NaN
        try:
            import pandas as pd
            if pd.isna(serp_data):
                return []
        except (ImportError, TypeError, ValueError):
            pass
        
        # Если это строка - пытаемся распарсить как JSON
        if isinstance(serp_data, str):
            serp_data = serp_data.strip()
            if not serp_data or serp_data == 'null' or serp_data == 'None' or serp_data == 'NULL':
                return []
            try:
                serp_data = json.loads(serp_data)
            except (json.JSONDecodeError, TypeError):
                # Если не JSON - возможно это разделенные URL
                if '|' in serp_data:
                    serp_data = [url.strip() for url in serp_data.split('|') if url.strip()]
                elif ',' in serp_data:
                    serp_data = [url.strip() for url in serp_data.split(',') if url.strip()]
                else:
                    return []
        
        # Если это не список - возвращаем пустой список
        if not isinstance(serp_data, list):
            return []
        
        # Если список пустой
        if len(serp_data) == 0:
            return []
        
        normalized = []
        
        for position, item in enumerate(serp_data, start=1):
            # Если элемент - строка (URL)
            if isinstance(item, str):
                url = item.strip()
                if not url:
                    continue
                
                normalized.append({
                    'position': position,
                    'url': url,
                    'domain': SERPDataNormalizer._extract_domain(url),
                    'title': '',
                    'snippet': '',
                    'extended_text': '',
                    'passages': '',
                    'is_commercial': False
                })
            
            # Если элемент - словарь
            elif isinstance(item, dict):
                url = item.get('url', '')
                
                # Если url - это словарь (неправильный формат)
                if isinstance(url, dict):
                    url = url.get('url', '')
                
                if not url or not isinstance(url, str):
                    continue
                
                # Извлекаем остальные поля
                domain = item.get('domain', '')
                if not domain:
                    domain = SERPDataNormalizer._extract_domain(url)
                
                # Проверяем наличие position, если нет - используем текущий индекс
                item_position = item.get('position')
                if item_position is None or (isinstance(item_position, (int, float)) and item_position <= 0):
                    item_position = position
                
                normalized.append({
                    'position': int(item_position),
                    'url': url,
                    'domain': domain,
                    'title': item.get('title', ''),
                    'snippet': item.get('snippet', ''),
                    'extended_text': item.get('extended_text', ''),
                    'passages': item.get('passages', ''),
                    'is_commercial': item.get('is_commercial', False)
                })
        
        return normalized
    
    @staticmethod
    def _extract_domain(url: str) -> str:
        """
        Извлекает домен из URL
        
        Args:
            url: Полный URL
            
        Returns:
            Домен без протокола и www
        """
        if not url:
            return ''
        
        try:
            # Добавляем протокол если его нет
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path.split('/')[0]
            
            # Убираем www
            if domain.startswith('www.'):
                domain = domain[4:]
            
            return domain.lower()
        except Exception:
            # Если не удалось распарсить - пытаемся извлечь вручную
            url = url.replace('https://', '').replace('http://', '')
            url = url.replace('www.', '')
            domain = url.split('/')[0].split('?')[0].split('#')[0]
            return domain.lower()

