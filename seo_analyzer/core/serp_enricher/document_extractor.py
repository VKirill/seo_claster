"""Извлечение документов из SERP XML"""

import xml.etree.ElementTree as ET
from typing import Dict, List, Any
from urllib.parse import urlparse
import re

from ..xml_text_extractor import XMLTextExtractor


def extract_domain(url: str) -> str:
    """Извлечь домен из URL"""
    try:
        parsed = urlparse(url)
        return parsed.netloc
    except:
        return ''


def is_commercial_domain(domain: str, commercial_patterns: list = None) -> bool:
    """
    Проверить является ли домен коммерческим (упрощённая версия для статистики)
    
    Примечание: Эта функция используется ТОЛЬКО для статистики в колонках
    serp_commercial_domains / serp_info_domains и НЕ влияет на main_intent.
    
    Main intent определяется через:
    1. Ключевые слова из commercial.txt / info.txt
    2. SERP offer_info (приоритет)
    
    Логика:
    - Явные коммерческие маркеры (shop, market, ozon) → коммерческий
    - Всё остальное → информационный (безопасное предположение)
    """
    if commercial_patterns is None:
        commercial_patterns = [
            # Только самые явные маркетплейсы и магазины
            r'\.shop', r'\.store', r'market\.yandex', r'-shop', r'store-',
            r'ozon\.ru', r'wildberries\.ru', r'avito\.ru', r'youla\.ru',
            r'aliexpress\.', r'goods', r'catalog', r'price',
            r'sale', r'buy'
        ]
    
    domain_lower = domain.lower()
    
    # Только явные коммерческие маркеры
    if any(re.search(pattern, domain_lower) for pattern in commercial_patterns):
        return True
    
    # Всё остальное - информационное (безопасное предположение)
    # Настоящая классификация интента происходит через SERP offer_info
    return False


def is_info_domain(domain: str, info_patterns: list = None) -> bool:
    """
    Проверить является ли домен информационным (упрощённая версия для статистики)
    
    Примечание: Используется ТОЛЬКО для статистики и НЕ влияет на main_intent.
    """
    if info_patterns is None:
        info_patterns = [
            # Только самые явные информационные домены
            r'wiki', r'wikipedia', r'blog', r'forum',
            r'youtube\.com', r'rutube\.ru', r'vk\.com', r'ok\.ru',
            r'habr', r'dzen', r'vc\.ru'
        ]
    
    domain_lower = domain.lower()
    return any(re.search(pattern, domain_lower) for pattern in info_patterns)


def extract_documents(
    root: ET.Element,
    query: str = None,
    xml_extractor: XMLTextExtractor = None,
    commercial_patterns: list = None
) -> List[Dict[str, Any]]:
    """
    Извлечь все документы из SERP
    
    Args:
        root: Корневой элемент XML
        query: Оригинальный запрос (не используется, но оставлен для совместимости)
        xml_extractor: Экстрактор XML текста
        commercial_patterns: Паттерны коммерческих доменов
        
    Returns:
        Список документов с позицией, URL, title, snippet, passages
    """
    if xml_extractor is None:
        xml_extractor = XMLTextExtractor()
    
    if commercial_patterns is None:
        commercial_patterns = [
            r'\.shop', r'\.store', r'market', r'shop', r'ozon', r'wildberries',
            r'avito', r'youla', r'aliexpress', r'goods', r'catalog', r'price'
        ]
    
    documents = []
    position = 1
    
    # Ищем все группы документов
    grouping = root.find('.//grouping')
    if grouping is None:
        return documents
    
    for group in grouping.findall('.//group'):
        # Берем первый документ из группы (главная страница)
        doc = group.find('.//doc')
        if doc is None:
            continue
        
        # URL
        url_elem = doc.find('url')
        url = url_elem.text if url_elem is not None and url_elem.text else ''
        
        # Domain
        domain_elem = doc.find('domain')
        domain = domain_elem.text if domain_elem is not None and domain_elem.text else ''
        if not domain and url:
            domain = extract_domain(url)
        
        # Title - используем XMLTextExtractor для правильного извлечения текста
        title_elem = doc.find('.//title')
        title = xml_extractor.extract_text(title_elem)
        
        # Extended text - более полное описание страницы из properties (ПРИОРИТЕТ!)
        extended_text = ''
        properties = doc.find('.//properties')
        if properties is not None:
            extended_elem = properties.find('extended-text')
            if extended_elem is not None:
                extended_text = xml_extractor.extract_text(extended_elem)
        
        # Snippet - используем extended_text как основной источник (самый полный)
        snippet = extended_text
        
        # FALLBACK: Если extended_text пустой, пробуем headline
        if not snippet:
            headline_elem = doc.find('.//headline')
            snippet = xml_extractor.extract_text(headline_elem)
        
        # Passages - используем XMLTextExtractor для каждого passage
        passages_elems = doc.findall('.//passage')
        passages_list = xml_extractor.extract_text_from_multiple(passages_elems)
        passages = ' '.join(passages_list)
        
        # FALLBACK: Если snippet всё ещё пустой, используем passages
        if not snippet and passages:
            snippet = passages
        
        # Определяем коммерческий ли домен (для статистики, не влияет на main_intent)
        is_commercial = is_commercial_domain(domain, commercial_patterns)
        
        documents.append({
            'position': position,
            'url': url,
            'domain': domain,
            'title': title,
            'snippet': snippet,
            'extended_text': extended_text,
            'passages': passages,
            'is_commercial': is_commercial
        })
        
        position += 1
    
    return documents

