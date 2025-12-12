"""Извлечение метрик из SERP XML"""

import xml.etree.ElementTree as ET
from typing import Dict
import re
from urllib.parse import urlparse

from ..xml_text_extractor import XMLTextExtractor
from .document_extractor import is_commercial_domain, is_info_domain
from ..lemmatizer import lemmatize_word


def get_found_count(root: ET.Element) -> int:
    """Получить общее количество найденных документов"""
    response = root.find('.//response')
    if response is not None:
        found = response.find('found')
        if found is not None and found.text:
            try:
                # Убираем пробелы и преобразуем в int
                return int(found.text.replace(' ', '').replace('\xa0', ''))
            except ValueError:
                pass
    return 0


def is_root_url(url: str) -> bool:
    """
    Проверить является ли URL корневой страницей домена.
    
    Корневой URL - это URL вида:
    - https://rusguard.shop/
    - https://www.rusguard.shop/
    - http://rusguard.shop/
    - https://rusguard.shop (без слеша тоже считается корневым)
    
    НЕ корневой URL:
    - https://rusguard.shop/products/...
    - https://rusguard.shop/page.html
    - https://rusguard.shop/category/item/
    
    Args:
        url: URL для проверки
        
    Returns:
        True если URL является корневой страницей домена
    """
    if not url:
        return False
    
    try:
        parsed = urlparse(url)
        path = parsed.path.strip()
        
        # Путь должен быть пустым или только "/"
        # Убираем параметры запроса и якоря для проверки
        if path == '' or path == '/':
            return True
        
        return False
    except Exception:
        return False


def count_main_pages(root: ET.Element) -> int:
    """
    Подсчитать количество главных страниц (корневых URL доменов).
    
    Считаются только URL вида https://domain.com/ или https://www.domain.com/,
    а не внутренние страницы типа https://domain.com/products/...
    
    Args:
        root: Корневой элемент XML
        
    Returns:
        Количество корневых URL в выдаче
    """
    grouping = root.find('.//grouping')
    if grouping is None:
        return 0
    
    count = 0
    groups = grouping.findall('.//group')
    
    for group in groups:
        # Берем первый документ из группы (главный документ группы)
        doc = group.find('.//doc')
        if doc is None:
            continue
        
        # Извлекаем URL
        url_elem = doc.find('url')
        if url_elem is not None and url_elem.text:
            url = url_elem.text.strip()
            # Проверяем является ли URL корневой страницей домена
            if is_root_url(url):
                count += 1
    
    return count


def count_titles_with_keyword(root: ET.Element, query: str = None, xml_extractor: XMLTextExtractor = None) -> int:
    """
    Подсчитать количество title, в которых встречаются все слова запроса.
    
    Игнорирует регистр, символы (дефисы, тире и т.д.) и падежи слов.
    Все слова запроса должны присутствовать в title (в любой форме).
    
    Примеры:
    - Запрос: "скуд acs 102 ce bm"
    - Найдет в: "Контроллер СКУД ACS-102-CE-BM"
    - Найдет в: "Контроллер СКУДов сетевой RusGuard ACS-102-CE-BM" (СКУДов → скуд)
    - Найдет в: "ACS-102-CE-BM | Официальный сайт СКУД"
    - Найдет в: "СКУДы ACS-102-CE-BM" (СКУДы → скуд)
    
    Args:
        root: Корневой элемент XML
        query: Запрос для поиска
        xml_extractor: Экстрактор XML текста
        
    Returns:
        Количество title, содержащих все слова запроса (с учетом падежей)
    """
    if not query:
        return 0
    
    if xml_extractor is None:
        xml_extractor = XMLTextExtractor()
    
    # Нормализуем и лемматизируем запрос: "скуд acs 102 ce bm" → ["скуд", "acs", "102", "ce", "bm"]
    # Лемматизация позволяет находить слова в разных падежах
    query_normalized = normalize_text_with_lemmatization(query)
    if not query_normalized:
        return 0
    
    query_words = set(query_normalized.split())
    if not query_words:
        return 0
    
    count = 0
    for title_elem in root.findall('.//title'):
        # Используем XMLTextExtractor для извлечения полного текста
        title_text = xml_extractor.extract_text(title_elem)
        if not title_text:
            continue
        
        # Нормализуем и лемматизируем title: "СКУДов ACS-102-CE-BM" → "скуд acs 102 ce bm"
        # Лемматизация преобразует "СКУДов" в "скуд", "СКУДы" в "скуд" и т.д.
        title_normalized = normalize_text_with_lemmatization(title_text)
        if not title_normalized:
            continue
        
        # Разбиваем на слова
        title_words = set(title_normalized.split())
        
        # Проверяем, что все слова запроса присутствуют в title
        # Например: {"скуд", "acs", "102", "ce", "bm"} ⊆ {"контроллер", "скуд", "acs", "102", "ce", "bm", ...}
        # Работает даже если в title было "СКУДов" или "СКУДы" - они лемматизируются в "скуд"
        if query_words.issubset(title_words):
            count += 1
    
    return count


def count_yandex_ads(root: ET.Element) -> int:
    """Подсчитать количество рекламных объявлений"""
    ads = 0
    
    # Ищем премиум размещения
    for doc in root.findall('.//doc'):
        if doc.get('premium') == 'yes':
            ads += 1
    
    return ads


def normalize_text(text: str) -> str:
    """
    Нормализация текста для сравнения.
    
    Удаляет все символы кроме букв, цифр и пробелов.
    Дефисы, тире и другие разделители заменяются на пробелы.
    
    Примеры:
    - "ACS-102-CE-BM" → "acs 102 ce bm"
    - "Контроллер СКУД ACS-102-CE-BM" → "контроллер скуд acs 102 ce bm"
    - "скуд acs 102 ce bm" → "скуд acs 102 ce bm"
    
    Args:
        text: Текст для нормализации
        
    Returns:
        Нормализованный текст в нижнем регистре
    """
    if not text:
        return ''
    
    # Удаляем HTML теги
    text = re.sub(r'<[^>]+>', '', text)
    
    # Заменяем все не-буквенно-цифровые символы (включая дефисы, тире, точки и т.д.) на пробелы
    # Это позволяет "ACS-102-CE-BM" совпасть с "acs 102 ce bm"
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Множественные пробелы в один
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip().lower()


def normalize_text_with_lemmatization(text: str) -> str:
    """
    Нормализация текста с лемматизацией для сравнения с учетом падежей.
    
    Удаляет все символы кроме букв, цифр и пробелов, затем лемматизирует слова.
    Это позволяет находить совпадения даже если слова в разных падежах.
    
    Примеры:
    - "СКУДов" → "скуд" (родительный падеж → именительный)
    - "СКУДы" → "скуд" (множественное число → единственное)
    - "Контроллер СКУДов ACS-102-CE-BM" → "контроллер скуд acs 102 ce bm"
    - "скуд acs 102 ce bm" → "скуд acs 102 ce bm"
    
    Args:
        text: Текст для нормализации
        
    Returns:
        Нормализованный и лемматизированный текст в нижнем регистре
    """
    if not text:
        return ''
    
    # Сначала нормализуем (удаляем символы, приводим к нижнему регистру)
    normalized = normalize_text(text)
    if not normalized:
        return ''
    
    # Разбиваем на слова и лемматизируем каждое слово
    words = normalized.split()
    lemmatized_words = []
    
    for word in words:
        # Лемматизируем слово (для русских слов вернет нормальную форму,
        # для английских и цифр вернет как есть)
        lemma = lemmatize_word(word)
        if lemma:
            lemmatized_words.append(lemma)
    
    return ' '.join(lemmatized_words)


def extract_metrics(
    root: ET.Element,
    documents: list,
    original_query: str = None,
    xml_extractor: XMLTextExtractor = None
) -> Dict[str, int]:
    """
    Извлечь все метрики из SERP XML
    
    Args:
        root: Корневой элемент XML
        documents: Список документов (для подсчета доменов)
        original_query: Оригинальный запрос
        xml_extractor: Экстрактор XML текста
        
    Returns:
        Dict с метриками
    """
    if xml_extractor is None:
        xml_extractor = XMLTextExtractor()
    
    metrics = {
        'found_docs': get_found_count(root),
        'main_pages_count': count_main_pages(root),
        'titles_with_keyword': count_titles_with_keyword(root, original_query, xml_extractor),
        'commercial_domains': 0,
        'info_domains': 0,
        'yandex_ads': count_yandex_ads(root),
    }
    
    # Подсчитываем типы доменов
    commercial_patterns = [
        r'\.shop', r'\.store', r'market', r'shop', r'ozon', r'wildberries',
        r'avito', r'youla', r'aliexpress', r'goods', r'catalog', r'price'
    ]
    
    info_patterns = [
        r'wiki', r'blog', r'forum', r'otvet', r'answer', r'info',
        r'reference', r'encyclopedia', r'faq', r'help'
    ]
    
    for doc in documents:
        domain = doc.get('domain', '').lower()
        if is_commercial_domain(domain, commercial_patterns):
            metrics['commercial_domains'] += 1
        if is_info_domain(domain, info_patterns):
            metrics['info_domains'] += 1
    
    return metrics


def get_empty_metrics() -> Dict[str, int]:
    """Пустые метрики при ошибке"""
    return {
        'found_docs': 0,
        'main_pages_count': 0,
        'titles_with_keyword': 0,
        'commercial_domains': 0,
        'info_domains': 0,
        'yandex_ads': 0,
    }


def calculate_serp_difficulty(metrics: Dict[str, int]) -> float:
    """
    Рассчитать сложность продвижения на основе SERP метрик
    
    Returns:
        Оценка сложности от 0 до 100 (выше = сложнее)
    """
    # Базовая сложность от количества конкурентов
    base_difficulty = min(metrics.get('found_docs', 0) / 1000000 * 50, 50)
    
    # Добавляем за главные страницы в топе
    main_pages_factor = min(metrics.get('main_pages_count', 0) * 1.5, 25)
    
    # Добавляем за точное вхождение в title
    titles_factor = min(metrics.get('titles_with_keyword', 0) * 0.8, 15)
    
    # Коммерческие домены увеличивают сложность
    commercial_factor = min(metrics.get('commercial_domains', 0) * 0.5, 10)
    
    total_difficulty = base_difficulty + main_pages_factor + titles_factor + commercial_factor
    
    return min(total_difficulty, 100)

