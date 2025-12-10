"""
Классификатор интента на основе коммерческих факторов в SERP XML.

Приоритет 1 (100% коммерческий):
- Если запрос содержит коммерческое слово из keyword_group/commercial.txt 
  в любом падеже → commercial с confidence=1.0

Приоритет 2 (на основе факторов):
- Смотрим на КОНКРЕТНУЮ выдачу для каждого запроса
- Считаем коммерческие факторы:
  1. Документы с <offer_info> (независимо от домена)
  2. Документы из коммерческих доменов (keywords_settings/commercial_domains.txt)
- Если сумма факторов >= 7 → commercial
- Если сумма факторов < 7 → informational

Также извлекаем ценовые данные:
- Средняя цена в выдаче
- Минимальная/максимальная цена
- Медианная цена
- Количество предложений со скидками
"""

import re
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Tuple, Optional, List, Set
from statistics import median, mean
from urllib.parse import urlparse

from ...core.lemmatizer import lemmatize_phrase


class SERPOfferClassifier:
    """
    Классификатор интента на основе коммерческих факторов в XML выдаче.
    
    Приоритет 1 (100% коммерческий):
    - Если запрос содержит коммерческое слово из keyword_group/commercial.txt 
      в любом падеже → commercial с confidence=1.0
    
    Приоритет 2 (на основе факторов):
    - Коммерческие факторы:
      1. Документы с <offer_info> (независимо от домена)
      2. Документы из коммерческих доменов (keywords_settings/commercial_domains.txt)
    - Если сумма факторов >= 7 → commercial
    - Если сумма факторов < 7 → informational
    
    Примеры:
    - "купить скуд" → содержит "купить" → commercial (100%)
    - 4 offer_info + 3 коммерческих домена = 7 факторов → commercial
    - 4 коммерческих домена + 3 offer_info = 7 факторов → commercial
    - 3 offer_info + 2 коммерческих домена = 5 факторов → informational
    
    Это точнее чем классификация только по offer_info, потому что:
    - Учитываем коммерческие слова в запросе (приоритет)
    - Учитываем коммерческие домены даже без offer_info
    - Смотрим на КОНКРЕТНУЮ выдачу для КОНКРЕТНОГО запроса
    """
    
    def __init__(
        self,
        top_n: int = 20,
        commercial_threshold: int = 7,
        commercial_ratio: float = 0.4,
        commercial_domains_file: Path = None,
        commercial_keywords_file: Path = None
    ):
        """
        Args:
            top_n: Анализировать топ-N документов (по умолчанию 20)
            commercial_threshold: Минимум коммерческих факторов для commercial (по умолчанию 7)
            commercial_ratio: Альтернативный порог - доля документов (по умолчанию 0.4 = 40%)
            commercial_domains_file: Путь к файлу с коммерческими доменами
            commercial_keywords_file: Путь к файлу с коммерческими ключевыми словами
        """
        self.top_n = top_n
        self.commercial_threshold = commercial_threshold
        self.commercial_ratio = commercial_ratio
        
        # Путь относительно корня проекта
        base_dir = Path(__file__).parent.parent.parent.parent
        
        # Загружаем коммерческие домены из файла
        if commercial_domains_file is None:
            commercial_domains_file = base_dir / 'keywords_settings' / 'commercial_domains.txt'
        self.commercial_domains = self._load_commercial_domains(commercial_domains_file)
        
        # Загружаем коммерческие ключевые слова из файла
        if commercial_keywords_file is None:
            commercial_keywords_file = base_dir / 'keyword_group' / 'commercial.txt'
        self.commercial_keywords = self._load_commercial_keywords(commercial_keywords_file)
    
    def _load_commercial_domains(self, file_path: Path) -> Set[str]:
        """
        Загружает коммерческие домены из файла.
        
        Args:
            file_path: Путь к файлу с коммерческими доменами
            
        Returns:
            Множество доменов (в нижнем регистре, без www)
        """
        domains = set()
        
        if not file_path.exists():
            return domains
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    
                    # Пропускаем комментарии и пустые строки
                    if not line or line.startswith('#'):
                        continue
                    
                    # Убираем комментарии в конце строки
                    domain = line.split('#')[0].strip()
                    
                    if domain:
                        # Убираем www. если есть
                        domain = re.sub(r'^www\.', '', domain.lower())
                        domains.add(domain)
        except Exception as e:
            print(f"⚠️ Ошибка при загрузке коммерческих доменов: {e}")
        
        return domains
    
    def _load_commercial_keywords(self, file_path: Path) -> Set[str]:
        """
        Загружает коммерческие ключевые слова из файла.
        
        Args:
            file_path: Путь к файлу с коммерческими ключевыми словами
            
        Returns:
            Множество ключевых слов (в нижнем регистре, лемматизированных)
        """
        keywords = set()
        
        if not file_path.exists():
            return keywords
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    
                    # Пропускаем комментарии и пустые строки
                    if not line or line.startswith('#'):
                        continue
                    
                    # Убираем комментарии в конце строки
                    keyword = line.split('#')[0].strip()
                    
                    if keyword:
                        # Лемматизируем ключевое слово для сравнения в любом падеже
                        keyword_lemmatized = lemmatize_phrase(keyword.lower())
                        keywords.add(keyword_lemmatized)
        except Exception as e:
            print(f"⚠️ Ошибка при загрузке коммерческих ключевых слов: {e}")
        
        return keywords
    
    def _has_commercial_keyword(self, query: str) -> bool:
        """
        Проверяет содержит ли запрос коммерческое ключевое слово в любом падеже.
        
        Args:
            query: Поисковый запрос
            
        Returns:
            True если запрос содержит коммерческое слово
        """
        if not query or not self.commercial_keywords:
            return False
        
        # Лемматизируем запрос для сравнения
        query_lemmatized = lemmatize_phrase(query.lower())
        query_words = set(query_lemmatized.split())
        
        # Проверяем есть ли коммерческое слово в запросе
        return bool(query_words & self.commercial_keywords)
    
    def _extract_domain_from_url(self, url: str) -> str:
        """
        Извлекает домен из URL.
        
        Args:
            url: Полный URL
            
        Returns:
            Домен (без www, в нижнем регистре)
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path.split('/')[0]
            # Убираем www.
            domain = re.sub(r'^www\.', '', domain.lower())
            return domain
        except Exception:
            return ""
    
    def extract_prices_from_offer_info(self, offer_info_text: str) -> Optional[Dict]:
        """
        Извлекает цены из <offer_info> JSON.
        
        Args:
            offer_info_text: JSON текст из <offer_info>
            
        Returns:
            Dict с ценами или None
        """
        try:
            data = json.loads(offer_info_text)
            
            # Извлекаем цену
            price = None
            currency = None
            oldprice = None
            discount_percent = None
            
            if 'price' in data and isinstance(data['price'], dict):
                price = data['price'].get('value')
                currency = data['price'].get('currency', 'RUR')
            
            # Извлекаем скидку если есть
            if 'discount' in data and isinstance(data['discount'], dict):
                oldprice = data['discount'].get('oldprice')
                discount_percent = data['discount'].get('percent')
            
            if price:
                return {
                    'price': price,
                    'currency': currency,
                    'oldprice': oldprice,
                    'discount_percent': discount_percent,
                    'has_discount': oldprice is not None
                }
        except (json.JSONDecodeError, TypeError, KeyError):
            pass
        
        return None
    
    def count_offers_in_xml(self, xml_response: str) -> Tuple[int, int, int, List[Dict]]:
        """
        Подсчитывает количество документов с <offer_info> и коммерческих доменов в XML.
        
        Args:
            xml_response: XML ответ от Яндекс.XML
            
        Returns:
            Tuple (docs_with_offers, commercial_domains_count, total_docs_analyzed, price_data)
        """
        if not xml_response:
            return (0, 0, self.top_n, [])
        
        try:
            root = ET.fromstring(xml_response)
        except ET.ParseError:
            return (0, 0, 0, [])
        
        # Находим все группы (каждая группа = 1 документ в выдаче)
        grouping = root.find('.//grouping')
        if grouping is None:
            return (0, 0, 0, [])
        
        groups = grouping.findall('.//group')[:self.top_n]
        
        if not groups:
            return (0, 0, 0, [])
        
        docs_with_offers = 0
        commercial_domains_count = 0
        price_data = []
        seen_domains = set()  # Чтобы не считать один домен дважды
        
        for group in groups:
            # Извлекаем URL из группы
            doc = group.find('.//doc')
            if doc is not None:
                url_elem = doc.find('url')
                if url_elem is not None and url_elem.text:
                    domain = self._extract_domain_from_url(url_elem.text)
                    if domain and domain in self.commercial_domains:
                        if domain not in seen_domains:
                            commercial_domains_count += 1
                            seen_domains.add(domain)
            
            # Проверяем есть ли <offer_info> внутри группы
            group_xml = ET.tostring(group, encoding='unicode')
            
            if '<offer_info>' in group_xml or 'offer_info' in group_xml:
                docs_with_offers += 1
                
                # Извлекаем цены
                offer_infos = re.findall(r'<offer_info>(.*?)</offer_info>', group_xml, re.DOTALL)
                for offer_text in offer_infos:
                    price_info = self.extract_prices_from_offer_info(offer_text)
                    if price_info:
                        price_data.append(price_info)
        
        return (docs_with_offers, commercial_domains_count, len(groups), price_data)
    
    def calculate_price_stats(self, price_data: List[Dict]) -> Dict:
        """
        Рассчитывает статистику по ценам.
        
        Args:
            price_data: Список словарей с ценами
            
        Returns:
            Dict со статистикой цен
        """
        if not price_data:
            return {
                'avg_price': None,
                'min_price': None,
                'max_price': None,
                'median_price': None,
                'currency': None,
                'offers_count': 0,
                'offers_with_discount': 0,
                'avg_discount_percent': None
            }
        
        prices = [p['price'] for p in price_data if p['price']]
        
        if not prices:
            return {
                'avg_price': None,
                'min_price': None,
                'max_price': None,
                'median_price': None,
                'currency': None,
                'offers_count': 0,
                'offers_with_discount': 0,
                'avg_discount_percent': None
            }
        
        # Подсчитываем скидки
        discounts = [p['discount_percent'] for p in price_data if p.get('discount_percent')]
        offers_with_discount = sum(1 for p in price_data if p.get('has_discount'))
        
        return {
            'avg_price': round(mean(prices), 2),
            'min_price': min(prices),
            'max_price': max(prices),
            'median_price': round(median(prices), 2),
            'currency': price_data[0].get('currency', 'RUR'),
            'offers_count': len(prices),
            'offers_with_discount': offers_with_discount,
            'avg_discount_percent': round(mean(discounts), 1) if discounts else None
        }
    
    def classify_by_offers(
        self,
        xml_response: str,
        query: str = None
    ) -> Tuple[str, float, Dict]:
        """
        Классифицирует интент на основе коммерческих факторов.
        
        Приоритет 1 (100% коммерческий):
        - Если запрос содержит коммерческое слово из keyword_group/commercial.txt 
          в любом падеже → commercial с confidence=1.0
        
        Приоритет 2 (на основе факторов):
        - Документы с <offer_info> (независимо от домена)
        - Документы из коммерческих доменов (keywords_settings/commercial_domains.txt)
        - Если сумма факторов >= 7 → commercial
        
        Args:
            xml_response: XML ответ от Яндекс.XML
            query: Поисковый запрос (для проверки коммерческих слов)
            
        Returns:
            Tuple (intent, confidence, stats)
            - intent: 'commercial' или 'informational'
            - confidence: уверенность от 0 до 1
            - stats: {
                'docs_with_offers': int,
                'commercial_domains_count': int,
                'commercial_factors': int,  # docs_with_offers + commercial_domains_count
                'has_commercial_keyword': bool,  # True если есть коммерческое слово
                'total_docs': int,
                'offer_ratio': float,
                'avg_price': float,
                'min_price': float,
                'max_price': float,
                'median_price': float,
                'currency': str,
                'offers_count': int,
                'offers_with_discount': int,
                'avg_discount_percent': float
              }
        """
        # ПРИОРИТЕТ 1: Проверяем коммерческие слова в запросе
        has_commercial_keyword = False
        if query:
            has_commercial_keyword = self._has_commercial_keyword(query)
            if has_commercial_keyword:
                # Если есть коммерческое слово → 100% коммерческий
                docs_with_offers, commercial_domains_count, total_docs, price_data = self.count_offers_in_xml(xml_response)
                price_stats = self.calculate_price_stats(price_data)
                
                return ('commercial', 1.0, {
                    'docs_with_offers': docs_with_offers,
                    'commercial_domains_count': commercial_domains_count,
                    'commercial_factors': docs_with_offers + commercial_domains_count,
                    'has_commercial_keyword': True,
                    'total_docs': total_docs,
                    'offer_ratio': docs_with_offers / total_docs if total_docs > 0 else 0.0,
                    **price_stats
                })
        
        # ПРИОРИТЕТ 2: Анализируем факторы (offer_info + коммерческие домены)
        docs_with_offers, commercial_domains_count, total_docs, price_data = self.count_offers_in_xml(xml_response)
        
        if total_docs == 0:
            # Нет данных → по умолчанию informational (безопасный вариант)
            return ('informational', 0.5, {
                'docs_with_offers': 0,
                'commercial_domains_count': 0,
                'commercial_factors': 0,
                'has_commercial_keyword': False,
                'total_docs': 0,
                'offer_ratio': 0.0,
                **self.calculate_price_stats([])
            })
        
        # Считаем общее количество коммерческих факторов
        # Важно: один документ может иметь и offer_info и быть из коммерческого домена
        # Но мы считаем их как отдельные факторы (как просил пользователь)
        commercial_factors = docs_with_offers + commercial_domains_count
        
        offer_ratio = docs_with_offers / total_docs if total_docs > 0 else 0.0
        
        # Определяем интент: если коммерческих факторов >= 7 → commercial
        is_commercial = commercial_factors >= self.commercial_threshold
        
        intent = 'commercial' if is_commercial else 'informational'
        
        # Рассчитываем уверенность на основе коммерческих факторов
        if is_commercial:
            # Чем больше факторов → тем выше уверенность
            # 7 факторов = 0.7, 10+ факторов = 1.0
            confidence = min(commercial_factors / 10.0, 1.0)
        else:
            # Чем меньше факторов → тем выше уверенность что это informational
            # 0 факторов = 1.0, 6 факторов = 0.6
            confidence = max(1.0 - (commercial_factors / 10.0), 0.5)
        
        # Собираем статистику
        price_stats = self.calculate_price_stats(price_data)
        
        stats = {
            'docs_with_offers': docs_with_offers,
            'commercial_domains_count': commercial_domains_count,
            'commercial_factors': commercial_factors,
            'has_commercial_keyword': has_commercial_keyword,
            'total_docs': total_docs,
            'offer_ratio': round(offer_ratio, 3),
            **price_stats
        }
        
        return (intent, confidence, stats)
    
    def classify_batch(
        self,
        xml_responses: list[str],
        queries: list[str] = None
    ) -> list[Dict[str, any]]:
        """
        Классифицирует список запросов по их XML и извлекает цены.
        
        Args:
            xml_responses: Список XML ответов
            queries: Список поисковых запросов (для проверки коммерческих слов)
            
        Returns:
            Список результатов классификации с ценовыми данными
        """
        results = []
        
        # Если queries не передан, используем None для каждого запроса
        if queries is None:
            queries = [None] * len(xml_responses)
        
        for i, xml_response in enumerate(xml_responses):
            query = queries[i] if i < len(queries) else None
            intent, confidence, stats = self.classify_by_offers(xml_response, query)
            
            results.append({
                'serp_intent': intent,
                'serp_confidence': confidence,
                'serp_docs_with_offers': stats['docs_with_offers'],
                'serp_total_docs': stats['total_docs'],
                'serp_offer_ratio': stats['offer_ratio'],
                # Ценовые данные
                'serp_avg_price': stats['avg_price'],
                'serp_min_price': stats['min_price'],
                'serp_max_price': stats['max_price'],
                'serp_median_price': stats['median_price'],
                'serp_currency': stats['currency'],
                'serp_offers_count': stats['offers_count'],
                'serp_offers_with_discount': stats['offers_with_discount'],
                'serp_avg_discount_percent': stats['avg_discount_percent']
            })
        
        return results


def classify_intent_by_serp_offers(
    xml_response: str,
    query: str = None,
    top_n: int = 20,
    commercial_threshold: int = 7
) -> str:
    """
    Быстрая функция для одиночной классификации.
    
    Args:
        xml_response: XML ответ от Яндекс.XML
        query: Поисковый запрос (для проверки коммерческих слов)
        top_n: Анализировать топ-N документов
        commercial_threshold: Минимум коммерческих факторов для commercial
        
    Returns:
        'commercial' или 'informational'
    """
    classifier = SERPOfferClassifier(top_n, commercial_threshold)
    intent, _, _ = classifier.classify_by_offers(xml_response, query)
    return intent

