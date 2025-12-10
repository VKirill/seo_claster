"""Основной класс обогащения SERP данных"""

import xml.etree.ElementTree as ET
from typing import Dict, Any

from .metrics_extractor import extract_metrics, calculate_serp_difficulty, get_empty_metrics
from .document_extractor import extract_documents
from ..xml_text_extractor import XMLTextExtractor
from ...classification.intent.serp_offer_classifier import SERPOfferClassifier


class SERPDataEnricher:
    """Извлечение дополнительных метрик из SERP XML"""
    
    def __init__(self):
        self.xml_extractor = XMLTextExtractor()
        # Паттерны коммерческих доменов
        self.commercial_patterns = [
            r'\.shop', r'\.store', r'market', r'shop', r'ozon', r'wildberries',
            r'avito', r'youla', r'aliexpress', r'goods', r'catalog', r'price'
        ]
        
        # Паттерны информационных доменов
        self.info_patterns = [
            r'wiki', r'blog', r'forum', r'otvet', r'answer', r'info',
            r'reference', r'encyclopedia', r'faq', r'help'
        ]
        
        # Классификатор для извлечения offer_info данных
        self.offer_classifier = SERPOfferClassifier(
            top_n=20,
            commercial_threshold=7,
            commercial_ratio=0.4
        )
    
    def enrich_from_serp(
        self,
        serp_xml: str,
        original_query: str = None
    ) -> Dict[str, Any]:
        """
        Извлечь все метрики из SERP XML
        
        Args:
            serp_xml: XML строка от xmlstock
            original_query: Оригинальный запрос для поиска в title
            
        Returns:
            Dict с метриками и списком документов
        """
        try:
            root = ET.fromstring(serp_xml)
        except ET.ParseError as e:
            return {
                'error': f'XML Parse Error: {e}',
                'metrics': get_empty_metrics(),
                'documents': []
            }
        
        # Извлекаем документы
        documents = extract_documents(root, original_query, self.xml_extractor, self.commercial_patterns)
        
        # Извлекаем метрики
        metrics = extract_metrics(root, documents, original_query, self.xml_extractor)
        
        # Извлекаем offer_info данные (serp_intent, цены и т.д.)
        try:
            intent, confidence, offer_stats = self.offer_classifier.classify_by_offers(serp_xml, original_query)
            
            # Добавляем offer_info данные в metrics
            metrics['serp_intent'] = intent
            metrics['serp_confidence'] = confidence
            metrics['docs_with_offers'] = offer_stats.get('docs_with_offers', 0)
            metrics['total_docs_analyzed'] = offer_stats.get('total_docs', 0)
            metrics['offer_ratio'] = offer_stats.get('offer_ratio', 0.0)
            metrics['avg_price'] = offer_stats.get('avg_price')
            metrics['min_price'] = offer_stats.get('min_price')
            metrics['max_price'] = offer_stats.get('max_price')
            metrics['median_price'] = offer_stats.get('median_price')
            metrics['currency'] = offer_stats.get('currency', 'RUR')
            metrics['offers_count'] = offer_stats.get('offers_count', 0)
            metrics['offers_with_discount'] = offer_stats.get('offers_with_discount', 0)
            metrics['avg_discount_percent'] = offer_stats.get('avg_discount_percent')
        except Exception as e:
            # Если ошибка при извлечении offer_info - заполняем пустыми значениями
            metrics['serp_intent'] = None
            metrics['serp_confidence'] = 0.0
            metrics['docs_with_offers'] = 0
            metrics['total_docs_analyzed'] = 0
            metrics['offer_ratio'] = 0.0
            metrics['avg_price'] = None
            metrics['min_price'] = None
            metrics['max_price'] = None
            metrics['median_price'] = None
            metrics['currency'] = 'RUR'
            metrics['offers_count'] = 0
            metrics['offers_with_discount'] = 0
            metrics['avg_discount_percent'] = None
        
        return {
            'metrics': metrics,
            'documents': documents,
            'error': None
        }
    
    def calculate_serp_difficulty(self, metrics: Dict[str, int]) -> float:
        """Рассчитать сложность продвижения на основе SERP метрик"""
        return calculate_serp_difficulty(metrics)

