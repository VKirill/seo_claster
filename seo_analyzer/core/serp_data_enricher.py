"""
SERP Data Enricher Module (фасад для обратной совместимости)
Извлечение метрик из XML ответов xmlstock для расчета KEI
"""

from typing import Dict

from .serp_enricher import SERPDataEnricher as SERPDataEnricherImpl
from .serp_enricher import get_empty_metrics


class SERPDataEnricher:
    """
    Извлечение дополнительных метрик из SERP XML
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.core.serp_enricher
    """
    
    def __init__(self):
        self._enricher = SERPDataEnricherImpl()
        # Делегируем атрибуты для обратной совместимости
        self.xml_extractor = self._enricher.xml_extractor
        self.commercial_patterns = self._enricher.commercial_patterns
        self.info_patterns = self._enricher.info_patterns
    
    def enrich_from_serp(self, serp_xml: str, original_query: str = None) -> Dict[str, any]:
        """Извлечь все метрики из SERP XML"""
        return self._enricher.enrich_from_serp(serp_xml, original_query)
    
    def calculate_serp_difficulty(self, metrics: Dict[str, int]) -> float:
        """Рассчитать сложность продвижения на основе SERP метрик"""
        return self._enricher.calculate_serp_difficulty(metrics)
    
    def _get_empty_metrics(self) -> Dict[str, int]:
        """Получить пустые метрики для обратной совместимости"""
        return get_empty_metrics()
