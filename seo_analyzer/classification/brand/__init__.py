"""
Brand Detection Module
Модули для определения брендов в запросах

Использует комбинацию:
- Эвристического анализа (капс, паттерны, морфология)
- NER (Named Entity Recognition) через Natasha для извлечения ORG
- Статистическое обучение на группе запросов (BrandLearner)
"""

from .detector import BrandDetector
from .analyzer import BrandAnalyzer
from .ner_enhancer import NERBrandEnhancer
from .brand_learner import BrandLearner
from .capitalization_fixer import CapitalizationFixer

__all__ = [
    'BrandDetector',
    'BrandAnalyzer',
    'NERBrandEnhancer',
    'BrandLearner',
    'CapitalizationFixer'
]


