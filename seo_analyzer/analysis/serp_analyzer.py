"""
SERP Analyzer Module
Анализ SERP через xmlstock API с кэшированием в SQLite

Устаревший модуль. Используйте seo_analyzer.analysis.serp.analyzer.SERPAnalyzer
"""

from .serp.analyzer import SERPAnalyzer

__all__ = ['SERPAnalyzer']

