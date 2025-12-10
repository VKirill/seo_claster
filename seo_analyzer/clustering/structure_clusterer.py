"""Кластеризация по структуре запросов

Устаревший модуль. Используйте seo_analyzer.clustering.structure.clusterer.StructureClusterer
"""

from .structure.clusterer import StructureClusterer, ModifierClusterer

__all__ = ['StructureClusterer', 'ModifierClusterer']

