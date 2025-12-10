"""
Проверка семантической совместимости запросов
"""

import sys
import importlib.util
from pathlib import Path


class SemanticChecker:
    """Проверка семантической совместимости"""
    
    @staticmethod
    def are_semantically_different(query1: str, query2: str, semantic_checker=None) -> bool:
        """
        Проверяет семантическую разницу между запросами
        
        Args:
            query1: Первый запрос
            query2: Второй запрос
            semantic_checker: Экземпляр SemanticClusterChecker
            
        Returns:
            True если запросы семантически разные
        """
        if semantic_checker is None:
            return False
        
        # Используем старую логику из backup файла
        backup_path = Path(__file__).parent.parent / 'serp_advanced_clusterer.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("serp_advanced_clusterer_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            # Создаём временный экземпляр для вызова метода
            temp_instance = module.AdvancedSERPClusterer()
            return temp_instance._are_semantically_different(query1, query2)
        else:
            # Fallback
            return False

