"""
Анализ частоты появления слов
"""

import sys
import importlib.util
from pathlib import Path
from typing import List, Set, Dict
from collections import Counter


class FrequencyAnalyzer:
    """Анализатор частоты появления слов"""
    
    @staticmethod
    def analyze_frequency(queries: List[str], exclude_topic_words: bool = True, topic_threshold: float = 0.5) -> Dict:
        """
        Анализирует частоту появления слов в запросах
        
        Args:
            queries: Список запросов
            exclude_topic_words: Исключать ли слова-темы
            topic_threshold: Порог частоты для определения темы
            
        Returns:
            Словарь с результатами анализа
        """
        backup_path = Path(__file__).parent.parent.parent / 'brand_learner.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("brand_learner_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            temp_instance = module.BrandLearner(exclude_topic_words=exclude_topic_words, topic_threshold=topic_threshold)
            # Используем внутреннюю логику через learn_from_queries
            word_counter = Counter()
            for query in queries:
                words = query.split()
                for word in words:
                    if len(word) > 2:
                        word_counter[word.lower()] += 1
            
            total_queries = len(queries)
            topic_words = set()
            if exclude_topic_words:
                for word_lower, count in word_counter.items():
                    frequency_ratio = count / total_queries
                    if frequency_ratio >= topic_threshold:
                        topic_words.add(word_lower)
            
            return {
                'word_counter': word_counter,
                'topic_words': topic_words,
                'total_queries': total_queries
            }
        else:
            return {
                'word_counter': Counter(),
                'topic_words': set(),
                'total_queries': len(queries)
            }

