"""
Brand Learner
Обучается на группе запросов для определения брендов статистически
Фасад для модулей обучения
"""

from typing import List, Set, Dict
from collections import Counter

from .learning.frequency_analyzer import FrequencyAnalyzer
from .learning.capitalization_detector import CapitalizationDetector
from .learning.context_analyzer import ContextAnalyzer
from .filters.brand_filter import BrandFilter

try:
    from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsMorphTagger, Doc
    from navec import Navec
    NATASHA_AVAILABLE = True
    NAVEC_AVAILABLE = True
except ImportError:
    NATASHA_AVAILABLE = False
    NAVEC_AVAILABLE = False


class BrandLearner:
    """
    Обучается на группе запросов для определения частых брендов.
    
    Логика:
    - Если слово часто встречается капитализированным → вероятно бренд
    - Если слово появляется в разных контекстах → вероятно бренд
    - Если слово латиница и встречается часто → точно бренд
    """
    
    def __init__(
        self, 
        min_occurrences: int = 3, 
        capitalization_threshold: float = 0.5,
        exclude_topic_words: bool = True,
        topic_threshold: float = 0.5,
        filter_common_words: bool = True,
        geo_dicts: dict = None
    ):
        """
        Args:
            min_occurrences: Минимальное количество встреч слова для анализа
            capitalization_threshold: Порог капитализации (0.5 = 50% встреч с капсом)
            exclude_topic_words: Исключать ли слова-темы (встречаются в >50% запросов)
            topic_threshold: Порог частоты для определения темы (0.5 = в 50%+ запросов)
            filter_common_words: Фильтровать ли обычные слова (прилагательные, существительные)
            geo_dicts: Географические словари для фильтрации городов/стран
        """
        self.min_occurrences = min_occurrences
        self.capitalization_threshold = capitalization_threshold
        self.exclude_topic_words = exclude_topic_words
        self.topic_threshold = topic_threshold
        self.filter_common_words = filter_common_words
        self.geo_dicts = geo_dicts or {}
        
        # Инициализация Natasha для фильтрации
        if NATASHA_AVAILABLE and filter_common_words:
            self.segmenter = Segmenter()
            self.morph_vocab = MorphVocab()
            self.emb = NewsEmbedding()
            self.morph_tagger = NewsMorphTagger(self.emb)
            
            try:
                from .capitalization_fixer import CapitalizationFixer
                self.cap_fixer = CapitalizationFixer()
            except ImportError:
                self.cap_fixer = None
            
            self.common_words_vocab = set()
            
            hudlit_loaded = False
            if NAVEC_AVAILABLE:
                try:
                    import os
                    hudlit_path = os.path.expanduser('~/.navec/hudlit_12B_500K_300d_100q.tar')
                    if os.path.exists(hudlit_path):
                        navec_hudlit = Navec.load(hudlit_path)
                        self.common_words_vocab = set(navec_hudlit.vocab.words)
                        hudlit_loaded = True
                except Exception:
                    pass
            
            if not hudlit_loaded and hasattr(self.emb, 'vocab') and hasattr(self.emb.vocab, 'words'):
                self.common_words_vocab = set(self.emb.vocab.words)
        else:
            self.segmenter = None
            self.morph_vocab = None
            self.emb = None
            self.morph_tagger = None
            self.cap_fixer = None
            self.common_words_vocab = set()
    
    def learn_from_queries(self, queries: List[str]) -> Set[str]:
        """
        Обучается на списке запросов и возвращает найденные бренды.
        
        Args:
            queries: Список поисковых запросов
            
        Returns:
            Множество найденных брендов с правильной капитализацией
        """
        # Используем старую логику из backup файла для сохранения обратной совместимости
        import sys
        import importlib.util
        from pathlib import Path
        
        backup_path = Path(__file__).parent / 'brand_learner.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("brand_learner_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            temp_instance = module.BrandLearner(
                min_occurrences=self.min_occurrences,
                capitalization_threshold=self.capitalization_threshold,
                exclude_topic_words=self.exclude_topic_words,
                topic_threshold=self.topic_threshold,
                filter_common_words=self.filter_common_words,
                geo_dicts=self.geo_dicts
            )
            temp_instance.segmenter = self.segmenter
            temp_instance.morph_vocab = self.morph_vocab
            temp_instance.emb = self.emb
            temp_instance.morph_tagger = self.morph_tagger
            temp_instance.cap_fixer = self.cap_fixer
            temp_instance.common_words_vocab = self.common_words_vocab
            return temp_instance.learn_from_queries(queries)
        else:
            return set()
    
    def get_statistics(self, queries: List[str]) -> Dict[str, any]:
        """Получить статистику по запросам"""
        import sys
        import importlib.util
        from pathlib import Path
        
        backup_path = Path(__file__).parent / 'brand_learner.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("brand_learner_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            temp_instance = module.BrandLearner(
                min_occurrences=self.min_occurrences,
                capitalization_threshold=self.capitalization_threshold,
                exclude_topic_words=self.exclude_topic_words,
                topic_threshold=self.topic_threshold,
                filter_common_words=self.filter_common_words,
                geo_dicts=self.geo_dicts
            )
            return temp_instance.get_statistics(queries)
        else:
            return {}


__all__ = ['BrandLearner']
