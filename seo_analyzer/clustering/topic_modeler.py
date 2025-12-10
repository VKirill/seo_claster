"""Topic Modeling для выделения скрытых тем (фасад для обратной совместимости)"""

from typing import Dict, List, Tuple, Optional
import pandas as pd

from .topic import TopicModeler as TopicModelerImpl


class TopicModeler:
    """
    Topic Modeling с использованием LDA и NMF
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.clustering.topic
    """
    
    def __init__(self, config: Dict = None):
        self._modeler = TopicModelerImpl(config)
        # Делегируем атрибуты для обратной совместимости
        self.config = config or {}
        self.vectorizer = self._modeler.vectorizer
        self.model = self._modeler.model
        self.feature_names = self._modeler.feature_names
        self.doc_topic_matrix = self._modeler.doc_topic_matrix
        self.n_topics = self._modeler.n_topics
        self.method = self._modeler.method
    
    def fit_lda(self, texts: List[str], n_topics: Optional[int] = None, max_iter: int = 50) -> 'TopicModeler':
        """Обучает LDA модель"""
        result = self._modeler.fit_lda(texts, n_topics, max_iter)
        self._sync_attributes()
        return self
    
    def fit_nmf(self, texts: List[str], n_topics: Optional[int] = None, max_iter: int = 200) -> 'TopicModeler':
        """Обучает NMF модель"""
        result = self._modeler.fit_nmf(texts, n_topics, max_iter)
        self._sync_attributes()
        return self
    
    def get_topic_top_words(self, topic_id: int, top_n: int = 10) -> List[Tuple[str, float]]:
        """Возвращает топ-слова для темы"""
        return self._modeler.get_topic_top_words(topic_id, top_n)
    
    def get_all_topics(self, top_n: int = 10) -> Dict[int, Dict]:
        """Возвращает информацию о всех темах"""
        return self._modeler.get_all_topics(top_n)
    
    def assign_topics(self, texts: List[str], threshold: float = 0.3) -> List[Tuple[int, float]]:
        """Присваивает темы текстам (hard clustering)"""
        return self._modeler.assign_topics(texts, threshold)
    
    def assign_topics_soft(self, texts: List[str], top_k: int = 3, min_probability: float = 0.1) -> List[List[Tuple[int, float]]]:
        """Присваивает темы текстам (soft clustering)"""
        return self._modeler.assign_topics_soft(texts, top_k, min_probability)
    
    def add_topics_to_dataframe(self, df: pd.DataFrame, text_column: str = 'lemmatized', use_soft_clustering: bool = False) -> pd.DataFrame:
        """Добавляет темы в DataFrame"""
        return self._modeler.add_topics_to_dataframe(df, text_column, use_soft_clustering)
    
    def get_topic_distribution(self, texts: List[str]) -> Dict[int, int]:
        """Возвращает распределение текстов по темам"""
        return self._modeler.get_topic_distribution(texts)
    
    def _sync_attributes(self):
        """Синхронизирует атрибуты для обратной совместимости"""
        self.vectorizer = self._modeler.vectorizer
        self.model = self._modeler.model
        self.feature_names = self._modeler.feature_names
        self.doc_topic_matrix = self._modeler.doc_topic_matrix
        self.n_topics = self._modeler.n_topics
        self.method = self._modeler.method

