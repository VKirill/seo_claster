"""Основная модель Topic Modeling"""

from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
from sklearn.decomposition import LatentDirichletAllocation, NMF

from .vectorizer import create_lda_vectorizer, create_nmf_vectorizer
from .analyzer import get_topic_top_words, get_all_topics, get_topic_distribution


class TopicModeler:
    """Topic Modeling с использованием LDA и NMF"""
    
    def __init__(self, config: Dict = None):
        """
        Инициализация модели
        
        Args:
            config: Конфигурация
        """
        self.config = config or {}
        self.vectorizer = None
        self.model = None
        self.feature_names = None
        self.doc_topic_matrix = None
        self.n_topics = None
        self.method = None
    
    def fit_lda(
        self,
        texts: List[str],
        n_topics: Optional[int] = None,
        max_iter: int = 50
    ) -> 'TopicModeler':
        """
        Обучает LDA модель
        
        Args:
            texts: Список текстов
            n_topics: Количество тем (если None, автоопределение)
            max_iter: Максимум итераций
            
        Returns:
            Self
        """
        # Определяем количество тем
        if n_topics is None:
            topic_config = self.config.get('topic_modeling', {})
            n_topics_range = topic_config.get('n_topics_range', (5, 20))
            n_topics = self._find_optimal_topics(texts, n_topics_range, 'lda')
        
        self.n_topics = n_topics
        self.method = 'lda'
        
        print(f"🔄 Обучение LDA (темы={n_topics})...")
        
        # Используем CountVectorizer для LDA
        self.vectorizer = create_lda_vectorizer()
        
        doc_term_matrix = self.vectorizer.fit_transform(texts)
        self.feature_names = self.vectorizer.get_feature_names_out()
        
        # Обучаем LDA
        self.model = LatentDirichletAllocation(
            n_components=n_topics,
            max_iter=max_iter,
            random_state=42,
            n_jobs=-1
        )
        
        self.doc_topic_matrix = self.model.fit_transform(doc_term_matrix)
        
        print(f"✓ LDA обучена: {n_topics} тем")
        return self
    
    def fit_nmf(
        self,
        texts: List[str],
        n_topics: Optional[int] = None,
        max_iter: int = 200
    ) -> 'TopicModeler':
        """
        Обучает NMF модель
        
        Args:
            texts: Список текстов
            n_topics: Количество тем
            max_iter: Максимум итераций
            
        Returns:
            Self
        """
        if n_topics is None:
            topic_config = self.config.get('topic_modeling', {})
            n_topics_range = topic_config.get('n_topics_range', (5, 20))
            n_topics = self._find_optimal_topics(texts, n_topics_range, 'nmf')
        
        self.n_topics = n_topics
        self.method = 'nmf'
        
        print(f"🔄 Обучение NMF (темы={n_topics})...")
        
        # Используем TF-IDF для NMF
        self.vectorizer = create_nmf_vectorizer()
        
        doc_term_matrix = self.vectorizer.fit_transform(texts)
        self.feature_names = self.vectorizer.get_feature_names_out()
        
        # Обучаем NMF
        self.model = NMF(
            n_components=n_topics,
            max_iter=max_iter,
            random_state=42,
            init='nndsvda',
            alpha_W=0.01,
            alpha_H=0.01
        )
        
        self.doc_topic_matrix = self.model.fit_transform(doc_term_matrix)
        
        print(f"✓ NMF обучена: {n_topics} тем")
        return self
    
    def _find_optimal_topics(
        self,
        texts: List[str],
        n_range: Tuple[int, int],
        method: str
    ) -> int:
        """
        Находит оптимальное количество тем
        
        Args:
            texts: Тексты
            n_range: Диапазон тем (min, max)
            method: Метод ('lda' или 'nmf')
            
        Returns:
            Оптимальное количество тем
        """
        print(f"🔍 Поиск оптимального числа тем для {method}...")
        
        min_topics, max_topics = n_range
        
        # Для простоты берем среднее
        # В полной версии можно использовать perplexity или coherence score
        optimal = (min_topics + max_topics) // 2
        
        print(f"✓ Выбрано {optimal} тем")
        return optimal
    
    def get_topic_top_words(
        self,
        topic_id: int,
        top_n: int = 10
    ) -> List[Tuple[str, float]]:
        """Возвращает топ-слова для темы"""
        return get_topic_top_words(self.model, self.feature_names, topic_id, top_n)
    
    def get_all_topics(self, top_n: int = 10) -> Dict[int, Dict]:
        """Возвращает информацию о всех темах"""
        return get_all_topics(self.model, self.feature_names, self.n_topics, top_n)
    
    def assign_topics(
        self,
        texts: List[str],
        threshold: float = 0.3
    ) -> List[Tuple[int, float]]:
        """
        Присваивает темы текстам (hard clustering)
        
        Args:
            texts: Тексты
            threshold: Минимальная вероятность темы
            
        Returns:
            Список (topic_id, probability)
        """
        if self.model is None:
            raise ValueError("Модель не обучена")
        
        # Если тексты новые, трансформируем их
        if self.doc_topic_matrix is None or len(texts) != len(self.doc_topic_matrix):
            doc_term_matrix = self.vectorizer.transform(texts)
            doc_topics = self.model.transform(doc_term_matrix)
        else:
            doc_topics = self.doc_topic_matrix
        
        results = []
        
        for doc_topic in doc_topics:
            # Находим тему с максимальной вероятностью
            max_topic_id = doc_topic.argmax()
            max_prob = doc_topic[max_topic_id]
            
            if max_prob >= threshold:
                results.append((int(max_topic_id), float(max_prob)))
            else:
                results.append((-1, 0.0))  # Нет четкой темы
        
        return results
    
    def assign_topics_soft(
        self,
        texts: List[str],
        top_k: int = 3,
        min_probability: float = 0.1
    ) -> List[List[Tuple[int, float]]]:
        """
        Присваивает темы текстам (soft clustering) - несколько тем на запрос
        
        Args:
            texts: Тексты
            top_k: Максимум тем на документ
            min_probability: Минимальная вероятность для включения темы
            
        Returns:
            Список списков [(topic_id, probability), ...]
        """
        if self.model is None:
            raise ValueError("Модель не обучена")
        
        # Если тексты новые, трансформируем их
        if self.doc_topic_matrix is None or len(texts) != len(self.doc_topic_matrix):
            doc_term_matrix = self.vectorizer.transform(texts)
            doc_topics = self.model.transform(doc_term_matrix)
        else:
            doc_topics = self.doc_topic_matrix
        
        results = []
        
        for doc_topic in doc_topics:
            # Находим топ-k тем с вероятностью выше порога
            topic_probs = [
                (topic_id, prob)
                for topic_id, prob in enumerate(doc_topic)
                if prob >= min_probability
            ]
            
            # Сортируем по вероятности
            topic_probs = sorted(topic_probs, key=lambda x: x[1], reverse=True)[:top_k]
            
            if not topic_probs:
                # Если нет тем выше порога, берем самую вероятную
                max_topic_id = doc_topic.argmax()
                topic_probs = [(int(max_topic_id), float(doc_topic[max_topic_id]))]
            
            results.append([(int(tid), float(prob)) for tid, prob in topic_probs])
        
        return results
    
    def add_topics_to_dataframe(
        self,
        df: pd.DataFrame,
        text_column: str = 'lemmatized',
        use_soft_clustering: bool = False
    ) -> pd.DataFrame:
        """
        Добавляет темы в DataFrame
        
        Args:
            df: DataFrame
            text_column: Колонка с текстом
            use_soft_clustering: Использовать soft clustering (несколько тем)
            
        Returns:
            DataFrame с добавленными колонками
        """
        print("🔄 Присваивание тем запросам...")
        
        texts = df[text_column].tolist()
        
        if use_soft_clustering:
            # Soft clustering - несколько тем на запрос
            topic_assignments = self.assign_topics_soft(texts, top_k=3, min_probability=0.15)
            
            # Основная тема (самая вероятная)
            df['topic_id'] = [t[0][0] if t else -1 for t in topic_assignments]
            df['topic_probability'] = [t[0][1] if t else 0.0 for t in topic_assignments]
            
            # Все темы как список
            df['all_topics'] = [
                [(tid, round(prob, 3)) for tid, prob in topics]
                for topics in topic_assignments
            ]
            
            # Все темы как строка (для CSV)
            df['all_topics_str'] = [
                ', '.join([f"T{tid}({prob:.2f})" for tid, prob in topics])
                for topics in topic_assignments
            ]
        else:
            # Hard clustering - одна тема
            topic_assignments = self.assign_topics(texts)
            
            df['topic_id'] = [t[0] for t in topic_assignments]
            df['topic_probability'] = [t[1] for t in topic_assignments]
        
        # Добавляем названия тем
        topics_info = self.get_all_topics()
        df['topic_name'] = df['topic_id'].map(
            lambda x: topics_info.get(x, {}).get('topic_name', 'No Topic')
        )
        
        clustering_type = "soft" if use_soft_clustering else "hard"
        print(f"✓ Темы присвоены ({clustering_type} clustering)")
        return df
    
    def get_topic_distribution(self, texts: List[str]) -> Dict[int, int]:
        """Возвращает распределение текстов по темам"""
        topic_assignments = self.assign_topics(texts)
        return get_topic_distribution(topic_assignments)

