"""Методы семантической кластеризации"""

from typing import Dict, List, Optional
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans, DBSCAN
from sklearn.metrics import silhouette_score
from tqdm import tqdm


class SemanticClusterer:
    """Семантическая кластеризация на основе TF-IDF"""
    
    def __init__(self, config: Dict = None):
        """
        Инициализация кластеризатора
        
        Args:
            config: Конфигурация TF-IDF и кластеризации
        """
        self.config = config or {}
        
        # TF-IDF параметры
        tfidf_params = self.config.get('tfidf', {})
        self.vectorizer = TfidfVectorizer(
            max_features=tfidf_params.get('max_features', 1000),
            min_df=tfidf_params.get('min_df', 2),
            max_df=tfidf_params.get('max_df', 0.8),
            ngram_range=tfidf_params.get('ngram_range', (1, 3)),
        )
        
        self.tfidf_matrix = None
        self.feature_names = None
        self.cluster_labels = None
        self.n_clusters = None
    
    def fit_tfidf(self, texts: List[str]) -> np.ndarray:
        """
        Обучает TF-IDF на текстах
        
        Args:
            texts: Список текстов
            
        Returns:
            TF-IDF матрица
        """
        print("🔄 Векторизация TF-IDF...")
        self.tfidf_matrix = self.vectorizer.fit_transform(texts)
        self.feature_names = self.vectorizer.get_feature_names_out()
        print(f"✓ Создана матрица {self.tfidf_matrix.shape}")
        return self.tfidf_matrix
    
    def find_optimal_clusters(
        self,
        min_clusters: int = 5,
        max_clusters: int = 50,
        method: str = 'elbow'
    ) -> int:
        """
        Находит оптимальное количество кластеров
        
        Args:
            min_clusters: Минимальное количество
            max_clusters: Максимальное количество
            method: Метод ('elbow' или 'silhouette')
            
        Returns:
            Оптимальное количество кластеров
        """
        if self.tfidf_matrix is None:
            raise ValueError("Сначала нужно вызвать fit_tfidf()")
        
        print(f"🔍 Поиск оптимального числа кластеров ({method})...")
        
        inertias = []
        silhouette_scores = []
        k_range = range(min_clusters, min(max_clusters, len(self.tfidf_matrix.toarray()) // 2))
        
        for k in tqdm(k_range, desc="Тестирование k"):
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(self.tfidf_matrix)
            
            inertias.append(kmeans.inertia_)
            
            if len(set(labels)) > 1:  # Нужно минимум 2 кластера для silhouette
                score = silhouette_score(self.tfidf_matrix, labels, sample_size=1000)
                silhouette_scores.append(score)
            else:
                silhouette_scores.append(0)
        
        if method == 'silhouette' and silhouette_scores:
            optimal_k = k_range[np.argmax(silhouette_scores)]
            print(f"✓ Оптимальное k (silhouette): {optimal_k}")
        else:
            # Elbow method - ищем точку перегиба
            if len(inertias) >= 3:
                # Простая эвристика: где уменьшение inertia становится < 10%
                diffs = np.diff(inertias)
                percent_changes = np.abs(diffs / inertias[:-1])
                elbow_idx = np.where(percent_changes < 0.1)[0]
                
                if len(elbow_idx) > 0:
                    optimal_k = k_range[elbow_idx[0]]
                else:
                    # Берем середину диапазона
                    optimal_k = k_range[len(k_range) // 2]
            else:
                optimal_k = min_clusters
            
            print(f"✓ Оптимальное k (elbow): {optimal_k}")
        
        return optimal_k
    
    def cluster_kmeans(
        self,
        n_clusters: Optional[int] = None,
        auto_detect: bool = True
    ) -> np.ndarray:
        """
        Кластеризация K-Means
        
        Args:
            n_clusters: Количество кластеров (если None, автоопределение)
            auto_detect: Автоматически определять k
            
        Returns:
            Массив меток кластеров
        """
        if self.tfidf_matrix is None:
            raise ValueError("Сначала нужно вызвать fit_tfidf()")
        
        if n_clusters is None and auto_detect:
            kmeans_config = self.config.get('kmeans', {})
            min_k, max_k = kmeans_config.get('n_clusters_range', (5, 50))
            n_clusters = self.find_optimal_clusters(min_k, max_k)
        elif n_clusters is None:
            n_clusters = 10
        
        self.n_clusters = n_clusters
        
        print(f"🔄 K-Means кластеризация (k={n_clusters})...")
        kmeans = KMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init=10,
            max_iter=300
        )
        
        self.cluster_labels = kmeans.fit_predict(self.tfidf_matrix)
        
        print(f"✓ Создано {n_clusters} кластеров")
        return self.cluster_labels
    
    def cluster_dbscan(self, eps: float = 0.3, min_samples: int = 3) -> np.ndarray:
        """
        Кластеризация DBSCAN
        
        Args:
            eps: Максимальное расстояние
            min_samples: Минимальное количество точек
            
        Returns:
            Массив меток кластеров
        """
        if self.tfidf_matrix is None:
            raise ValueError("Сначала нужно вызвать fit_tfidf()")
        
        print(f"🔄 DBSCAN кластеризация (eps={eps}, min_samples={min_samples})...")
        
        dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='cosine')
        self.cluster_labels = dbscan.fit_predict(self.tfidf_matrix)
        
        n_clusters = len(set(self.cluster_labels)) - (1 if -1 in self.cluster_labels else 0)
        n_noise = list(self.cluster_labels).count(-1)
        
        print(f"✓ Создано {n_clusters} кластеров, {n_noise} шумовых точек")
        
        self.n_clusters = n_clusters
        return self.cluster_labels

