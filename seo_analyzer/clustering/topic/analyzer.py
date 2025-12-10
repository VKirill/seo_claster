"""Анализ тем в Topic Modeling"""

from typing import Dict, List, Tuple
from collections import Counter


def get_topic_top_words(
    model,
    feature_names,
    topic_id: int,
    top_n: int = 10
) -> List[Tuple[str, float]]:
    """
    Возвращает топ-слова для темы
    
    Args:
        model: Обученная модель (LDA или NMF)
        feature_names: Названия признаков (слова)
        topic_id: ID темы
        top_n: Количество слов
        
    Returns:
        Список (слово, вес)
    """
    if model is None:
        return []
    
    topic_weights = model.components_[topic_id]
    top_indices = topic_weights.argsort()[-top_n:][::-1]
    
    top_words = [
        (feature_names[i], topic_weights[i])
        for i in top_indices
    ]
    
    return top_words


def get_all_topics(
    model,
    feature_names,
    n_topics: int,
    top_n: int = 10
) -> Dict[int, Dict]:
    """
    Возвращает информацию о всех темах
    
    Args:
        model: Обученная модель
        feature_names: Названия признаков
        n_topics: Количество тем
        top_n: Количество топ-слов
        
    Returns:
        Словарь с темами
    """
    if model is None:
        return {}
    
    topics = {}
    
    for topic_id in range(n_topics):
        top_words = get_topic_top_words(model, feature_names, topic_id, top_n)
        
        # Генерируем название темы из топ-3 слов
        topic_name = " + ".join([word for word, _ in top_words[:3]])
        
        topics[topic_id] = {
            'topic_id': topic_id,
            'topic_name': topic_name,
            'top_words': top_words,
        }
    
    return topics


def get_topic_distribution(topic_assignments: List[Tuple[int, float]]) -> Dict[int, int]:
    """
    Возвращает распределение текстов по темам
    
    Args:
        topic_assignments: Список (topic_id, probability)
        
    Returns:
        Словарь {topic_id: count}
    """
    topic_counter = Counter([t[0] for t in topic_assignments if t[0] >= 0])
    return dict(topic_counter)

