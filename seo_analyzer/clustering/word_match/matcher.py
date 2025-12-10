"""Алгоритм сопоставления запросов для кластеризации"""

from typing import Set, Optional, List, Dict
import re


def tokenize_query(query: str, exclude_stopwords: bool = True) -> Set[str]:
    """
    Токенизирует запрос
    
    Args:
        query: Запрос
        exclude_stopwords: Исключать стоп-слова из подсчета совпадений
        
    Returns:
        Множество слов (токенов)
    """
    # Базовые стоп-слова для группировки
    basic_stopwords = {
        'в', 'на', 'с', 'из', 'к', 'по', 'для', 'и', 'или', 'а', 'но',
        'что', 'как', 'где', 'когда', 'это', 'весь', 'все', 'этот', 'тот'
    }
    
    # Убираем знаки препинания и приводим к нижнему регистру
    tokens = re.findall(r'\b[а-яёa-z0-9]+\b', query.lower())
    
    # Исключаем стоп-слова если нужно
    if exclude_stopwords:
        tokens = [t for t in tokens if t not in basic_stopwords]
    
    return set(tokens)


def find_best_cluster(
    query_tokens: Set[str],
    clusters: List[Dict],
    min_matches: int,
    count_matches_func
) -> Optional[int]:
    """
    Находит наилучший кластер для запроса
    
    Args:
        query_tokens: Токены запроса
        clusters: Список существующих кластеров
        min_matches: Минимальное количество совпадений
        count_matches_func: Функция для подсчета совпадений
        
    Returns:
        Индекс кластера или None
    """
    best_cluster_idx = None
    max_matches = 0
    
    for idx, cluster in enumerate(clusters):
        # Считаем среднее количество совпадений с запросами в кластере
        total_matches = 0
        for member_query in cluster['queries']:
            member_tokens = tokenize_query(member_query)
            matches = count_matches_func(query_tokens, member_tokens)
            total_matches += matches
        
        avg_matches = total_matches / len(cluster['queries']) if cluster['queries'] else 0
        
        # Если совпадений достаточно и это лучший результат
        if avg_matches >= min_matches and avg_matches > max_matches:
            max_matches = avg_matches
            best_cluster_idx = idx
    
    return best_cluster_idx

