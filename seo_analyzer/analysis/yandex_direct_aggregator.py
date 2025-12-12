"""
Агрегация метрик Yandex Direct для кластеров.

Вычисляет суммарные и средневзвешенные показатели кластера
на основе данных отдельных запросов.
"""

from typing import Dict


def aggregate_cluster_metrics(direct_data: Dict[str, Dict]) -> Dict:
    """
    Агрегация метрик для кластера.
    
    Args:
        direct_data: Dict[query -> direct_metrics]
        
    Returns:
        Dict с агрегированными метриками кластера
    """
    if not direct_data:
        return {
            "total_shows": 0,
            "total_clicks": 0,
            "avg_cpc": 0.0,
            "competition_level": "unknown",
            "recommended_cpc": 0.0
        }
        
    total_shows = sum(d["shows"] for d in direct_data.values())
    total_clicks = sum(d["clicks"] for d in direct_data.values())
    
    # Средневзвешенный CPC
    weighted_cpc = sum(d["avg_cpc"] * d["shows"] for d in direct_data.values())
    avg_cpc = weighted_cpc / total_shows if total_shows > 0 else 0.0
    
    # Рекомендуемый CPC (медиана)
    recommended_cpcs = sorted([d["recommended_cpc"] for d in direct_data.values()])
    mid = len(recommended_cpcs) // 2
    recommended_cpc = recommended_cpcs[mid] if recommended_cpcs else 0.0
    
    # Уровень конкуренции (наиболее частый)
    competition_levels = [d["competition_level"] for d in direct_data.values()]
    competition_level = max(set(competition_levels), key=competition_levels.count)
    
    return {
        "total_shows": total_shows,
        "total_clicks": total_clicks,
        "avg_cpc": round(avg_cpc, 2),
        "competition_level": competition_level,
        "recommended_cpc": round(recommended_cpc, 2)
    }


def get_empty_metrics(query: str) -> Dict:
    """
    Пустые метрики при отсутствии данных.
    
    Args:
        query: Запрос
        
    Returns:
        Dict с нулевыми значениями метрик
    """
    return {
        "phrase": query,
        "shows": 0,
        "clicks": 0,
        "ctr": 0.0,
        "premium_ctr": 0.0,
        "min_cpc": 0.0,
        "avg_cpc": 0.0,
        "max_cpc": 0.0,
        "recommended_cpc": 0.0,
        "competition_level": "unknown",
        "first_place_bid": 0.0,
        "first_place_price": 0.0
    }





