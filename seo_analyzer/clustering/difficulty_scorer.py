"""Оценка сложности продвижения запросов"""

from typing import Dict, List
import pandas as pd
import numpy as np


class DifficultyScorer:
    """Оценщик сложности продвижения запросов"""
    
    def __init__(self):
        """Инициализация оценщика"""
        pass
    
    def calculate_difficulty_score(
        self,
        frequency_world: int,
        frequency_exact: int,
        word_count: int,
        is_commercial: bool = False,
        has_brand: bool = False
    ) -> float:
        """
        Вычисляет скор сложности (0-100)
        
        Args:
            frequency_world: Общая частотность
            frequency_exact: Точная частотность
            word_count: Количество слов
            is_commercial: Коммерческий ли запрос
            has_brand: Есть ли бренд
            
        Returns:
            Скор сложности (0=легко, 100=очень сложно)
        """
        score = 0.0
        
        # Фактор частотности (чем выше, тем сложнее)
        if frequency_world > 10000:
            score += 40
        elif frequency_world > 5000:
            score += 30
        elif frequency_world > 1000:
            score += 20
        elif frequency_world > 100:
            score += 10
        else:
            score += 5
        
        # Фактор конкуренции (точная частотность)
        if frequency_exact > 0:
            competition_ratio = frequency_exact / max(frequency_world, 1)
            score += competition_ratio * 20  # До 20 баллов
        
        # Фактор длины запроса (короткие сложнее)
        if word_count == 1:
            score += 20
        elif word_count == 2:
            score += 10
        elif word_count >= 5:
            score -= 10  # Длинные проще
        
        # Коммерческие сложнее
        if is_commercial:
            score += 15
        
        # Брендовые могут быть сложнее (зависит от бренда)
        if has_brand:
            score += 10
        
        # Нормализуем в диапазон 0-100
        score = max(0, min(100, score))
        
        return score
    
    def classify_difficulty(self, score: float) -> str:
        """
        Классифицирует сложность
        
        Args:
            score: Скор сложности
            
        Returns:
            Категория сложности
        """
        if score >= 75:
            return "very_hard"
        elif score >= 60:
            return "hard"
        elif score >= 40:
            return "medium"
        elif score >= 20:
            return "easy"
        else:
            return "very_easy"
    
    def classify_by_strategy(
        self,
        frequency_world: int,
        frequency_exact: int,
        is_commercial: bool = False
    ) -> str:
        """
        Классифицирует по стратегии продвижения
        
        Args:
            frequency_world: Общая частотность
            frequency_exact: Точная частотность
            is_commercial: Коммерческий ли
            
        Returns:
            Тип стратегии
        """
        # Quick Wins: низкая конкуренция + средняя частота
        if 100 <= frequency_world <= 1000 and frequency_exact < 50:
            return "quick_wins"
        
        # Long Tail: низкая конкуренция + низкая частота
        if frequency_world < 100:
            return "long_tail"
        
        # Competitive: высокая конкуренция + высокая частота
        if frequency_world > 5000 and frequency_exact > 500:
            return "competitive"
        
        # Informational Easy: информационные с низкой конкуренцией
        if not is_commercial and frequency_exact < 100:
            return "informational_easy"
        
        # Medium: все остальное
        return "medium"
    
    def score_batch(
        self,
        df: pd.DataFrame,
        freq_world_col: str = 'frequency_world',
        freq_exact_col: str = 'frequency_exact',
        word_count_col: str = 'words_count',
        commercial_col: str = 'is_commercial',
        brand_col: str = 'is_brand_query'
    ) -> pd.DataFrame:
        """
        Оценивает сложность для всего DataFrame
        
        Args:
            df: DataFrame с запросами
            freq_world_col: Колонка с общей частотностью
            freq_exact_col: Колонка с точной частотностью
            word_count_col: Колонка с количеством слов
            commercial_col: Колонка с флагом коммерческого
            brand_col: Колонка с флагом бренда
            
        Returns:
            DataFrame с добавленными колонками
        """
        print("🔄 Оценка сложности продвижения...")
        
        # Вычисляем скоры
        df['difficulty_score'] = df.apply(
            lambda row: self.calculate_difficulty_score(
                row.get(freq_world_col, 0),
                row.get(freq_exact_col, 0),
                row.get(word_count_col, 1),
                row.get(commercial_col, False),
                row.get(brand_col, False)
            ),
            axis=1
        )
        
        # Классифицируем по сложности
        df['difficulty_level'] = df['difficulty_score'].apply(self.classify_difficulty)
        
        # Классифицируем по стратегии
        df['difficulty_cluster'] = df.apply(
            lambda row: self.classify_by_strategy(
                row.get(freq_world_col, 0),
                row.get(freq_exact_col, 0),
                row.get(commercial_col, False)
            ),
            axis=1
        )
        
        print("✓ Оценка сложности завершена")
        return df
    
    def get_difficulty_distribution(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Возвращает распределение по уровням сложности
        
        Args:
            df: DataFrame с оценками
            
        Returns:
            Словарь с распределением
        """
        if 'difficulty_level' not in df.columns:
            return {}
        
        return df['difficulty_level'].value_counts().to_dict()
    
    def get_strategy_distribution(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Возвращает распределение по стратегиям
        
        Args:
            df: DataFrame с оценками
            
        Returns:
            Словарь с распределением
        """
        if 'difficulty_cluster' not in df.columns:
            return {}
        
        return df['difficulty_cluster'].value_counts().to_dict()
    
    def get_top_opportunities(
        self,
        df: pd.DataFrame,
        strategy: str = 'quick_wins',
        top_n: int = 50
    ) -> pd.DataFrame:
        """
        Возвращает топ возможностей по стратегии
        
        Args:
            df: DataFrame с оценками
            strategy: Стратегия ('quick_wins', 'long_tail', etc.)
            top_n: Количество запросов
            
        Returns:
            DataFrame с топ запросами
        """
        if 'difficulty_cluster' not in df.columns:
            return pd.DataFrame()
        
        filtered = df[df['difficulty_cluster'] == strategy]
        
        # Сортируем по частотности (для quick_wins) или сложности
        if 'frequency_world' in filtered.columns:
            sorted_df = filtered.sort_values('frequency_world', ascending=False)
        else:
            sorted_df = filtered.sort_values('difficulty_score', ascending=True)
        
        return sorted_df.head(top_n)






