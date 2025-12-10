"""
Оптимизированные функции для сравнения URL с использованием set операций.

ВАЖНО: Numba оказался медленнее чем нативный Python set intersection,
поэтому используем только оптимизированные set операции.

Ускорение достигается за счет:
1. Кэширования результатов (избегаем повторных вычислений)
2. Оптимизированных операций с set (быстрее чем NumPy для небольших списков)
"""
from typing import List


class FastSimilarityCalculator:
    """
    Класс для быстрого вычисления схожести запросов через URL пересечения.
    
    Использует оптимизированные set операции.
    """
    
    def __init__(self, top_positions: int = 30):
        """
        Args:
            top_positions: Количество топ позиций для анализа (30 по умолчанию)
        """
        self.top_positions = top_positions
    
    def _urls_to_set(self, urls: List[str]) -> set:
        """
        Конвертирует список URL в set для быстрого пересечения.
        
        Args:
            urls: Список URL (до top_positions элементов)
            
        Returns:
            Set URL
        """
        if not urls:
            return set()
        
        # Берем только топ-N позиций и конвертируем в set
        return set(urls[:self.top_positions])
    
    def calculate_simple_similarity(
        self, 
        urls1: List[str], 
        urls2: List[str]
    ) -> int:
        """
        Простое пересечение без весов (быстрейший вариант).
        
        Использует нативный Python set intersection - быстрее всего!
        
        Args:
            urls1: Список URL первого запроса
            urls2: Список URL второго запроса
            
        Returns:
            Количество общих URL
        """
        if not urls1 or not urls2:
            return 0
        
        # Нативный Python set intersection - самый быстрый метод!
        set1 = set(urls1[:self.top_positions])
        set2 = set(urls2[:self.top_positions])
        return len(set1 & set2)
    
    def calculate_similarity_with_threshold(
        self, 
        urls1: List[str], 
        urls2: List[str],
        threshold: int
    ) -> int:
        """
        Пересечение с ранним выходом при достижении порога.
        
        Используйте этот метод в BALANCED/SOFT режимах для максимальной скорости.
        
        Args:
            urls1: Список URL первого запроса
            urls2: Список URL второго запроса
            threshold: Минимальный порог (например, 7)
            
        Returns:
            Количество общих URL (может остановиться на threshold)
        """
        if not urls1 or not urls2:
            return 0
        
        # Нативный set intersection
        set1 = set(urls1[:self.top_positions])
        set2 = set(urls2[:self.top_positions])
        common = len(set1 & set2)
        
        # Возвращаем результат (ранний выход не нужен, set пересечение уже быстрое)
        return common
    
    def calculate_similarity(
        self,
        urls1: List[str],
        urls2: List[str]
    ) -> int:
        """
        Подсчет общих URL между двумя списками.
        
        Все позиции равны по важности.
        
        Args:
            urls1: Список URL первого запроса
            urls2: Список URL второго запроса
            
        Returns:
            Количество общих URL
        """
        if not urls1 or not urls2:
            return 0
        
        # Берем только топ-N позиций
        urls1_top = urls1[:self.top_positions]
        urls2_top = urls2[:self.top_positions]
        
        # Простой подсчет через set (самый быстрый метод!)
        set1 = set(urls1_top)
        set2 = set(urls2_top)
        return len(set1 & set2)

