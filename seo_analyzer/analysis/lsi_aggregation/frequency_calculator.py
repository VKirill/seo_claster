"""
Подсчет частоты LSI фраз
"""

from collections import Counter
from typing import List, Dict, Any


class FrequencyCalculator:
    """Калькулятор частоты фраз"""
    
    @staticmethod
    def calculate_frequency(phrases: List[Dict[str, Any]]) -> Counter:
        """
        Подсчитывает частоту появления фраз
        
        Args:
            phrases: Список фраз в формате словарей
            
        Returns:
            Counter с частотами фраз
        """
        phrase_counter = Counter()
        
        for item in phrases:
            if isinstance(item, dict):
                phrase = item.get('phrase', '')
                frequency = item.get('frequency', 1)
                if phrase:
                    phrase_counter[phrase] += frequency
            elif isinstance(item, str):
                if item.strip():
                    phrase_counter[item.strip()] += 1
        
        return phrase_counter

