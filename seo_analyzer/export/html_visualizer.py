"""HTML визуализация результатов (фасад для обратной совместимости)"""

from pathlib import Path
from typing import Dict
import pandas as pd

from .html import build_dashboard


class HTMLVisualizer:
    """
    Генератор HTML дашборда с результатами
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.export.html
    """
    
    def __init__(self):
        """Инициализация"""
        pass
    
    def generate_dashboard(
        self,
        df: pd.DataFrame,
        output_path: Path,
        stats: Dict = None
    ) -> bool:
        """
        Генерирует HTML дашборд
        
        Args:
            df: DataFrame с результатами
            output_path: Путь для сохранения
            stats: Статистика анализа
            
        Returns:
            True если успешно
        """
        return build_dashboard(df, output_path, stats)
