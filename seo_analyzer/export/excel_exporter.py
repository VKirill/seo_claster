"""
Excel Exporter Module (фасад для обратной совместимости)
Продвинутый экспорт в Excel с форматированием, группировкой, фильтрами
"""

from pathlib import Path
from typing import Dict
import pandas as pd

from .excel import ExcelExporter as ExcelExporterImpl


class ExcelExporter:
    """
    Экспорт в Excel с профессиональным форматированием
    
    Устаревший класс для обратной совместимости.
    Использует модульную структуру из seo_analyzer.export.excel
    """
    
    def __init__(self):
        self._exporter = ExcelExporterImpl()
        self.workbook = None
        self.formats = {}
    
    def set_hierarchy_data(self, hierarchy_df: pd.DataFrame):
        """
        Установить данные иерархии для экспорта
        
        Args:
            hierarchy_df: DataFrame с иерархией
        """
        self._exporter.set_hierarchy_data(hierarchy_df)
    
    def export_to_excel(
        self,
        df: pd.DataFrame,
        output_path: Path,
        include_charts: bool = True,
        group_by_clusters: bool = True
    ):
        """
        Создать Excel файл с форматированием
        
        Args:
            df: DataFrame с данными
            output_path: Путь для сохранения
            include_charts: Добавить графики
            group_by_clusters: Группировать по кластерам
        """
        self._exporter.export_to_excel(df, output_path, include_charts, group_by_clusters)

