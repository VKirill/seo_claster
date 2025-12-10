"""
Контроллер экспорта результатов
"""

from pathlib import Path
from typing import Dict, Any

from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
from seo_analyzer.export.excel_exporter import ExcelExporter
from seo_analyzer.export.csv_exporter import CSVExporter
from seo_analyzer.export.json_exporter import JSONExporter
from seo_analyzer.export.html_visualizer import HTMLVisualizer


class ExportController:
    """Контроллер экспорта результатов"""
    
    def __init__(self):
        self.master_db = MasterQueryDatabase()
    
    def export_group(
        self,
        group_name: str,
        format_name: str,
        output_path: Path,
        options: Dict[str, Any] = None
    ) -> bool:
        """
        Экспортировать группу в указанный формат
        
        Args:
            group_name: Название группы
            format_name: Формат экспорта (excel, csv, json, html)
            output_path: Путь для сохранения
            options: Дополнительные опции
            
        Returns:
            True если успешно
        """
        if options is None:
            options = {}
        
        try:
            # Загружаем данные
            df = self.master_db.load_queries(group_name, include_serp_urls=False)
            if df is None or df.empty:
                return False
            
            # Выбираем экспортер
            if format_name == 'excel':
                exporter = ExcelExporter()
                exporter.export_to_excel(
                    df,
                    output_path,
                    include_charts=options.get('include_charts', False),
                    group_by_clusters=True
                )
            elif format_name == 'csv':
                exporter = CSVExporter()
                exporter.export_full_results(df, output_path)
            elif format_name == 'json':
                exporter = JSONExporter()
                exporter.export_hierarchical(df, output_path)
            elif format_name == 'html':
                visualizer = HTMLVisualizer()
                visualizer.generate_dashboard(df, output_path)
            else:
                return False
            
            return True
            
        except Exception as e:
            import traceback
            print(f"Ошибка экспорта: {e}")
            print(traceback.format_exc())
            return False

