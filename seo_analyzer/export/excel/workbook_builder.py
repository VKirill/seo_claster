"""Создание Excel книги и основная логика экспорта"""

from pathlib import Path
from typing import Dict
import pandas as pd

from .sheet_formatter import create_formats
from .data_writer import (
    create_all_queries_sheet,
    create_top_priority_sheet,
    create_clusters_summary_sheet,
    create_intent_summary_sheet,
    create_lsi_sheet,
    create_intent_filtered_sheet,
    create_mixed_intent_sheet
)
from .faq_generator import create_faq_sheet
# ОТКЛЮЧЕНО: from .hierarchy_sheet import create_hierarchy_sheet


class ExcelExporter:
    """Экспорт в Excel с профессиональным форматированием"""
    
    def __init__(self):
        self.workbook = None
        self.formats = {}
        self.hierarchy_df = None
    
    def set_hierarchy_data(self, hierarchy_df: pd.DataFrame):
        """
        Установить данные иерархии для экспорта
        
        Args:
            hierarchy_df: DataFrame с иерархией
        """
        self.hierarchy_df = hierarchy_df
    
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
        print(f"📊 Создание Excel файла: {output_path.name}")
        
        # Создаем writer
        with pd.ExcelWriter(output_path, engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
            self.workbook = writer.book
            
            # Создаем форматы
            self.formats = create_formats(self.workbook)
            
            # Лист 1: Все запросы
            print("  📄 Создание листа 'Все запросы'...")
            create_all_queries_sheet(df, writer, self.formats, group_by_clusters)
            
            # Лист 2: Коммерческие кластеры (>70% коммерческих запросов)
            if 'main_intent' in df.columns and 'semantic_cluster_id' in df.columns:
                print("  📄 Создание листа 'Коммерческие' (>70% коммерческих запросов)...")
                create_intent_filtered_sheet(df, writer, self.formats, 'commercial', group_by_clusters)
            
            # Лист 3: Информационные кластеры (>70% информационных запросов)
            if 'main_intent' in df.columns and 'semantic_cluster_id' in df.columns:
                print("  📄 Создание листа 'Информационные' (>70% информационных запросов)...")
                create_intent_filtered_sheet(df, writer, self.formats, 'informational', group_by_clusters)
            
            # Лист 4: Смешанные кластеры (30-70% коммерческих запросов)
            if 'main_intent' in df.columns and 'semantic_cluster_id' in df.columns:
                print("  📄 Создание листа 'Смешанные' (30-70% коммерческих запросов)...")
                create_mixed_intent_sheet(df, writer, self.formats, group_by_clusters)
            
            # Лист 5: FAQ - справка по столбцам
            print("  📄 Создание листа 'FAQ'...")
            create_faq_sheet(writer, self.formats)
        
        print(f"✓ Excel файл создан: {output_path}")

