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
    create_intent_filtered_sheet
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
            
            # Листы кластеров: каждый кластер на отдельном листе
            # Классификация по коммерческим факторам из SERP (домены + offer)
            # Если сумма факторов >= 12, кластер считается коммерческим
            if 'semantic_cluster_id' in df.columns:
                print("  📄 Создание листов кластеров (каждый кластер на отдельном листе)...")
                from .writers.cluster_sheets_writer import create_cluster_sheets
                create_cluster_sheets(df, writer, self.formats, commercial_threshold=12)
            
            # ОТКЛЮЧЕНО: Лист 2: Топ запросы по priority_score
            # if 'priority_score' in df.columns:
            #     print("  📄 Создание листа 'Топ приоритетных'...")
            #     create_top_priority_sheet(df, writer, self.formats)
            
            # ОТКЛЮЧЕНО: Лист 3: Сводка по кластерам
            # if 'semantic_cluster_id' in df.columns:
            #     print("  📄 Создание листа 'Сводка по кластерам'...")
            #     create_clusters_summary_sheet(df, writer, self.formats)
            
            # ОТКЛЮЧЕНО: Лист 4: Сводка по интентам
            # if 'main_intent' in df.columns:
            #     print("  📄 Создание листа 'Сводка по интентам'...")
            #     create_intent_summary_sheet(df, writer, self.formats)
            
            # ОТКЛЮЧЕНО: Лист 5: LSI фразы (если есть)
            # if 'cluster_lsi_phrases' in df.columns:
            #     print("  📄 Создание листа 'LSI фразы'...")
            #     create_lsi_sheet(df, writer, self.formats)
            
            # ОТКЛЮЧЕНО: Лист 6: Иерархия проекта
            # print("  📄 Создание листа 'Иерархия проекта'...")
            # create_hierarchy_sheet(writer, self.formats, self.hierarchy_df)
            
            # Лист 4: FAQ - справка по столбцам
            print("  📄 Создание листа 'FAQ'...")
            create_faq_sheet(writer, self.formats)
        
        print(f"✓ Excel файл создан: {output_path}")

