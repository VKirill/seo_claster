"""
Создание листа со всеми запросами
"""

import pandas as pd

from ..utils.column_selector import select_columns_for_export
from ..utils.column_translator import get_column_translation
from ..sheet_formatter import set_column_widths, add_conditional_formatting, apply_number_formats
from .lsi_converter import convert_query_lsi_phrases, convert_cluster_lsi_phrases


def create_all_queries_sheet(
    df: pd.DataFrame,
    writer: pd.ExcelWriter,
    formats: dict,
    group_by_clusters: bool
):
    """
    Создать лист со всеми запросами
    
    Args:
        df: DataFrame с данными
        writer: ExcelWriter объект
        formats: Словарь с форматами
        group_by_clusters: Группировать по кластерам
    """
    sheet_name = 'Все запросы'
    
    # Сортируем
    if group_by_clusters and 'semantic_cluster_id' in df.columns:
        df_sorted = df.sort_values(['semantic_cluster_id', 'frequency_world'], ascending=[True, False])
    else:
        df_sorted = df.sort_values('frequency_world', ascending=False)
    
    # Выбираем колонки для экспорта
    columns_to_export = select_columns_for_export(df_sorted)
    df_export = df_sorted[columns_to_export].copy()
    
    # Конвертируем списки в строки для Excel (только для LSI фраз)
    if 'lsi_phrases' in df_export.columns:
        df_export['lsi_phrases'] = df_export['lsi_phrases'].apply(convert_query_lsi_phrases)
    
    # Конвертируем cluster_lsi_phrases если есть (список словарей -> строка)
    if 'cluster_lsi_phrases' in df_export.columns:
        df_export['cluster_lsi_phrases'] = df_export['cluster_lsi_phrases'].apply(convert_cluster_lsi_phrases)
    
    # Записываем в Excel
    df_export.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)
    
    worksheet = writer.sheets[sheet_name]
    
    # Записываем заголовки с переводом на русский
    for col_num, col_name in enumerate(df_export.columns):
        russian_name = get_column_translation(col_name)
        worksheet.write(0, col_num, russian_name, formats['header'])
    
    # Настройки листа
    worksheet.freeze_panes(1, 0)  # Заморозить первую строку
    
    # Автофильтр
    worksheet.autofilter(0, 0, len(df_export), len(df_export.columns) - 1)
    
    # Настройка ширины колонок
    set_column_widths(worksheet, df_export.columns)
    
    # Применяем форматирование чисел
    apply_number_formats(worksheet, df_export, formats)
    
    # Условное форматирование
    add_conditional_formatting(worksheet, df_export, sheet_name)

