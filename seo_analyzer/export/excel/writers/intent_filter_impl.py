"""
Базовая реализация создания листов с фильтрацией по интентам
"""

import pandas as pd

from ..utils.column_selector import select_columns_for_export
from ..utils.column_translator import get_column_translation
from ..sheet_formatter import set_column_widths, add_conditional_formatting, apply_number_formats
from .lsi_converter import convert_query_lsi_phrases, convert_cluster_lsi_phrases


def create_intent_filtered_sheet_impl(
    df: pd.DataFrame,
    writer: pd.ExcelWriter,
    formats: dict,
    intent_type: str,
    group_by_clusters: bool = True
):
    """
    Базовая реализация создания листа с фильтрацией по интенту
    
    Args:
        df: DataFrame с данными
        writer: ExcelWriter объект
        formats: Словарь с форматами
        intent_type: Тип интента ('commercial' или 'informational')
        group_by_clusters: Группировать по кластерам
    """
    # Названия листов
    sheet_names = {
        'commercial': 'Коммерческие кластеры',
        'informational': 'Информационные кластеры'
    }
    
    sheet_name = sheet_names.get(intent_type, f'{intent_type.capitalize()} кластеры')
    
    # Проверяем наличие нужных колонок
    if 'semantic_cluster_id' not in df.columns or 'main_intent' not in df.columns:
        print(f"ℹ️  Пропускаем лист '{sheet_name}' - нет колонок semantic_cluster_id или main_intent")
        return
    
    # Фильтруем полностью чистые кластеры по интенту
    pure_intent_cluster_ids = []
    for cluster_id, cluster_df in df.groupby('semantic_cluster_id'):
        # Все запросы кластера должны иметь одинаковый интент
        intents = cluster_df['main_intent'].unique()
        if len(intents) == 1 and intents[0] == intent_type:
            pure_intent_cluster_ids.append(cluster_id)
    
    if not pure_intent_cluster_ids:
        print(f"ℹ️  Пропускаем лист '{sheet_name}' - нет полностью {intent_type} кластеров")
        return
    
    # Фильтруем DataFrame
    df_filtered = df[df['semantic_cluster_id'].isin(pure_intent_cluster_ids)].copy()
    
    # Сортируем
    if group_by_clusters:
        df_filtered = df_filtered.sort_values(['semantic_cluster_id', 'frequency_world'], ascending=[True, False])
    else:
        df_filtered = df_filtered.sort_values('frequency_world', ascending=False)
    
    # Выбираем колонки для экспорта
    columns_to_export = select_columns_for_export(df_filtered)
    df_export = df_filtered[columns_to_export].copy()
    
    # Конвертируем LSI фразы
    if 'lsi_phrases' in df_export.columns:
        df_export['lsi_phrases'] = df_export['lsi_phrases'].apply(convert_query_lsi_phrases)
    
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
    worksheet.freeze_panes(1, 0)
    worksheet.autofilter(0, 0, len(df_export), len(df_export.columns) - 1)
    
    # Форматирование
    set_column_widths(worksheet, df_export.columns)
    apply_number_formats(worksheet, df_export, formats)
    add_conditional_formatting(worksheet, df_export, sheet_name)
    
    print(f"  ✓ Создан лист '{sheet_name}': {len(pure_intent_cluster_ids)} кластеров, {len(df_export)} запросов")


def create_intent_summary_sheet_impl(df: pd.DataFrame, writer: pd.ExcelWriter, formats: dict):
    """
    Базовая реализация создания сводки по интентам
    
    Args:
        df: DataFrame с данными
        writer: ExcelWriter объект
        formats: Словарь с форматами
    """
    sheet_name = 'Сводка по интентам'
    
    # Проверяем наличие нужной колонки
    if 'main_intent' not in df.columns:
        print(f"ℹ️  Пропускаем лист '{sheet_name}' - нет колонки main_intent")
        return
    
    # Группируем по интентам
    intent_stats = df.groupby('main_intent').agg({
        'keyword': 'count',
        'frequency_world': 'sum'
    }).reset_index()
    
    intent_stats.columns = ['Интент', 'Количество запросов', 'Суммарная частота']
    intent_stats = intent_stats.sort_values('Суммарная частота', ascending=False)
    
    # Записываем
    intent_stats.to_excel(writer, sheet_name=sheet_name, index=False)
    
    worksheet = writer.sheets[sheet_name]
    worksheet.freeze_panes(1, 0)
    
    # Форматирование
    worksheet.set_column('A:A', 20, formats.get('text', None))
    worksheet.set_column('B:B', 18, formats.get('number', None))
    worksheet.set_column('C:C', 18, formats.get('number', None))
    
    print(f"  ✓ Создан лист '{sheet_name}': {len(intent_stats)} интентов")

