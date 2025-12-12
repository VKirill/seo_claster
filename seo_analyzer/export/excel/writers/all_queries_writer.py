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
    # Проверяем наличие колонки frequency_world перед сортировкой
    if group_by_clusters and 'semantic_cluster_id' in df.columns:
        if 'frequency_world' in df.columns:
            # Заменяем NaN на 0 для корректной сортировки
            df_sorted = df.copy()
            df_sorted['frequency_world'] = df_sorted['frequency_world'].fillna(0)
            df_sorted = df_sorted.sort_values(['semantic_cluster_id', 'frequency_world'], ascending=[True, False])
        else:
            df_sorted = df.sort_values('semantic_cluster_id', ascending=True)
    else:
        if 'frequency_world' in df.columns:
            # Заменяем NaN на 0 для корректной сортировки
            df_sorted = df.copy()
            df_sorted['frequency_world'] = df_sorted['frequency_world'].fillna(0)
            df_sorted = df_sorted.sort_values('frequency_world', ascending=False)
        else:
            df_sorted = df.copy()
    
    # Диагностика частот ДО выбора колонок
    if 'frequency_world' in df_sorted.columns:
        non_zero_before = (df_sorted['frequency_world'] > 0).sum()
        print(f"  🔍 ДИАГНОСТИКА: В df_sorted до select_columns: {non_zero_before} из {len(df_sorted)} с ненулевой частотой")
    
    # Выбираем колонки для экспорта
    columns_to_export = select_columns_for_export(df_sorted)
    
    # Проверяем, включены ли колонки частот в экспорт
    if 'frequency_world' not in columns_to_export and 'frequency_world' in df_sorted.columns:
        print(f"  ⚠️  ВНИМАНИЕ: frequency_world не включена в columns_to_export!")
        print(f"  ℹ️  Доступные колонки в df_sorted: {list(df_sorted.columns)}")
        print(f"  ℹ️  Выбранные колонки для экспорта: {columns_to_export}")
    
    if 'frequency_exact' not in columns_to_export and 'frequency_exact' in df_sorted.columns:
        print(f"  ⚠️  ВНИМАНИЕ: frequency_exact не включена в columns_to_export!")
    
    df_export = df_sorted[columns_to_export].copy()
    
    # Диагностика частот перед экспортом
    if 'frequency_world' in df_export.columns:
        non_zero_freq_world = (df_export['frequency_world'] > 0).sum()
        total_rows = len(df_export)
        print(f"  ℹ️  Частота (мир): {non_zero_freq_world} из {total_rows} запросов с ненулевой частотой")
        if non_zero_freq_world == 0 and total_rows > 0:
            print(f"  ⚠️  ВНИМАНИЕ: Все частоты равны нулю! Проверьте данные в БД.")
            # Проверяем исходные данные
            if 'frequency_world' in df_sorted.columns:
                original_non_zero = (df_sorted['frequency_world'] > 0).sum()
                print(f"  ℹ️  В исходном DataFrame: {original_non_zero} из {len(df_sorted)} с ненулевой частотой")
                # Дополнительная диагностика
                print(f"  ℹ️  Тип данных frequency_world в df_sorted: {df_sorted['frequency_world'].dtype}")
                print(f"  ℹ️  Тип данных frequency_world в df_export: {df_export['frequency_world'].dtype}")
                print(f"  ℹ️  Примеры значений в df_sorted: {df_sorted['frequency_world'].head(10).tolist()}")
                print(f"  ℹ️  Примеры значений в df_export: {df_export['frequency_world'].head(10).tolist()}")
    
    if 'frequency_exact' in df_export.columns:
        non_zero_freq_exact = (df_export['frequency_exact'] > 0).sum()
        total_rows = len(df_export)
        print(f"  ℹ️  Частота (точная): {non_zero_freq_exact} из {total_rows} запросов с ненулевой частотой")
    
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

