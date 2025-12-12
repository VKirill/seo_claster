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
    
    Фильтрует кластеры где >70% запросов имеют указанный интент:
    - Коммерческие: >70% коммерческих запросов
    - Информационные: >70% информационных запросов
    
    Args:
        df: DataFrame с данными
        writer: ExcelWriter объект
        formats: Словарь с форматами
        intent_type: Тип интента ('commercial' или 'informational')
        group_by_clusters: Группировать по кластерам
    """
    # Названия листов
    sheet_names = {
        'commercial': 'Коммерческие',
        'informational': 'Информационные'
    }
    
    sheet_name = sheet_names.get(intent_type, intent_type.capitalize())
    
    # Проверяем наличие нужных колонок
    if 'semantic_cluster_id' not in df.columns or 'main_intent' not in df.columns:
        print(f"ℹ️  Пропускаем лист '{sheet_name}' - нет колонок semantic_cluster_id или main_intent")
        return
    
    # Определяем какие интенты считаются коммерческими
    commercial_intents = ['commercial', 'commercial_geo', 'transactional']
    
    # Порог для классификации кластера (70%)
    threshold = 0.7
    
    # Фильтруем кластеры по порогу 70%
    filtered_cluster_ids = []
    for cluster_id, cluster_df in df.groupby('semantic_cluster_id'):
        total_count = len(cluster_df)
        if total_count == 0:
            continue
        
        if intent_type == 'commercial':
            # Подсчитываем коммерческие запросы
            commercial_count = cluster_df[cluster_df['main_intent'].isin(commercial_intents)].shape[0]
            commercial_ratio = commercial_count / total_count
            
            # Кластер считается коммерческим, если >70% запросов коммерческие
            if commercial_ratio > threshold:
                filtered_cluster_ids.append(cluster_id)
        elif intent_type == 'informational':
            # Подсчитываем информационные запросы
            informational_count = cluster_df[cluster_df['main_intent'] == 'informational'].shape[0]
            informational_ratio = informational_count / total_count
            
            # Кластер считается информационным, если >70% запросов информационные
            if informational_ratio > threshold:
                filtered_cluster_ids.append(cluster_id)
        else:
            # Для других типов проверяем точное совпадение с порогом
            matching_count = cluster_df[cluster_df['main_intent'] == intent_type].shape[0]
            matching_ratio = matching_count / total_count
            if matching_ratio > threshold:
                filtered_cluster_ids.append(cluster_id)
    
    if not filtered_cluster_ids:
        print(f"ℹ️  Пропускаем лист '{sheet_name}' - нет кластеров с >70% {intent_type} запросов")
        return
    
    # Фильтруем DataFrame
    df_filtered = df[df['semantic_cluster_id'].isin(filtered_cluster_ids)].copy()
    
    if df_filtered.empty:
        print(f"ℹ️  Пропускаем лист '{sheet_name}' - нет данных после фильтрации")
        return
    
    # Сортируем
    if group_by_clusters and 'semantic_cluster_id' in df_filtered.columns:
        # Проверяем наличие frequency_world перед сортировкой
        if 'frequency_world' in df_filtered.columns:
            df_filtered['frequency_world'] = df_filtered['frequency_world'].fillna(0)
            df_filtered = df_filtered.sort_values(['semantic_cluster_id', 'frequency_world'], ascending=[True, False])
        else:
            df_filtered = df_filtered.sort_values('semantic_cluster_id', ascending=True)
    else:
        if 'frequency_world' in df_filtered.columns:
            df_filtered['frequency_world'] = df_filtered['frequency_world'].fillna(0)
            df_filtered = df_filtered.sort_values('frequency_world', ascending=False)
    
    # Выбираем колонки для экспорта
    columns_to_export = select_columns_for_export(df_filtered)
    df_export = df_filtered[columns_to_export].copy()
    
    # Диагностика частот перед экспортом
    if 'frequency_world' in df_export.columns:
        non_zero_freq_world = (df_export['frequency_world'] > 0).sum()
        total_rows = len(df_export)
        print(f"    ℹ️  Частота (мир): {non_zero_freq_world} из {total_rows} запросов с ненулевой частотой")
        if non_zero_freq_world == 0 and total_rows > 0:
            print(f"    ⚠️  ВНИМАНИЕ: Все частоты равны нулю! Проверьте данные в БД.")
    
    if 'frequency_exact' in df_export.columns:
        non_zero_freq_exact = (df_export['frequency_exact'] > 0).sum()
        total_rows = len(df_export)
        print(f"    ℹ️  Частота (точная): {non_zero_freq_exact} из {total_rows} запросов с ненулевой частотой")
    
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
    
    # Статистика
    clusters_count = df_filtered['semantic_cluster_id'].nunique() if 'semantic_cluster_id' in df_filtered.columns else 0
    print(f"  ✓ Создан лист '{sheet_name}': {clusters_count} кластеров, {len(df_export)} запросов")


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


def create_mixed_intent_sheet_impl(
    df: pd.DataFrame,
    writer: pd.ExcelWriter,
    formats: dict,
    group_by_clusters: bool = True,
    min_mixed_ratio: float = 0.3,
    max_mixed_ratio: float = 0.7
):
    """
    Создает лист со смешанными кластерами (где есть и коммерческие, и информационные запросы)
    
    Кластер считается смешанным, если соотношение коммерческих запросов
    находится в диапазоне [min_mixed_ratio, max_mixed_ratio] (от 30% до 70%)
    
    Логика классификации:
    - >70% коммерческих → коммерческий кластер
    - >70% информационных → информационный кластер
    - 30-70% коммерческих → смешанный кластер
    
    Args:
        df: DataFrame с данными
        writer: ExcelWriter объект
        formats: Словарь с форматами
        group_by_clusters: Группировать по кластерам
        min_mixed_ratio: Минимальное соотношение коммерческих (по умолчанию 0.3 = 30%)
        max_mixed_ratio: Максимальное соотношение коммерческих (по умолчанию 0.7 = 70%)
    """
    sheet_name = 'Смешанные'
    
    # Проверяем наличие нужных колонок
    if 'semantic_cluster_id' not in df.columns or 'main_intent' not in df.columns:
        print(f"ℹ️  Пропускаем лист '{sheet_name}' - нет колонок semantic_cluster_id или main_intent")
        return
    
    # Определяем какие интенты считаются коммерческими
    commercial_intents = ['commercial', 'commercial_geo', 'transactional']
    
    # Порог для классификации (70%)
    threshold = 0.7
    
    # Находим смешанные кластеры
    # Смешанные = не коммерческие (>70%) и не информационные (>70%)
    mixed_cluster_ids = []
    for cluster_id, cluster_df in df.groupby('semantic_cluster_id'):
        total_count = len(cluster_df)
        if total_count == 0:
            continue
        
        # Подсчитываем коммерческие и информационные запросы
        commercial_count = cluster_df[cluster_df['main_intent'].isin(commercial_intents)].shape[0]
        informational_count = cluster_df[cluster_df['main_intent'] == 'informational'].shape[0]
        
        # Пропускаем кластеры где нет смешения (только один тип интента)
        if commercial_count == 0 or informational_count == 0:
            continue
        
        # Вычисляем соотношение коммерческих запросов
        commercial_ratio = commercial_count / total_count
        informational_ratio = informational_count / total_count
        
        # Кластер считается смешанным, если:
        # - коммерческих запросов <= 70% И
        # - информационных запросов <= 70% И
        # - соотношение коммерческих в диапазоне [min_mixed_ratio, max_mixed_ratio]
        if commercial_ratio <= threshold and informational_ratio <= threshold:
            if min_mixed_ratio <= commercial_ratio <= max_mixed_ratio:
                mixed_cluster_ids.append(cluster_id)
    
    if not mixed_cluster_ids:
        print(f"ℹ️  Пропускаем лист '{sheet_name}' - нет смешанных кластеров")
        return
    
    # Фильтруем DataFrame - только смешанные кластеры
    df_filtered = df[df['semantic_cluster_id'].isin(mixed_cluster_ids)].copy()
    
    if df_filtered.empty:
        print(f"ℹ️  Пропускаем лист '{sheet_name}' - нет данных после фильтрации")
        return
    
    # Сортируем
    if group_by_clusters and 'semantic_cluster_id' in df_filtered.columns:
        # Проверяем наличие frequency_world перед сортировкой
        if 'frequency_world' in df_filtered.columns:
            df_filtered['frequency_world'] = df_filtered['frequency_world'].fillna(0)
            df_filtered = df_filtered.sort_values(['semantic_cluster_id', 'frequency_world'], ascending=[True, False])
        else:
            df_filtered = df_filtered.sort_values('semantic_cluster_id', ascending=True)
    else:
        if 'frequency_world' in df_filtered.columns:
            df_filtered['frequency_world'] = df_filtered['frequency_world'].fillna(0)
            df_filtered = df_filtered.sort_values('frequency_world', ascending=False)
    
    # Выбираем колонки для экспорта
    columns_to_export = select_columns_for_export(df_filtered)
    df_export = df_filtered[columns_to_export].copy()
    
    # Диагностика частот перед экспортом
    if 'frequency_world' in df_export.columns:
        non_zero_freq_world = (df_export['frequency_world'] > 0).sum()
        total_rows = len(df_export)
        print(f"    ℹ️  Частота (мир): {non_zero_freq_world} из {total_rows} запросов с ненулевой частотой")
        if non_zero_freq_world == 0 and total_rows > 0:
            print(f"    ⚠️  ВНИМАНИЕ: Все частоты равны нулю! Проверьте данные в БД.")
    
    if 'frequency_exact' in df_export.columns:
        non_zero_freq_exact = (df_export['frequency_exact'] > 0).sum()
        total_rows = len(df_export)
        print(f"    ℹ️  Частота (точная): {non_zero_freq_exact} из {total_rows} запросов с ненулевой частотой")
    
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
    
    # Статистика
    clusters_count = len(mixed_cluster_ids)
    print(f"  ✓ Создан лист '{sheet_name}': {clusters_count} смешанных кластеров, {len(df_export)} запросов")

