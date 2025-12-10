"""
Экспорт кластеров по отдельным листам с классификацией по коммерческим факторам
"""

import pandas as pd
from typing import Dict, Tuple, Set
from pathlib import Path

from ...core.lemmatizer import lemmatize_phrase
from ..utils.column_selector import select_columns_for_export
from ..utils.column_translator import get_column_translation
from ..sheet_formatter import set_column_widths, add_conditional_formatting, apply_number_formats
from .lsi_converter import convert_query_lsi_phrases, convert_cluster_lsi_phrases


def calculate_cluster_commercial_factors(
    cluster_df: pd.DataFrame,
    commercial_domains_column: str = 'serp_commercial_domains',
    offers_column: str = 'serp_docs_with_offers'
) -> Tuple[int, int, int]:
    """
    Рассчитывает коммерческие факторы для кластера.
    
    Коммерческие факторы = сумма (домены + offer) по всем запросам кластера.
    Суммируются значения из колонок serp_commercial_domains и serp_docs_with_offers (или serp_offers_count).
    
    Args:
        cluster_df: DataFrame с запросами одного кластера
        commercial_domains_column: Название колонки с коммерческими доменами
        offers_column: Название колонки с документами с offer_info
        
    Returns:
        Tuple (total_commercial_domains, total_offers, total_factors)
        - total_commercial_domains: Сумма коммерческих доменов по всем запросам
        - total_offers: Сумма документов с offer_info по всем запросам
        - total_factors: Общая сумма факторов (domains + offers)
    """
    total_domains = 0
    total_offers = 0
    
    # Суммируем коммерческие домены
    if commercial_domains_column in cluster_df.columns:
        total_domains = int(cluster_df[commercial_domains_column].fillna(0).sum())
    
    # Суммируем offers (проверяем обе возможные колонки)
    if offers_column in cluster_df.columns:
        total_offers = int(cluster_df[offers_column].fillna(0).sum())
    elif 'serp_offers_count' in cluster_df.columns:
        # Альтернативная колонка для offers
        total_offers = int(cluster_df['serp_offers_count'].fillna(0).sum())
    
    total_factors = total_domains + total_offers
    
    return (total_domains, total_offers, total_factors)


def _load_commercial_keywords(commercial_keywords_file: Path = None) -> Set[str]:
    """
    Загружает коммерческие ключевые слова из файла.
    
    Args:
        commercial_keywords_file: Путь к файлу с коммерческими ключевыми словами
        
    Returns:
        Множество ключевых слов (в нижнем регистре, лемматизированных)
    """
    keywords = set()
    
    # Путь относительно корня проекта
    if commercial_keywords_file is None:
        base_dir = Path(__file__).parent.parent.parent.parent.parent
        commercial_keywords_file = base_dir / 'keyword_group' / 'commercial.txt'
    
    if not commercial_keywords_file.exists():
        return keywords
    
    try:
        with open(commercial_keywords_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                
                # Пропускаем комментарии и пустые строки
                if not line or line.startswith('#'):
                    continue
                
                # Убираем комментарии в конце строки
                keyword = line.split('#')[0].strip()
                
                if keyword:
                    # Лемматизируем ключевое слово для сравнения в любом падеже
                    keyword_lemmatized = lemmatize_phrase(keyword.lower())
                    keywords.add(keyword_lemmatized)
    except Exception as e:
        print(f"⚠️ Ошибка при загрузке коммерческих ключевых слов: {e}")
    
    return keywords


def _has_commercial_keyword_in_cluster(
    cluster_df: pd.DataFrame,
    query_column: str = 'keyword',
    commercial_keywords: Set[str] = None
) -> bool:
    """
    Проверяет содержит ли хотя бы один запрос в кластере коммерческое ключевое слово.
    
    Args:
        cluster_df: DataFrame с запросами одного кластера
        query_column: Название колонки с запросами
        commercial_keywords: Множество коммерческих ключевых слов (лемматизированных)
        
    Returns:
        True если хотя бы один запрос содержит коммерческое слово
    """
    if commercial_keywords is None or len(commercial_keywords) == 0:
        return False
    
    if query_column not in cluster_df.columns:
        return False
    
    # Проверяем каждый запрос в кластере
    for query in cluster_df[query_column]:
        if pd.isna(query) or not query:
            continue
        
        # Лемматизируем запрос для сравнения
        query_lemmatized = lemmatize_phrase(str(query).lower())
        query_words = set(query_lemmatized.split())
        
        # Проверяем есть ли коммерческое слово в запросе
        if query_words & commercial_keywords:
            return True
    
    return False


def classify_cluster_by_factors(
    total_factors: int,
    threshold: int = 12
) -> str:
    """
    Классифицирует кластер на основе суммы коммерческих факторов из SERP.
    
    Если сумма факторов (serp_commercial_domains + serp_docs_with_offers) >= threshold,
    то кластер считается коммерческим.
    
    Args:
        total_factors: Сумма коммерческих факторов кластера (домены + offer)
        threshold: Порог для классификации (по умолчанию 12)
        
    Returns:
        'commercial' если total_factors >= threshold, иначе 'informational'
    """
    return 'commercial' if total_factors >= threshold else 'informational'


def create_cluster_sheets(
    df: pd.DataFrame,
    writer: pd.ExcelWriter,
    formats: dict,
    commercial_threshold: int = 10,
    cluster_column: str = 'semantic_cluster_id'
):
    """
    Создает отдельный лист для каждого кластера с классификацией по коммерческим факторам.
    
    Кластер считается коммерческим, если сумма коммерческих факторов (домены + offer)
    по всем запросам кластера > threshold.
    
    Args:
        df: DataFrame с данными
        writer: ExcelWriter объект
        formats: Словарь с форматами
        commercial_threshold: Порог для классификации кластера как коммерческого (по умолчанию 10)
        cluster_column: Название колонки с ID кластера
    """
    # Проверяем наличие нужных колонок
    if cluster_column not in df.columns:
        print(f"ℹ️  Пропускаем создание листов кластеров - нет колонки {cluster_column}")
        return
    
    # Проверяем наличие колонок с коммерческими факторами
    commercial_domains_col = 'serp_commercial_domains'
    offers_col = 'serp_docs_with_offers'
    
    has_domains = commercial_domains_col in df.columns
    has_offers = offers_col in df.columns or 'serp_offers_count' in df.columns
    
    if not has_domains and not has_offers:
        print(f"ℹ️  Пропускаем создание листов кластеров - нет колонок с коммерческими факторами")
        return
    
    # Группируем по кластерам
    cluster_groups = df.groupby(cluster_column)
    
    commercial_clusters = []
    informational_clusters = []
    
    # Классифицируем каждый кластер
    for cluster_id, cluster_df in cluster_groups:
        # Пропускаем пустые кластеры
        if len(cluster_df) == 0:
            continue
        
        # Рассчитываем коммерческие факторы (сумма по всем запросам кластера)
        total_domains, total_offers, total_factors = calculate_cluster_commercial_factors(
            cluster_df,
            commercial_domains_col,
            offers_col
        )
        
        # Классифицируем кластер по сумме факторов
        # Если сумма >= 12, то кластер коммерческий
        cluster_intent = classify_cluster_by_factors(total_factors, commercial_threshold)
        
        cluster_info = {
            'cluster_id': cluster_id,
            'cluster_df': cluster_df,
            'total_domains': total_domains,
            'total_offers': total_offers,
            'total_factors': total_factors,
            'intent': cluster_intent,
            'size': len(cluster_df)
        }
        
        if cluster_intent == 'commercial':
            commercial_clusters.append(cluster_info)
        else:
            informational_clusters.append(cluster_info)
    
    # Сортируем кластеры по размеру (от большего к меньшему)
    commercial_clusters.sort(key=lambda x: x['size'], reverse=True)
    informational_clusters.sort(key=lambda x: x['size'], reverse=True)
    
    # Создаем листы для коммерческих кластеров
    for cluster_info in commercial_clusters:
        _create_single_cluster_sheet(
            cluster_info,
            writer,
            formats,
            cluster_column
        )
    
    # Создаем листы для информационных кластеров
    for cluster_info in informational_clusters:
        _create_single_cluster_sheet(
            cluster_info,
            writer,
            formats,
            cluster_column
        )
    
    print(f"  ✓ Создано листов кластеров: {len(commercial_clusters)} коммерческих, {len(informational_clusters)} информационных")


def _create_single_cluster_sheet(
    cluster_info: Dict,
    writer: pd.ExcelWriter,
    formats: dict,
    cluster_column: str
):
    """
    Создает один лист для кластера.
    
    Args:
        cluster_info: Словарь с информацией о кластере
        writer: ExcelWriter объект
        formats: Словарь с форматами
        cluster_column: Название колонки с ID кластера
    """
    cluster_id = cluster_info['cluster_id']
    cluster_df = cluster_info['cluster_df']
    total_factors = cluster_info['total_factors']
    intent = cluster_info['intent']
    
    # Формируем название листа
    # Ограничиваем длину названия (Excel ограничение 31 символ)
    intent_prefix = 'К' if intent == 'commercial' else 'И'
    # Используем номер кластера (начиная с 1)
    cluster_num = cluster_id + 1 if isinstance(cluster_id, int) else str(cluster_id)
    sheet_name = f"{intent_prefix}_{cluster_num}"
    
    # Если название слишком длинное, обрезаем
    if len(sheet_name) > 31:
        sheet_name = sheet_name[:31]
    
    # Сортируем запросы по частоте
    if 'frequency_world' in cluster_df.columns:
        cluster_df = cluster_df.sort_values('frequency_world', ascending=False)
    elif 'frequency_exact' in cluster_df.columns:
        cluster_df = cluster_df.sort_values('frequency_exact', ascending=False)
    
    # Выбираем колонки для экспорта
    columns_to_export = select_columns_for_export(cluster_df)
    df_export = cluster_df[columns_to_export].copy()
    
    # Конвертируем LSI фразы
    if 'lsi_phrases' in df_export.columns:
        df_export['lsi_phrases'] = df_export['lsi_phrases'].apply(convert_query_lsi_phrases)
    
    if 'cluster_lsi_phrases' in df_export.columns:
        df_export['cluster_lsi_phrases'] = df_export['cluster_lsi_phrases'].apply(convert_cluster_lsi_phrases)
    
    # Записываем в Excel (начинаем с строки 2, чтобы первая строка была для метаданных)
    df_export.to_excel(writer, sheet_name=sheet_name, index=False, startrow=2, header=False)
    
    worksheet = writer.sheets[sheet_name]
    
    # Записываем метаданные кластера в первую строку
    cluster_num = cluster_id + 1 if isinstance(cluster_id, int) else str(cluster_id)
    intent_ru = 'Коммерческий' if intent == 'commercial' else 'Информационный'
    cluster_meta = f"Кластер {cluster_num} | {intent_ru} | Коммерческих факторов: {total_factors} (домены: {cluster_info['total_domains']}, offer: {cluster_info['total_offers']})"
    worksheet.write(0, 0, cluster_meta, formats.get('header', None))
    
    # Записываем заголовки с переводом на русский во вторую строку
    for col_num, col_name in enumerate(df_export.columns):
        russian_name = get_column_translation(col_name)
        worksheet.write(1, col_num, russian_name, formats['header'])
    
    # Настройки листа (замораживаем строку с заголовками)
    worksheet.freeze_panes(2, 0)
    # Автофильтр начинается со строки заголовков (строка 1)
    worksheet.autofilter(1, 0, len(df_export) + 1, len(df_export.columns) - 1)
    
    # Форматирование
    set_column_widths(worksheet, df_export.columns)
    apply_number_formats(worksheet, df_export, formats)
    add_conditional_formatting(worksheet, df_export, sheet_name)

