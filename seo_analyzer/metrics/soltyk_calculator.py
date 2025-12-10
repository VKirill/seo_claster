"""Soltyk KEI формулы"""

import pandas as pd


def kei_soltyk_competition(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 1: Уровень конкуренции (минимизировать)
    ((1 + MainPages*2 + TitlesCount) * (DocsCount / ExactFreq)) / (1 + ExactFreq/12)
    """
    main_pages = df.get('serp_main_pages', 0)
    titles_count = df.get('serp_titles_count', 0)
    docs_count = df.get('serp_docs_count', 0)
    exact_freq = df.get('frequency_exact', 0)
    
    numerator = (1 + main_pages * 2 + titles_count) * (docs_count / (exact_freq + 0.01))
    denominator = 1 + (exact_freq / 12)
    
    return numerator / denominator


def kei_soltyk_effectiveness(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 2: Эффективность запроса (максимизировать)
    ((ExactFreq / 12) + ExactFreq) / (2 + MainPages + TitlesCount)
    """
    exact_freq = df.get('frequency_exact', 0)
    main_pages = df.get('serp_main_pages', 0)
    titles_count = df.get('serp_titles_count', 0)
    
    numerator = (exact_freq / 12) + exact_freq
    denominator = 2 + main_pages + titles_count
    
    return numerator / (denominator + 0.01)


def kei_soltyk_coefficient(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 3: Коэффициент KEI (максимизировать)
    ExactFreq / (MainPages + TitlesCount + 1)
    """
    exact_freq = df.get('frequency_exact', 0)
    main_pages = df.get('serp_main_pages', 0)
    titles_count = df.get('serp_titles_count', 0)
    
    return exact_freq / (main_pages + titles_count + 1)


def kei_soltyk_popularity(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 4: Популярность запроса (максимизировать)
    (BaseFreq + ExactFreq * 2) / 3
    """
    base_freq = df.get('frequency_world', 0)
    exact_freq = df.get('frequency_exact', 0)
    
    return (base_freq + exact_freq * 2) / 3


def kei_soltyk_potential_traffic(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 5: Потенциальный трафик (максимизировать)
    
    Формула с Yandex Direct:
    ((ExactFreq + 0.0001) / 12) * (((CTR + 0.0001) / 100) + 0.1)
    
    - Деление на 12: усреднение годовых данных по месяцам
    - CTR из Yandex Direct Premium CTR
    - +0.1: учет органического трафика даже при нулевом CTR в Директе
    
    Возвращает: потенциальный месячный трафик
    """
    exact_freq = df.get('frequency_exact', 0).fillna(0)
    
    # CTR из Yandex Direct (premium_ctr) или дефолт 3%
    if 'premium_ctr' in df.columns:
        ctr = df.get('premium_ctr', 3).fillna(3)
    else:
        ctr = pd.Series(3, index=df.index)  # Средний CTR по органике
    
    # Формула
    return ((exact_freq + 0.0001) / 12) * (((ctr + 0.0001) / 100) + 0.1)


def kei_soltyk_cost_per_visit(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 6: Стоимость визита (максимизировать)
    
    Формула с Yandex Direct:
    (Budget + 0.0001) / (ExactFreq + 0.01)
    
    Оценивает стоимость привлечения одного посетителя через контекстную рекламу.
    Чем выше значение, тем более "дорогим" считается запрос (высокая коммерческая ценность).
    
    Budget рассчитывается как: Shows * (CTR/100) * AvgCPC
    """
    exact_freq = df.get('frequency_exact', 0).fillna(0)
    
    # Если есть реальные данные Direct - используем их
    if 'direct_shows' in df.columns and 'premium_ctr' in df.columns and 'direct_avg_cpc' in df.columns:
        shows = df.get('direct_shows', 0).fillna(0)
        ctr = df.get('premium_ctr', 3).fillna(3)
        cpc = df.get('direct_avg_cpc', 100).fillna(100)
        
        # Расчет бюджета: Shows * (CTR/100) * CPC
        budget = shows * (ctr / 100) * cpc
        
        # Если данных Direct нет (shows == 0), используем оценочный бюджет
        budget = budget.mask(shows == 0, exact_freq * 1.0)
    else:
        # Оценочный бюджет на основе частотности (1% * 100₽ CPC)
        budget = exact_freq * 1.0
    
    # Формула
    return (budget + 0.0001) / (exact_freq + 0.01)


def kei_soltyk_potential_revenue(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 7: Потенциальный доход (максимизировать)
    
    Формула: KEI_5 × KEI_6
    
    KEI_5 (Потенциальный трафик) × KEI_6 (Стоимость визита)
    = Потенциальный месячный трафик × Стоимость визита
    
    Показывает ТОЛЬКО если есть:
    - Частотность (Wordstat) 
    - Данные Yandex Direct (для точного расчета KEI 5 и KEI 6)
    
    Возвращает: потенциальный месячный доход в рублях
    """
    kei5 = kei_soltyk_potential_traffic(df)  # Потенциальный трафик (визиты/месяц)
    kei6 = kei_soltyk_cost_per_visit(df)      # Стоимость визита (₽)
    
    return kei5 * kei6


def kei_soltyk_synergy(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 8: Синергия (максимизировать)
    (Effectiveness * Popularity) / (Competition + 1)
    """
    effectiveness = kei_soltyk_effectiveness(df)
    popularity = kei_soltyk_popularity(df)
    competition = kei_soltyk_competition(df)
    
    return (effectiveness * popularity) / (competition + 1)


def kei_soltyk_yandex_relevance(df: pd.DataFrame) -> pd.Series:
    """
    Soltyk KEI 9: Релевантность Яндекса (максимизировать)
    (TitlesCount / MainPages) * (ExactFreq / BaseFreq)
    """
    titles_count = df.get('serp_titles_count', 0)
    main_pages = df.get('serp_main_pages', 0)
    exact_freq = df.get('frequency_exact', 0)
    base_freq = df.get('frequency_world', 0)
    
    title_ratio = titles_count / (main_pages + 0.01)
    freq_ratio = exact_freq / (base_freq + 0.01)
    
    return title_ratio * freq_ratio

