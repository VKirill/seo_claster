"""
Конфигурация и вспомогательные функции для Yandex Direct этапа.
"""

from typing import Tuple, Dict


def should_run_yandex_direct() -> Tuple[bool, Dict]:
    """
    Проверить нужно ли запускать Yandex Direct.
    
    Returns:
        (should_run, config_dict)
    """
    try:
        from config_local import (
            YANDEX_DIRECT_ENABLED,
            YANDEX_DIRECT_TOKEN,
            YANDEX_DIRECT_CLIENT_ID,
            YANDEX_DIRECT_CLIENT_SECRET,
            YANDEX_DIRECT_USE_SANDBOX
        )
        
        # GEO_ID с дефолтным значением
        try:
            from config_local import YANDEX_DIRECT_GEO_ID
        except ImportError:
            YANDEX_DIRECT_GEO_ID = 213
            
        if not YANDEX_DIRECT_ENABLED:
            return False, {}
            
        if not YANDEX_DIRECT_CLIENT_ID or not YANDEX_DIRECT_CLIENT_SECRET:
            return False, {}
            
        return True, {
            'token': YANDEX_DIRECT_TOKEN,
            'client_id': YANDEX_DIRECT_CLIENT_ID,
            'client_secret': YANDEX_DIRECT_CLIENT_SECRET,
            'use_sandbox': YANDEX_DIRECT_USE_SANDBOX,
            'geo_id': YANDEX_DIRECT_GEO_ID
        }
    except ImportError:
        return False, {}


def print_enrichment_statistics(df, total_queries: int):
    """
    Вывод статистики обогащения данными Direct.
    
    Args:
        df: DataFrame с данными
        total_queries: Общее количество запросов
    """
    enriched_count = (df['direct_shows'] > 0).sum()
    print(f"✓ Обогащено запросов: {enriched_count} из {total_queries}")
    
    if enriched_count > 0:
        # Статистика
        total_shows = df['direct_shows'].sum()
        total_clicks = df['direct_clicks'].sum()
        avg_cpc = df[df['direct_avg_cpc'] > 0]['direct_avg_cpc'].mean()
        
        print(f"  📊 Суммарные показы: {total_shows:,}")
        print(f"  📊 Суммарные клики: {total_clicks:,}")
        print(f"  📊 Средний CPC: {avg_cpc:.2f} руб" if avg_cpc > 0 else "  📊 Средний CPC: нет данных")
        
        # Распределение по конкуренции
        competition_dist = df['direct_competition'].value_counts()
        print(f"\n  Распределение по конкуренции:")
        for level, count in competition_dist.items():
            if level != 'unknown':
                print(f"    {level}: {count} запросов ({count/total_queries*100:.1f}%)")
    else:
        print("  ⚠️  Нет данных Direct (возможно, запросы низкочастотные)")


def print_metrics_calculated(market_share: float, conversion_rate: float, margin: float):
    """
    Вывод информации о рассчитанных метриках.
    
    Args:
        market_share: Доля рынка
        conversion_rate: Процент конверсии
        margin: Маржинальность
    """
    print(f"  ✓ KEI Direct Efficiency")
    print(f"  ✓ KEI Direct Quality Score")
    print(f"  ✓ KEI Direct Profitability")
    print(f"  ✓ Месячный бюджет (доля рынка {market_share}%)")
    print(f"  ✓ Стоимость конверсии")
    print(f"  ✓ Прогноз ROI (конверсия {conversion_rate}%, маржа {margin}%)")
    print(f"  ✓ Безубыточный CPC")
    print(f"  ✓ Эффективность бюджета")





