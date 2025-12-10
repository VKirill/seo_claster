"""
Этап оценки конверсии через DeepSeek AI.

Анализирует топ-N высокочастотных запросов для определения:
- Средней стоимости лида
- Процента конверсии
"""

from typing import Tuple


def estimate_conversion_parameters(analyzer, args) -> Tuple[float, float]:
    """
    Оценка параметров конверсии через DeepSeek или из конфига.
    
    Args:
        analyzer: Экземпляр SEOAnalyzer с DataFrame
        args: Аргументы командной строки
        
    Returns:
        Tuple[avg_check, conversion_rate]
    """
    # Проверка конфигурации DeepSeek
    try:
        from config_local import (
            DEEPSEEK_ENABLED,
            DEEPSEEK_API_KEY,
            DEEPSEEK_TOP_N_QUERIES
        )
        deepseek_enabled = DEEPSEEK_ENABLED
        deepseek_api_key = DEEPSEEK_API_KEY
        deepseek_top_n = DEEPSEEK_TOP_N_QUERIES
    except ImportError:
        deepseek_enabled = False
        deepseek_api_key = None
        deepseek_top_n = 15
    
    print(f"\n🤖 Оценка стоимости лида и конверсии...")
    
    # Если DeepSeek включён и доступен API ключ
    if deepseek_enabled and deepseek_api_key:
        try:
            from seo_analyzer.analysis.deepseek_conversion_estimator import (
                estimate_conversion_for_dataframe
            )
            
            lead_cost, conversion_rate = estimate_conversion_for_dataframe(
                analyzer.df,
                api_key=deepseek_api_key,
                top_n=deepseek_top_n,
                frequency_column='frequency_exact'
            )
            
            print(f"  💰 Стоимость лида (DeepSeek AI): {lead_cost:.2f} руб")
            print(f"  📊 Конверсия (DeepSeek AI): {conversion_rate:.2f}%")
            
            return lead_cost, conversion_rate
            
        except Exception as e:
            print(f"  ⚠️  Ошибка DeepSeek: {e}")
            print(f"  💡 Использую дефолтные значения")
    else:
        print(f"  💡 DeepSeek отключен, использую дефолтные/настроенные значения")
    
    # Fallback: значения из args или дефолт
    avg_check = getattr(args, 'avg_check', 5000)
    conversion_rate = getattr(args, 'conversion_rate', 2.0)
    
    print(f"  💰 Стоимость лида (дефолт): {avg_check:.2f} руб")
    print(f"  📊 Конверсия (дефолт): {conversion_rate:.2f}%")
    
    return avg_check, conversion_rate


def print_business_parameters(avg_check: float, conversion_rate: float, 
                             margin: float, market_share: float):
    """
    Вывод используемых параметров бизнеса.
    
    Args:
        avg_check: Средний чек (стоимость лида)
        conversion_rate: Процент конверсии
        margin: Маржинальность
        market_share: Доля рынка
    """
    print(f"\n📊 Используемые параметры бизнеса:")
    print(f"   💰 Средний чек (стоимость лида): {avg_check:.2f} руб")
    print(f"   📊 Конверсия: {conversion_rate:.2f}%")
    print(f"   📈 Маржа: {margin:.2f}%")
    print(f"   🎯 Доля рынка: {market_share:.2f}%")


