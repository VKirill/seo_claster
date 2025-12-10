"""
Этап 4.6: Обогащение данными Yandex Direct API.

Получает реальные данные о трафике, CPC и конкуренции для запросов.
"""

from seo_analyzer.analysis import YandexDirectIntegrator
from seo_analyzer.metrics import (
    calculate_monthly_budget
)
from seo_analyzer.core.yandex_direct_auto_auth import ensure_yandex_direct_token
from .deepseek_conversion_stage import estimate_conversion_parameters, print_business_parameters
from .yandex_direct_config import (
    should_run_yandex_direct,
    print_enrichment_statistics,
    print_metrics_calculated
)


async def enrich_with_yandex_direct_stage(args, analyzer):
    """
    Обогащение данными Yandex Direct API.
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в analyzer)
    """
    # Проверяем нужно ли запускать
    should_run, config = should_run_yandex_direct()
    if not should_run:
        return
    
    # Автоматическая проверка и получение токена если нужно
    token = ensure_yandex_direct_token(
        client_id=config['client_id'],
        client_secret=config['client_secret'],
        current_token=config['token']
    )
    
    if not token:
        # Пользователь отказался - тихо пропускаем
        return
    
    print("🎯 ЭТАП 7.5: Обогащение DataFrame данными Yandex Direct")
    print("-" * 80)
    
    # Инициализация интегратора (используем полученный/существующий токен)
    # Передаём путь к БД из analyzer (output/serp_data.db)
    db_path = getattr(analyzer, 'db_path', 'output/serp_data.db')
    
    integrator = YandexDirectIntegrator(
        token=token,
        use_sandbox=config['use_sandbox'],
        geo_id=config['geo_id'],
        enabled=True,
        db_path=db_path
    )
    
    # Обогащение DataFrame (данные берутся из кэша)
    total_queries = len(analyzer.df)
    print(f"📊 Обогащение {total_queries} запросов данными из кэша...")
    
    analyzer.df = integrator.enrich_dataframe(analyzer.df, query_column='keyword')
    
    # Вывод статистики обогащения
    print_enrichment_statistics(analyzer.df, total_queries)
    
    # Проверяем успешность
    enriched_count = (analyzer.df['direct_shows'] > 0).sum()
    
    if enriched_count > 0:
        
        # Оценка конверсии через DeepSeek AI или дефолтные значения
        avg_check, conversion_rate = estimate_conversion_parameters(analyzer, args)
        
        # Остальные параметры бизнеса
        margin = getattr(args, 'margin', 30.0)
        market_share = getattr(args, 'market_share', 5.0)
        
        # Вывод используемых параметров
        print_business_parameters(avg_check, conversion_rate, margin, market_share)
        
        # Расчет KEI метрик с Direct
        print(f"\n🔄 Расчет метрик с данными Direct...")
        
        # Бюджетирование
        analyzer.df['direct_monthly_budget'] = calculate_monthly_budget(
            analyzer.df,
            market_share=market_share
        )
        
        # Вывод информации о рассчитанных метриках
        print_metrics_calculated(market_share, conversion_rate, margin)
    
    print()

