"""
Пример использования Yandex Direct интеграции в реальном проекте.

Показывает как обогатить результаты кластеризации данными из Директа
и рассчитать расширенные KEI метрики.
"""

import pandas as pd
from pathlib import Path

# Импорт модулей
from seo_analyzer.analysis import YandexDirectIntegrator
from seo_analyzer.metrics import (
    kei_direct_efficiency,
    kei_direct_profitability,
    kei_direct_quality_score,
    kei_direct_traffic_potential,
    kei_direct_budget_required
)


def enrich_csv_with_direct_data(input_csv: str, output_csv: str):
    """
    Обогащение CSV файла с запросами данными из Yandex Direct.
    
    Args:
        input_csv: Путь к входному CSV (должен содержать колонку 'query')
        output_csv: Путь для сохранения обогащенного CSV
    """
    print(f"📂 Загрузка данных из {input_csv}...")
    df = pd.read_csv(input_csv)
    
    print(f"📊 Загружено запросов: {len(df)}")
    
    # Загрузка конфига
    try:
        from config_local import (
            YANDEX_DIRECT_ENABLED,
            YANDEX_DIRECT_TOKEN,
            YANDEX_DIRECT_USE_SANDBOX,
            YANDEX_DIRECT_GEO_ID
        )
    except ImportError:
        print("❌ Не найден config_local.py с настройками Yandex Direct")
        return
    
    if not YANDEX_DIRECT_ENABLED:
        print("⚠️  Yandex Direct отключен в конфиге")
        return
    
    # Инициализация интегратора
    print("🔧 Инициализация Yandex Direct...")
    integrator = YandexDirectIntegrator(
        token=YANDEX_DIRECT_TOKEN,
        use_sandbox=YANDEX_DIRECT_USE_SANDBOX,
        geo_id=YANDEX_DIRECT_GEO_ID,
        enabled=True
    )
    
    # Обогащение данными Direct
    print("⏳ Получение данных из Yandex Direct API...")
    print("   (это может занять несколько минут для большого количества запросов)")
    
    enriched_df = integrator.enrich_dataframe(df, query_column='query')
    
    # Расчет KEI метрик
    print("📈 Расчет KEI метрик...")
    
    enriched_df['kei_direct_efficiency'] = kei_direct_efficiency(enriched_df)
    enriched_df['kei_direct_quality'] = kei_direct_quality_score(enriched_df)
    enriched_df['kei_direct_traffic_top3'] = kei_direct_traffic_potential(
        enriched_df, target_position=3
    )
    enriched_df['kei_direct_budget_100clicks'] = kei_direct_budget_required(
        enriched_df, target_clicks=100
    )
    
    # Прибыльность (настройте под ваш бизнес)
    AVG_CHECK = 5000  # средний чек в рублях
    CONVERSION_RATE = 2.0  # конверсия в процентах
    
    enriched_df['kei_direct_profit'] = kei_direct_profitability(
        enriched_df,
        avg_check=AVG_CHECK,
        conversion_rate=CONVERSION_RATE
    )
    
    # Сохранение результата
    print(f"💾 Сохранение в {output_csv}...")
    enriched_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    
    # Статистика
    print("\n" + "="*70)
    print("✅ ГОТОВО! Статистика обогащения:")
    print("="*70)
    
    total_shows = enriched_df['direct_shows'].sum()
    total_clicks = enriched_df['direct_clicks'].sum()
    avg_cpc = enriched_df['direct_avg_cpc'].mean()
    
    print(f"Всего запросов обработано: {len(enriched_df)}")
    print(f"Суммарные показы: {total_shows:,}")
    print(f"Суммарные клики: {total_clicks:,}")
    print(f"Средний CPC: {avg_cpc:.2f} руб")
    
    # Распределение по конкуренции
    competition_dist = enriched_df['direct_competition'].value_counts()
    print(f"\nРаспределение по конкуренции:")
    for level, count in competition_dist.items():
        print(f"  {level}: {count} запросов ({count/len(enriched_df)*100:.1f}%)")
    
    # Топ-5 по эффективности
    print("\n🏆 Топ-5 по KEI эффективности:")
    top5 = enriched_df.nlargest(5, 'kei_direct_efficiency')[
        ['query', 'direct_shows', 'direct_avg_cpc', 'kei_direct_efficiency']
    ]
    print(top5.to_string(index=False))
    
    print(f"\n📄 Полные результаты сохранены в: {output_csv}")


def analyze_cluster_economics(cluster_df: pd.DataFrame):
    """
    Экономический анализ кластера с данными Direct.
    
    Args:
        cluster_df: DataFrame с запросами кластера (уже обогащенный)
    """
    print("\n" + "="*70)
    print("💰 ЭКОНОМИЧЕСКИЙ АНАЛИЗ КЛАСТЕРА")
    print("="*70)
    
    # Агрегированные метрики
    total_shows = cluster_df['direct_shows'].sum()
    total_clicks = cluster_df['direct_clicks'].sum()
    
    # Средневзвешенный CPC
    weighted_cpc = (
        cluster_df['direct_avg_cpc'] * cluster_df['direct_shows']
    ).sum() / total_shows if total_shows > 0 else 0
    
    # Средний CTR
    avg_ctr = cluster_df['premium_ctr'].mean()
    
    print(f"\n📊 Текущее состояние:")
    print(f"  Показы (месяц): {total_shows:,}")
    print(f"  Клики (месяц): {total_clicks:,}")
    print(f"  Средневзв. CPC: {weighted_cpc:.2f} руб")
    print(f"  Средний CTR: {avg_ctr:.2f}%")
    
    # Прогноз для разных позиций
    print(f"\n🎯 Прогноз трафика по позициям:")
    
    for position in [1, 3, 5, 7]:
        traffic = kei_direct_traffic_potential(
            cluster_df, target_position=position
        ).sum()
        
        budget = traffic * weighted_cpc
        
        print(f"  Позиция #{position}: ~{traffic:.0f} кликов/мес, бюджет ~{budget:,.0f} руб")
    
    # ROI анализ
    print(f"\n💡 ROI анализ (при конверсии 2% и среднем чеке 5000 руб):")
    
    total_profit = cluster_df['kei_direct_profit'].sum()
    total_cost = total_clicks * weighted_cpc
    
    if total_cost > 0:
        roi = (total_profit / total_cost - 1) * 100
        print(f"  Текущие затраты: {total_cost:,.0f} руб")
        print(f"  Прогноз выручки: {total_profit:,.0f} руб")
        print(f"  ROI: {roi:+.1f}%")
    else:
        print("  Недостаточно данных для расчета ROI")


def main():
    """Пример использования."""
    
    print("="*70)
    print("ПРИМЕР ИСПОЛЬЗОВАНИЯ YANDEX DIRECT ИНТЕГРАЦИИ")
    print("="*70)
    
    # Пример 1: Обогащение CSV файла
    print("\n📋 Пример 1: Обогащение CSV файла\n")
    
    # Создаем тестовый CSV
    test_data = pd.DataFrame({
        'query': [
            'купить холодильник москва',
            'холодильник цена',
            'холодильник отзывы',
            'холодильник недорого',
            'где купить холодильник'
        ],
        'frequency_world': [1200, 800, 500, 450, 350]
    })
    
    test_data.to_csv('test_queries.csv', index=False)
    
    enrich_csv_with_direct_data(
        input_csv='test_queries.csv',
        output_csv='test_queries_enriched.csv'
    )
    
    # Пример 2: Анализ экономики кластера
    print("\n📋 Пример 2: Экономический анализ кластера\n")
    
    if Path('test_queries_enriched.csv').exists():
        enriched = pd.read_csv('test_queries_enriched.csv')
        analyze_cluster_economics(enriched)
    
    print("\n" + "="*70)
    print("✅ Примеры выполнены!")
    print("="*70)
    print("\n📚 См. также:")
    print("  - docs/YANDEX_DIRECT_QUICKSTART.md - быстрый старт")
    print("  - docs/guides/YANDEX_DIRECT_INTEGRATION.md - полная документация")
    print("  - test_yandex_direct_integration.py - тесты интеграции")
    print()


if __name__ == "__main__":
    main()

