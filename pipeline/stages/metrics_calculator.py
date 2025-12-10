"""Этап 4.5: Расчет KEI и SEO метрик"""

from seo_analyzer.metrics.seo_metrics_calculator import SEOMetricsCalculator
from .stage_logger import get_group_prefix, print_stage



async def calculate_metrics_stage(args, analyzer):
    """
    Расчет KEI и SEO метрик
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в analyzer)
    """
    prefix = get_group_prefix(analyzer)
    print_stage(analyzer, "📊 ЭТАП 7: Расчет SEO метрик (KEI, сложность)")
    print_stage(analyzer, "-" * 80)
    
    # Проверяем что DataFrame не пустой
    if len(analyzer.df) == 0:
        print_stage(analyzer, "⚠️  DataFrame пустой, пропускаем расчет метрик")
        print()
        return
    
    analyzer.metrics_calculator = SEOMetricsCalculator()
    
    # Проверяем наличие необходимых данных
    required_cols = ['frequency_world', 'frequency_exact']
    if not all(col in analyzer.df.columns for col in required_cols):
        print_stage(analyzer, "⚠️  Недостаточно данных для расчета метрик (нет частотностей)")
        print()
        return
    
    # Если нет SERP данных, создаем заглушки
    if 'serp_docs_count' not in analyzer.df.columns:
        print_stage(analyzer, "⚠️  SERP данные отсутствуют, используются оценочные значения")
        analyzer.df['serp_docs_count'] = analyzer.df['frequency_world'] * 1000  # Примерная оценка
        analyzer.df['serp_main_pages'] = 30
        analyzer.df['serp_titles_count'] = 15
    
    print_stage(analyzer, "🔄 Расчет всех KEI формул...")
    analyzer.df = analyzer.metrics_calculator.calculate_all_metrics(analyzer.df)
    
    # Статистика
    metrics_summary = analyzer.metrics_calculator.get_metrics_summary(analyzer.df)
    print_stage(analyzer, f"✓ Рассчитано метрик: {metrics_summary['metrics_calculated']}")
    print_stage(analyzer, f"  Средний KEI effectiveness: {metrics_summary['average_metrics'].get('kei_effectiveness', {}).get('mean', 0):.2f}")
    print_stage(analyzer, f"  Средний priority_score: {metrics_summary['average_metrics'].get('priority_score', {}).get('mean', 0):.2f}")
    print()

