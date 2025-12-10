"""Этап 4.7: Агрегация LSI фраз по кластерам"""

from seo_analyzer.analysis.cluster_lsi_aggregator import ClusterLSIAggregator
from seo_analyzer.core.config import LSI_CONFIG
from .stage_logger import get_group_prefix, print_stage



async def aggregate_cluster_lsi_stage(args, analyzer):
    """
    Агрегация LSI фраз по кластерам
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в analyzer)
    """
    prefix = get_group_prefix(analyzer)
    print_stage(analyzer, "🔤 ЭТАП 5: Агрегация LSI фраз по кластерам")
    print_stage(analyzer, "-" * 80)
    
    # Проверяем что DataFrame не пустой
    if len(analyzer.df) == 0:
        print_stage(analyzer, "⚠️  DataFrame пустой, пропускаем агрегацию LSI")
        print()
        return
    
    analyzer.lsi_aggregator = ClusterLSIAggregator(
        top_n_per_cluster=LSI_CONFIG['top_n_per_cluster']
    )
    
    # Агрегируем LSI по кластерам
    print_stage(analyzer, "🔄 Агрегация LSI фраз по кластерам...")
    cluster_lsi = analyzer.lsi_aggregator.aggregate_cluster_lsi(
        analyzer.df,
        cluster_column='semantic_cluster_id'
    )
    
    # Добавляем LSI в DataFrame
    analyzer.df = analyzer.lsi_aggregator.add_cluster_lsi_to_dataframe(
        analyzer.df,
        cluster_lsi,
        cluster_column='semantic_cluster_id'
    )
    
    # Агрегируем SERP URL по кластерам
    print_stage(analyzer, "🔄 Агрегация SERP URL по кластерам...")
    cluster_urls = analyzer.lsi_aggregator.aggregate_cluster_serp_urls(
        analyzer.df,
        cluster_column='semantic_cluster_id',
        serp_urls_column='serp_urls',
        top_n=20  # Топ-20 URL кластера (соответствует глубине SERP)
    )
    
    # Добавляем SERP URL в DataFrame
    analyzer.df = analyzer.lsi_aggregator.add_cluster_serp_urls_to_dataframe(
        analyzer.df,
        cluster_urls,
        cluster_column='semantic_cluster_id'
    )
    
    # Экспортируем LSI отдельно (ОТКЛЮЧЕНО: не используется)
    # lsi_export_path = analyzer.output_dir / 'cluster_lsi_phrases.csv'
    # analyzer.lsi_aggregator.export_cluster_lsi(cluster_lsi, lsi_export_path)
    
    # Статистика
    lsi_stats = analyzer.lsi_aggregator.get_statistics(cluster_lsi)
    print_stage(analyzer, f"✓ LSI агрегация завершена:")
    print_stage(analyzer, f"  Кластеров: {lsi_stats['total_clusters']}")
    print_stage(analyzer, f"  Уникальных фраз: {lsi_stats['total_unique_phrases']}")
    print_stage(analyzer, f"  Среднее фраз/кластер: {lsi_stats['avg_phrases_per_cluster']}")
    
    # Статистика SERP URL
    clusters_with_urls = sum(1 for urls in cluster_urls.values() if urls)
    print_stage(analyzer, f"✓ SERP URL агрегация завершена:")
    print_stage(analyzer, f"  Кластеров с URL: {clusters_with_urls}/{len(cluster_urls)}")
    print()

