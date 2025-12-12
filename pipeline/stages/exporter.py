"""Этап 6: Экспорт всех результатов"""

import os
import pandas as pd
from seo_analyzer.export.csv_exporter import CSVExporter
from seo_analyzer.export.json_exporter import JSONExporter
from seo_analyzer.export.graph_exporter import GraphExporter
# ОТКЛЮЧЕНО: from seo_analyzer.export.html_visualizer import HTMLVisualizer
from seo_analyzer.export.excel_exporter import ExcelExporter
from seo_analyzer.core.config import EXCEL_CONFIG, HIERARCHY_CONFIG, PROJECT_ROOT
# ОТКЛЮЧЕНО: from seo_analyzer.analysis import HierarchyBuilder
# ОТКЛЮЧЕНО: Анализ связей между кластерами
# from seo_analyzer.analysis.cluster_relationship_analyzer import ClusterRelationshipAnalyzer
# from seo_analyzer.analysis.cluster_relationship_applier import apply_cluster_relationships


def get_deepseek_api_key():
    """
    Получить DeepSeek API ключ из разных источников
    
    Returns:
        API ключ или None
    """
    # 1. Попробовать config_local.py
    try:
        import config_local
        api_key = getattr(config_local, 'DEEPSEEK_API_KEY', None)
        if api_key and api_key != "":
            print("✓ DeepSeek API ключ загружен из config_local.py")
            return api_key
    except ImportError:
        pass
    
    # 2. Попробовать переменную окружения
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if api_key:
        print("✓ DeepSeek API ключ загружен из переменной окружения")
        return api_key
    
    # 3. Попробовать из конфига
    api_key = HIERARCHY_CONFIG.get('deepseek_api_key', '')
    if api_key and api_key != "":
        return api_key
    
    return None


def is_hierarchy_enabled():
    """
    Проверить включен ли анализ иерархии
    
    Returns:
        True если включен
    """
    # 1. Попробовать config_local.py
    try:
        import config_local
        enabled = getattr(config_local, 'ENABLE_HIERARCHY_ANALYSIS', None)
        if enabled is not None:
            return enabled
    except ImportError:
        pass
    
    # 2. Попробовать переменную окружения
    env_enabled = os.getenv('ENABLE_HIERARCHY_ANALYSIS')
    if env_enabled is not None:
        return env_enabled.lower() in ['true', '1', 'yes']
    
    # 3. Из конфига
    return HIERARCHY_CONFIG.get('enabled', False)


async def export_results_stage(args, analyzer):
    """
    Экспорт всех результатов
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (файлы сохраняются в output_dir)
    """
    print("💾 ЭТАП 9: Экспорт результатов")
    print("-" * 80)
    
    # Проверяем что DataFrame не пустой
    if len(analyzer.df) == 0:
        print("⚠️  DataFrame пустой после фильтрации")
        print("⚠️  Нет данных для экспорта. Экспорт пропущен.")
        print()
        return
    
    # ОТКЛЮЧЕНО: Анализ связей между кластерами для перелинковки
    relationships = {}
    # if 'semantic_cluster_id' in analyzer.df.columns:
    #     relationship_analyzer = ClusterRelationshipAnalyzer(
    #         min_url_overlap=3,      # Минимум 3 общих URL
    #         min_word_overlap=2,     # Минимум 2 общих ключевых слова
    #         max_related_clusters=5  # Максимум 5 связанных кластеров
    #     )
    #     relationships = relationship_analyzer.analyze_relationships(analyzer.df)
    #     
    #     # Добавляем колонку related_clusters в DataFrame
    #     analyzer.df = apply_cluster_relationships(
    #         analyzer.df,
    #         relationships,
    #         cluster_column='semantic_cluster_id'
    #     )
    
    # Инициализируем экспортеры (передаём relationships в JSON экспортер)
    analyzer.csv_exporter = CSVExporter()
    analyzer.json_exporter = JSONExporter(relationships=relationships)
    analyzer.graph_exporter = GraphExporter()
    # ОТКЛЮЧЕНО: analyzer.html_visualizer = HTMLVisualizer()
    
    # Генерируем суффикс для файлов на основе параметров кластеризации
    clustering_threshold = getattr(args, 'clustering_threshold', None)
    max_cluster_size_param = getattr(args, 'max_cluster_size_param', None)
    
    if clustering_threshold is not None:
        if max_cluster_size_param is not None:
            # Оба параметра указаны: "6_0" или "6_50"
            file_suffix = f"_{clustering_threshold}_{max_cluster_size_param}"
        else:
            # Только threshold: "6"
            file_suffix = f"_{clustering_threshold}"
    else:
        file_suffix = ""
    
    # CSV экспорт
    csv_path = analyzer.output_dir / f"seo_analysis_full{file_suffix}.csv"
    analyzer.csv_exporter.export_full_results(analyzer.df, csv_path)
    
    # Топ запросы (ОТКЛЮЧЕНО: не используется)
    # top_csv = analyzer.output_dir / "seo_analysis_top1000.csv"
    # analyzer.csv_exporter.export_top_queries(analyzer.df, top_csv, top_n=1000)
    
    # Сводка кластеров (ОТКЛЮЧЕНО: не используется)
    # Кластеры (только если есть semantic_cluster_id)
    if 'semantic_cluster_id' in analyzer.df.columns:
        # clusters_csv = analyzer.output_dir / "clusters_summary.csv"
        # analyzer.csv_exporter.export_clusters_summary(analyzer.df, clusters_csv)
        
        # JSON экспорт
        json_path = analyzer.output_dir / f"seo_analysis_hierarchy{file_suffix}.json"
        clustering_params = getattr(args, 'clustering_params', None)
        analyzer.json_exporter.export_hierarchical(analyzer.df, json_path, clustering_params=clustering_params)
    else:
        print("ℹ️  Пропускаем экспорт кластеров (ЭТАП 4 отключен)")
    
    # Статистика
    stats_path = analyzer.output_dir / f"statistics{file_suffix}.json"
    analyzer.json_exporter.export_statistics(analyzer.df, stats_path)
    
    # Граф (если построен)
    if analyzer.graph_builder and analyzer.graph_builder.graph:
        graphml_path = analyzer.output_dir / f"queries_graph{file_suffix}.graphml"
        analyzer.graph_exporter.export_graphml(
            analyzer.graph_builder.graph,
            graphml_path,
            analyzer.graph_builder.communities,
            analyzer.graph_builder.pagerank_scores
        )
        
        gexf_path = analyzer.output_dir / f"queries_graph{file_suffix}.gexf"
        analyzer.graph_exporter.export_gexf(
            analyzer.graph_builder.graph,
            gexf_path,
            analyzer.graph_builder.communities,
            analyzer.graph_builder.pagerank_scores
        )
    
    # HTML дашборд (ОТКЛЮЧЕНО: не используется)
    # html_path = analyzer.output_dir / "dashboard.html"
    # analyzer.html_visualizer.generate_dashboard(analyzer.df, html_path)
    
    # Excel экспорт (если не пропущен)
    if not args.skip_excel:
        analyzer.excel_exporter = ExcelExporter()
        excel_path = analyzer.output_dir / f"seo_analysis{file_suffix}.xlsx"
        
        print(f"💾 Экспорт в Excel: seo_analysis{file_suffix}.xlsx...")
        include_charts = args.excel_with_charts or EXCEL_CONFIG['include_charts']
        
        # ОТКЛЮЧЕНО: Построение иерархии (если включено)
        # hierarchy_df = None
        # if is_hierarchy_enabled() and 'serp_urls' in analyzer.df.columns:
        #     api_key = get_deepseek_api_key()
        #     
        #     if api_key:
        #         try:
        #             print("🏗️  Построение иерархии проекта...")
        #             
        #             # Пути к БД и стоп-доменам
        #             db_path = analyzer.output_dir / "page_content.db"
        #             stop_domains_file = PROJECT_ROOT / "keywords_stop" / "domain_stop.txt"
        #             
        #             hierarchy_builder = HierarchyBuilder(
        #                 deepseek_api_key=api_key,
        #                 max_urls_per_query=HIERARCHY_CONFIG.get('max_urls_per_query', 3),
        #                 db_path=db_path,
        #                 stop_domains_file=stop_domains_file,
        #                 collect_breadcrumbs=HIERARCHY_CONFIG.get('collect_breadcrumbs', False),
        #                 use_breadcrumbs=HIERARCHY_CONFIG.get('use_breadcrumbs', False)
        #             )
        #             
        #             hierarchy_result = hierarchy_builder.build_hierarchy_from_dataframe(
        #                 analyzer.df,
        #                 use_clusters=HIERARCHY_CONFIG.get('use_clusters', True)
        #             )
        #             
        #             if hierarchy_result.get('success'):
        #                 hierarchy_df = hierarchy_builder.format_for_excel(hierarchy_result)
        #                 print(f"✓ Иерархия построена: {len(hierarchy_df)} записей")
        #             else:
        #                 print(f"⚠️  Не удалось построить иерархию: {hierarchy_result.get('error')}")
        #         
        #         except Exception as e:
        #             print(f"⚠️  Ошибка построения иерархии: {e}")
        #     else:
        #         print("ℹ️  API ключ DeepSeek не настроен, иерархия не будет построена")
        # 
        # # Устанавливаем данные иерархии в экспортер
        # if hierarchy_df is not None and not hierarchy_df.empty:
        #     analyzer.excel_exporter.set_hierarchy_data(hierarchy_df)
        
        analyzer.excel_exporter.export_to_excel(
            analyzer.df,
            excel_path,
            include_charts=include_charts,
            group_by_clusters=EXCEL_CONFIG['group_by_clusters']
        )
        
        print(f"✓ Excel файл создан: {excel_path}")
    
    # Экспорт брендов (автоматически)
    if analyzer.brand_detector:
        print("💾 Экспорт брендов: brands.csv...")
        
        # Получаем все найденные бренды
        all_brands = analyzer.brand_detector.get_top_brands(1000)  # Топ-1000
        
        if all_brands:
            brands_df = pd.DataFrame(all_brands, columns=['brand', 'count'])
            
            # Полная таблица брендов
            brands_path = analyzer.output_dir / f"brands{file_suffix}.csv"
            brands_df.to_csv(brands_path, index=False, encoding='utf-8-sig')
            
            # Топ-100 брендов
            top_brands_path = analyzer.output_dir / f"brands_top100{file_suffix}.csv"
            brands_df.head(100).to_csv(top_brands_path, index=False, encoding='utf-8-sig')
            
            print(f"✓ Экспортировано {len(brands_df)} брендов")
            print(f"  - {brands_path.name} (все бренды)")
            print(f"  - {top_brands_path.name} (топ-100)")
            
            # Дополнительно: экспорт запросов с брендами
            if 'is_brand' in analyzer.df.columns:
                branded_queries = analyzer.df[analyzer.df['is_brand'] == True].copy()
                if len(branded_queries) > 0:
                    branded_path = analyzer.output_dir / f"branded_queries{file_suffix}.csv"
                    branded_queries.to_csv(branded_path, index=False, encoding='utf-8-sig')
                    print(f"  - {branded_path.name} ({len(branded_queries)} брендовых запросов)")
        else:
            print("  ℹ️  Бренды не найдены")
    
    # Экспорт заблокированных стоп-словами запросов
    if hasattr(analyzer, 'stopwords_filter') and analyzer.stopwords_filter.blocked_queries:
        print("💾 Экспорт запросов со стоп-словами: stopwords_blocked.csv...")
        blocked_df = pd.DataFrame(analyzer.stopwords_filter.blocked_queries)
        blocked_path = analyzer.output_dir / f"stopwords_blocked{file_suffix}.csv"
        blocked_df.to_csv(blocked_path, index=False, encoding='utf-8-sig')
        print(f"✓ Экспортировано {len(blocked_df)} заблокированных запросов")
    
    # Экспорт удаленных неявных дублей
    if hasattr(analyzer, 'removed_implicit_duplicates') and not analyzer.removed_implicit_duplicates.empty:
        print("💾 Экспорт неявных дублей: implicit_duplicates_removed.csv...")
        removed_path = analyzer.output_dir / f"implicit_duplicates_removed{file_suffix}.csv"
        analyzer.removed_implicit_duplicates.to_csv(removed_path, index=False, encoding='utf-8-sig')
        print(f"✓ Экспортировано {len(analyzer.removed_implicit_duplicates)} удаленных дублей")
    
    # Экспорт групп дублей для проверки
    if hasattr(analyzer, 'advanced_deduplicator') and analyzer.advanced_deduplicator.duplicate_groups:
        print("💾 Экспорт групп дублей: implicit_duplicates_groups.csv...")
        groups_path = analyzer.output_dir / f"implicit_duplicates_groups{file_suffix}.csv"
        analyzer.advanced_deduplicator.export_duplicate_groups(groups_path)
    
    print()

