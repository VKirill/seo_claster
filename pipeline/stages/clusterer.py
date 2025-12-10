"""Этап 4.6: Кластеризация запросов на основе SERP данных"""

from seo_analyzer.clustering.serp_clusterer import SERPClusterer
from seo_analyzer.clustering.serp_advanced_clusterer import AdvancedSERPClusterer
from seo_analyzer.clustering.iterative_serp_clusterer import IterativeSERPClusterer
from seo_analyzer.clustering.word_match_clusterer import WordMatchClusterer
from seo_analyzer.core.config import CLUSTERING_CONFIG
from .stage_logger import get_group_prefix, print_stage



async def clustering_stage(args, analyzer):
    """
    Кластеризация запросов на основе SERP данных
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в analyzer)
    """
    prefix = get_group_prefix(analyzer)
    print_stage(analyzer, "🔬 ЭТАП 4: Кластеризация (на основе SERP)")
    print_stage(analyzer, "-" * 80)
    
    # Проверяем что DataFrame не пустой
    if len(analyzer.df) == 0:
        print_stage(analyzer, "⚠️  DataFrame пустой, пропускаем кластеризацию")
        print()
        return
    
    # Выбор алгоритма кластеризации
    use_legacy = getattr(args, 'use_legacy_serp', False)  # По умолчанию используем продвинутый
    use_maxmin = getattr(args, 'maxmin', True)  # Итеративная кластеризация от большего к меньшему (по умолчанию включено)
    
    if use_maxmin:
        # Итеративная кластеризация от большего к меньшему порогу
        print_stage(analyzer, "🔄 Итеративная кластеризация (от большего к меньшему порогу)...")
        print_stage(analyzer, f"   Диапазон порогов: от 20 до 4 общих URL")
        print_stage(analyzer, f"   Алгоритм: сначала формируются кластеры с максимальными связями (20 URL),")
        print_stage(analyzer, f"              затем постепенно снижается порог до 4 URL")
        
        serp_config = CLUSTERING_CONFIG.get('serp_advanced', {})
        
        iterative_clusterer = IterativeSERPClusterer(
            min_threshold=3,
            max_threshold=10,
            top_positions=getattr(args, 'serp_top_positions', serp_config.get('top_positions', 20)),
            max_cluster_size=getattr(args, 'max_cluster_size', serp_config.get('max_cluster_size', 100)),
            semantic_check=True,
            geo_dicts=analyzer.geo_dicts,
            verbose=True
        )
        
        # 🌍 Передаем geo_processor если он был создан ранее (асинхронная обработка)
        geo_processor = getattr(analyzer, 'geo_processor', None)
        
        analyzer.df = await iterative_clusterer.cluster_by_serp(
            analyzer.df,
            serp_column='serp_urls',
            geo_processor=geo_processor
        )
        
        # Выводим статистику
        stats = iterative_clusterer.get_cluster_stats()
        if stats and 'total_clusters' in stats:
            print_stage(analyzer, f"\n📊 Статистика кластеризации:")
            print_stage(analyzer, f"  • Всего кластеров: {stats.get('total_clusters', 0)}")
            if 'avg_cluster_size' in stats:
                print_stage(analyzer, f"  • Средний размер: {stats['avg_cluster_size']:.1f}")
            if 'min_cluster_size' in stats and 'max_cluster_size' in stats:
                print_stage(analyzer, f"  • Мин/Макс: {stats['min_cluster_size']}/{stats['max_cluster_size']}")
            if 'singleton_clusters' in stats:
                print_stage(analyzer, f"  • Одиночных запросов: {stats['singleton_clusters']}")
            
            # Выводим точное количество общих URL для кластеров (первые 5 для примера)
            url_overlaps = iterative_clusterer.get_cluster_url_overlaps(analyzer.df, serp_column='serp_urls')
            if url_overlaps:
                print_stage(analyzer, f"\n📊 Точное количество общих URL в кластерах:")
                shown_clusters = 0
                for cluster_id, overlaps in sorted(url_overlaps.items())[:5]:  # Показываем первые 5 кластеров
                    if overlaps:
                        cluster_queries = iterative_clusterer.cluster_queries.get(cluster_id, [])
                        print_stage(analyzer, f"  Кластер {cluster_id + 1} (размер: {len(cluster_queries)}):")
                        for overlap_info in overlaps[:3]:  # Показываем первые 3 пары
                            q1 = overlap_info['query1'][:40]
                            q2 = overlap_info['query2'][:40]
                            overlap = overlap_info['overlap']
                            print_stage(analyzer, f"    • {q1} ↔ {q2}: {overlap} общих URL")
                        if len(overlaps) > 3:
                            print_stage(analyzer, f"    ... и еще {len(overlaps) - 3} пар")
                        shown_clusters += 1
                if len(url_overlaps) > shown_clusters:
                    print_stage(analyzer, f"  ... и еще {len(url_overlaps) - shown_clusters} кластеров")
    
    elif not use_legacy:
        # Продвинутая SERP кластеризация с контролем транзитивности
        serp_config = CLUSTERING_CONFIG.get('serp_advanced', {})
        
        serp_clusterer = AdvancedSERPClusterer(
            min_common_urls=getattr(args, 'serp_similarity_threshold', serp_config.get('min_common_urls', 7)),
            top_positions=getattr(args, 'serp_top_positions', serp_config.get('top_positions', 20)),
            max_cluster_size=getattr(args, 'max_cluster_size', serp_config.get('max_cluster_size', 100)),
            mode=getattr(args, 'serp_mode', serp_config.get('mode', 'balanced')),
            semantic_check=True,  # ВКЛЮЧЕНО: Для географической сегментации кластеров
            min_cluster_cohesion=serp_config.get('min_cluster_cohesion', 0.6),
            geo_dicts=analyzer.geo_dicts  # Передаем гео-словари для семантической проверки
        )
        
        # 🌍 Передаем geo_processor если он был создан ранее (асинхронная обработка)
        geo_processor = getattr(analyzer, 'geo_processor', None)
        
        analyzer.df = await serp_clusterer.cluster_by_serp(
            analyzer.df,
            serp_column='serp_urls',
            geo_processor=geo_processor
        )
        
        # Выводим статистику
        stats = serp_clusterer.get_cluster_stats()
        if stats and 'total_clusters' in stats:
            print_stage(analyzer, f"\n📊 Статистика кластеризации:")
            print_stage(analyzer, f"  • Всего кластеров: {stats.get('total_clusters', 0)}")
            if 'avg_cluster_size' in stats:
                print_stage(analyzer, f"  • Средний размер: {stats['avg_cluster_size']:.1f}")
            if 'min_cluster_size' in stats and 'max_cluster_size' in stats:
                print_stage(analyzer, f"  • Мин/Макс: {stats['min_cluster_size']}/{stats['max_cluster_size']}")
            if 'singleton_clusters' in stats:
                print_stage(analyzer, f"  • Одиночных запросов: {stats['singleton_clusters']}")
    else:
        # Старый алгоритм (для обратной совместимости)
        serp_clusterer = SERPClusterer(
            min_common_urls=args.serp_similarity_threshold,
            top_positions=args.serp_top_positions,
            max_cluster_size=getattr(args, 'max_cluster_size', 50)
        )
        
        # Старый алгоритм не поддерживает geo_processor, но если он есть - подождем его завершения
        geo_processor = getattr(analyzer, 'geo_processor', None)
        if geo_processor is not None:
            print_stage(analyzer, "  ⏳ Ожидание завершения фоновой обработки географии...")
            await geo_processor.get_result()
        
        analyzer.df = serp_clusterer.cluster_by_serp(
            analyzer.df,
            serp_column='serp_urls'
        )
    
    # Граф связей (только если явно включен)
    if args.enable_graph and not args.skip_embeddings:
        await build_graph_stage(args, analyzer)
    
    # Группировка по совпадениям слов (аналог KeyCollector)
    if getattr(args, 'enable_word_match', False):
        await word_match_clustering_stage(args, analyzer)
    
    print()


async def build_graph_stage(args, analyzer):
    """Построение графа связей"""
    print_stage(analyzer, "🔄 Построение графа связей...")
    
    try:
        # Используем sentence-transformers для embeddings
        from sentence_transformers import SentenceTransformer
        
        model_name = CLUSTERING_CONFIG['embeddings']['model_name']
        print_stage(analyzer, f"  Загрузка модели: {model_name}...")
        model = SentenceTransformer(model_name)
        
        # Генерируем embeddings
        print_stage(analyzer, "  Генерация embeddings...")
        queries = analyzer.df['keyword'].tolist()
        embeddings = model.encode(
            queries,
            batch_size=CLUSTERING_CONFIG['embeddings']['batch_size'],
            show_progress_bar=True
        )
        
        # Строим граф
        from seo_analyzer.clustering.graph_builder import GraphBuilder
        analyzer.graph_builder = GraphBuilder(CLUSTERING_CONFIG)
        analyzer.graph_builder.build_graph_from_similarity(embeddings, queries)
        
        # Community detection
        analyzer.graph_builder.detect_communities_louvain()
        
        # PageRank
        analyzer.graph_builder.calculate_pagerank()
        
        # Добавляем графовые фичи в DataFrame
        analyzer.df = analyzer.graph_builder.add_graph_features_to_dataframe(analyzer.df)
        
        print_stage(analyzer, f"✓ Граф построен")
        
    except Exception as e:
        print_stage(analyzer, f"⚠️ Ошибка построения графа: {e}")
        print_stage(analyzer, f"  Продолжаем без графа...")


async def word_match_clustering_stage(args, analyzer):
    """Группировка по совпадениям слов (аналог KeyCollector)"""
    print_stage(analyzer, "🔄 Группировка по совпадениям слов (KeyCollector-подобная)...")
    
    # Инициализация с параметрами из аргументов
    word_match_config = CLUSTERING_CONFIG.get('word_match', {})
    
    word_match_clusterer = WordMatchClusterer(
        min_match_strength=args.word_match_strength or word_match_config.get('min_match_strength', 2),
        min_group_size=args.word_match_min_size or word_match_config.get('min_group_size', 2),
        strengthen_links=args.word_match_strengthen,
        exclude_stopwords=word_match_config.get('exclude_stopwords', True),
        use_lemmatization=word_match_config.get('use_lemmatization', True)
    )
    
    # Подготавливаем данные для кластеризации
    queries = analyzer.df['keyword'].tolist()
    
    # Создаем словарь частотностей для сортировки
    frequencies = {}
    if 'frequency_exact' in analyzer.df.columns:
        frequencies = dict(zip(analyzer.df['keyword'], analyzer.df['frequency_exact']))
    elif 'frequency_world' in analyzer.df.columns:
        frequencies = dict(zip(analyzer.df['keyword'], analyzer.df['frequency_world']))
    
    # Кластеризация
    clusters = word_match_clusterer.cluster_queries(queries, frequencies)
    
    # Добавляем результаты в DataFrame
    analyzer.df = word_match_clusterer.add_to_dataframe(
        analyzer.df,
        query_column='keyword',
        cluster_column='word_match_cluster_id',
        cluster_name_column='word_match_cluster_name'
    )
    
    # Выводим статистику
    stats = word_match_clusterer.get_cluster_stats()
    print_stage(analyzer, f"✓ Группировка завершена:")
    print_stage(analyzer, f"  • Создано групп: {stats.get('total_clusters', 0)}")
    print_stage(analyzer, f"  • Средний размер группы: {stats.get('avg_cluster_size', 0):.1f}")
    print_stage(analyzer, f"  • Мин/Макс размер: {stats.get('min_cluster_size', 0)}/{stats.get('max_cluster_size', 0)}")

