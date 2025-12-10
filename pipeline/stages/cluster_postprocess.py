"""Пост-обработка кластеров: делим крупные и прикрепляем одиночки."""

from seo_analyzer.clustering.cluster_postprocessor import ClusterPostprocessor
from seo_analyzer.core.config import CLUSTERING_CONFIG
from .stage_logger import get_group_prefix, print_stage



async def postprocess_clusters_stage(args, analyzer):
    """
    Дополнительный шаг после основной кластеризации.

    Делит кластеры > max_cluster_size, повышая порог общих URL, и
    повторно прикрепляет одиночные запросы, если найдены совпадения.
    
    В maxmin режиме деление больших кластеров отключено (max_cluster_size=10000),
    так как IterativeSERPClusterer уже контролирует размер кластеров.
    """
    prefix = get_group_prefix(analyzer)
    # Проверяем что DataFrame не пустой
    if len(analyzer.df) == 0:
        print_stage(analyzer, "🔧 Пост-обработка кластеров (деление больших групп)...")
        print_stage(analyzer, "⚠️  DataFrame пустой, пропускаем пост-обработку")
        print()
        return
    
    serp_cfg = CLUSTERING_CONFIG.get("serp_advanced", {})
    postprocess_cfg = CLUSTERING_CONFIG.get("postprocess", {})
    
    # Проверяем, используется ли maxmin режим
    use_maxmin = getattr(args, 'maxmin', False)
    
    base_threshold = getattr(args, "serp_similarity_threshold", serp_cfg.get("min_common_urls", 7))
    top_positions = getattr(args, "serp_top_positions", serp_cfg.get("top_positions", 30))
    # Используем max_cluster_size из CLI аргументов, если не указан - берем из конфига постобработки
    # В maxmin режиме устанавливаем очень большой размер, чтобы не делить кластеры
    if use_maxmin:
        max_cluster_size = 10000  # Очень большой размер - деление не произойдет
    else:
        max_cluster_size = getattr(args, "max_cluster_size", postprocess_cfg.get("max_cluster_size", 12))
    threshold_step = getattr(args, "post_threshold_step", postprocess_cfg.get("threshold_step", 1))
    skip_singleton_reattach = getattr(args, "skip_singleton_reattach", False)

    if use_maxmin:
        print_stage(analyzer, "🔧 Пост-обработка кластеров (без деления больших групп, maxmin режим)...")
    else:
        print_stage(analyzer, "🔧 Пост-обработка кластеров (деление больших групп)...")
    post = ClusterPostprocessor(
        base_threshold=base_threshold,
        top_positions=top_positions,
        max_cluster_size=max_cluster_size,
        threshold_step=threshold_step,
        geo_dicts=analyzer.geo_dicts,  # 🌍 Передаем гео-словари для проверки географии
        skip_singleton_reattach=skip_singleton_reattach,  # ⚡ Ускорение: пропускаем прикрепление одиночек
    )
    analyzer.df = post.process(analyzer.df)
    stats = post.get_stats()
    if stats:
        print(
            f"  • Кластеров после пост-обработки: {stats['total_clusters']}, "
            f"макс. размер: {stats['max_cluster_size']}, "
            f"одиночек: {stats['singleton_clusters']}"
        )
    print()

