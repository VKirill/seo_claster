"""Создание аргументов CLI c учетом RUN_CONFIG и конфигов кластеризации."""

import argparse
from seo_analyzer.core.config import RUN_CONFIG, CLUSTERING_CONFIG


def create_argument_parser():
    """
    Создает парсер аргументов командной строки.

    Defaults берутся из RUN_CONFIG (можно переопределить через config_local.py).
    """
    parser = argparse.ArgumentParser(
        description="SEO Analyzer - Универсальный анализатор SEO запросов"
    )

    parser.add_argument(
        "input_file",
        nargs="?",
        default=None,  # По умолчанию None - будут обработаны все группы
        help="Название группы или путь к CSV файлу (по умолчанию - все группы)",
    )
    
    # Система групп
    parser.add_argument(
        "--list-groups",
        action="store_true",
        help="Показать все доступные группы запросов",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Последовательная обработка групп (по умолчанию)",
    )
    parser.add_argument(
        "--single-file",
        action="store_true",
        help="Режим одного файла (для обратной совместимости)",
    )

    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        default=RUN_CONFIG["skip_embeddings"],
        help="Пропустить генерацию embeddings (быстрее)",
    )
    parser.add_argument(
        "--enable-graph",
        action="store_true",
        default=RUN_CONFIG["enable_graph"],
        help="Включить построение графа связей",
    )
    parser.add_argument(
        "--skip-topics",
        action="store_true",
        default=RUN_CONFIG["skip_topics"],
        help="Пропустить topic modeling",
    )
    parser.add_argument(
        "--skip-hierarchical",
        action="store_true",
        default=RUN_CONFIG["skip_hierarchical"],
        help="Пропустить иерархическую кластеризацию",
    )
    parser.add_argument(
        "--skip-forms",
        action="store_true",
        default=RUN_CONFIG["skip_forms"],
        help="Пропустить генерацию падежных форм",
    )
    parser.add_argument(
        "--export-brands",
        action="store_true",
        default=RUN_CONFIG["export_brands"],
        help="Экспортировать список брендов",
    )

    parser.add_argument(
        "--min-frequency",
        type=int,
        default=RUN_CONFIG["min_frequency"],
        help="Минимальная частотность запроса",
    )
    parser.add_argument(
        "--max-frequency-ratio",
        type=float,
        default=RUN_CONFIG["max_frequency_ratio"],
        help="Макс. соотношение frequency_world/frequency_exact",
    )

    # SERP / кластеризация
    serp_cfg = CLUSTERING_CONFIG.get("serp_advanced", {})
    parser.add_argument(
        "--serp-similarity-threshold",
        type=int,
        default=RUN_CONFIG["serp_similarity_threshold"] or serp_cfg.get("min_common_urls", 8),
        help="Мин. общих URL для группировки (SERP, по умолчанию 8)",
    )
    parser.add_argument(
        "--serp-top-positions",
        type=int,
        default=RUN_CONFIG["serp_top_positions"] or serp_cfg.get("top_positions", 30),
        help="Глубина SERP для анализа",
    )
    parser.add_argument(
        "--max-cluster-size",
        type=int,
        default=RUN_CONFIG["max_cluster_size"] or serp_cfg.get("max_cluster_size", 100),
        help="Максимальный размер кластера",
    )
    parser.add_argument(
        "--serp-mode",
        type=str,
        default=RUN_CONFIG["serp_mode"] or serp_cfg.get("mode", "balanced"),
        choices=["strict", "balanced", "soft"],
        help="Режим кластеризации SERP",
    )
    parser.add_argument(
        "--use-legacy-serp",
        action="store_true",
        default=RUN_CONFIG["use_legacy_serp"],
        help="Использовать старый алгоритм SERP кластеризации",
    )
    # maxmin режим включен по умолчанию (из RUN_CONFIG)
    maxmin_default = RUN_CONFIG.get("maxmin", True)
    parser.add_argument(
        "--no-maxmin",
        dest="maxmin",
        action="store_false",
        default=maxmin_default,
        help="Отключить итеративную кластеризацию от большего к меньшему (по умолчанию включено)",
    )
    parser.add_argument(
        "--maxmin",
        dest="maxmin",
        action="store_true",
        help="Явно включить итеративную кластеризацию от большего к меньшему (20→4 общих URL)",
    )

    # Word-match
    parser.add_argument(
        "--enable-word-match",
        action="store_true",
        default=RUN_CONFIG["enable_word_match"],
        help="Включить группировку по совпадениям слов",
    )
    parser.add_argument(
        "--word-match-strength",
        type=int,
        default=RUN_CONFIG["word_match_strength"],
        help="Мин. совпадений слов для группировки",
    )
    parser.add_argument(
        "--word-match-min-size",
        type=int,
        default=RUN_CONFIG["word_match_min_size"],
        help="Минимальный размер группы",
    )
    parser.add_argument(
        "--word-match-strengthen",
        action="store_true",
        default=RUN_CONFIG["word_match_strengthen"],
        help="Усиливать связи в группах",
    )
    parser.add_argument(
        "--word-match-no-strengthen",
        dest="word_match_strengthen",
        action="store_false",
        help="Отключить усиление связей",
    )

    # SERP / KEI / Excel
    parser.add_argument(
        "--xmlstock-api-key",
        type=str,
        default=None,
        help="API ключ xmlstock (опционально, если не указан в config_local.py)",
    )
    parser.add_argument(
        "--serp-batch-async",
        action="store_true",
        default=True,  # По умолчанию включён!
        help="Массовый асинхронный режим SERP (отправка всех сразу, потом получение) - РЕКОМЕНДУЕТСЯ",
    )
    parser.add_argument(
        "--no-serp-batch-async",
        dest="serp_batch_async",
        action="store_false",
        help="Отключить массовый async режим (старый режим - последовательная обработка)",
    )
    parser.add_argument(
        "--skip-excel",
        action="store_true",
        default=RUN_CONFIG["skip_excel"],
        help="Пропустить экспорт в Excel",
    )
    parser.add_argument(
        "--excel-with-charts",
        action="store_true",
        default=RUN_CONFIG["excel_with_charts"],
        help="Включить графики в Excel (экспериментально)",
    )
    parser.add_argument(
        "--soft-clustering",
        action="store_true",
        default=RUN_CONFIG["soft_clustering"],
        help="Soft clustering для тем (несколько тем на запрос)",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Принудительная перезагрузка и обработка (игнорировать кэш запросов)",
    )

    return parser


__all__ = ["create_argument_parser"]

