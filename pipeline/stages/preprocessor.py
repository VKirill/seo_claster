"""Этап 2: Предобработка запросов"""

from pathlib import Path
from seo_analyzer.core.normalizer import QueryNormalizer
from seo_analyzer.clustering.geo_processor import AsyncGeoProcessor
from .stage_logger import get_group_prefix, print_stage
from .preprocessing.filter_handler import FilterHandler
from .preprocessing.normalization_handler import NormalizationHandler
from .preprocessing.extraction_handler import ExtractionHandler
from .preprocessing.deduplication_handler import DeduplicationHandler
from .preprocessing.cache_handler import CacheHandler


async def preprocessing_stage(args, analyzer):
    """
    Предобработка запросов
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в analyzer)
    """
    prefix = get_group_prefix(analyzer)
    print_stage(analyzer, "🔧 ЭТАП 2: Предобработка")
    print_stage(analyzer, "-" * 80)
    
    # Debug: проверяем текущее состояние
    loaded_from_cache = getattr(analyzer, 'loaded_from_cache', False)
    loaded_from_master_db = getattr(analyzer, 'loaded_from_master_db', False)
    print_stage(analyzer, f"🔍 DEBUG: loaded_from_cache = {loaded_from_cache}, loaded_from_master_db = {loaded_from_master_db}")
    print_stage(analyzer, f"🔍 DEBUG: Запросов = {len(analyzer.df)}")
    print_stage(analyzer, f"🔍 DEBUG: Колонки на входе: {list(analyzer.df.columns)[:10]}...")
    
    # Если загружено из Master DB - пропускаем ВСЮ предобработку
    if loaded_from_master_db:
        print_stage(analyzer, "✅ Данные из Master DB - пропускаем предобработку (уже готово)")
        print_stage(analyzer, f"   Доступно колонок: {len(analyzer.df.columns)}")
        print_stage(analyzer, f"   Интент: {'✓' if 'main_intent' in analyzer.df.columns else '✗'}")
        print_stage(analyzer, f"   SERP: {'✓' if 'serp_found_docs' in analyzer.df.columns else '✗'}")
        print_stage(analyzer, f"   SERP URLs: {'✓' if 'serp_top_urls' in analyzer.df.columns else '✗'}")
        print()
        return
    
    # Если загружено из query_cache - пропускаем часть предобработки
    if loaded_from_cache:
        _handle_cached_data(args, analyzer, print_stage)
        return
    
    # Инициализация обработчиков
    filter_handler = FilterHandler(analyzer.stopwords)
    normalization_handler = NormalizationHandler()
    extraction_handler = ExtractionHandler()
    deduplication_handler = DeduplicationHandler()
    
    # Применяем фильтры
    analyzer.df = filter_handler.apply_filters(analyzer.df, args, lambda msg: print_stage(analyzer, msg))
    analyzer.stopwords_filter = filter_handler.stopwords_filter
    
    # Проверка: остались ли запросы после фильтрации
    if len(analyzer.df) == 0:
        print_stage(analyzer, "\n⚠️  Все запросы были отфильтрованы!")
        print_stage(analyzer, "⚠️  Попробуйте:")
        print_stage(analyzer, "    - Уменьшить max_frequency_ratio (--max-frequency-ratio)")
        print_stage(analyzer, "    - Использовать другой CSV файл с более качественными запросами")
        print()
        
        # Создаем пустые колонки чтобы не было ошибок в последующих этапах
        for col in ['normalized', 'lemmatized', 'words_count', 'has_latin', 'has_numbers',
                    'main_words', 'key_phrase', 'ner_entities', 'ner_locations']:
            if col not in analyzer.df.columns:
                analyzer.df[col] = []
        return
    
    # Нормализация
    queries_list = analyzer.df['keyword'].tolist()
    normalized_results = await normalization_handler.normalize_queries(queries_list, lambda msg: print_stage(analyzer, msg))
    analyzer.df = normalization_handler.apply_normalization_to_df(analyzer.df, normalized_results)
    analyzer.normalizer = normalization_handler.normalizer
    
    # Извлечение ключевых фраз и NER
    analyzer.df = await extraction_handler.extract_key_phrases(queries_list, analyzer.df, lambda msg: print_stage(analyzer, msg))
    analyzer.df = await extraction_handler.extract_ner(queries_list, analyzer.df, lambda msg: print_stage(analyzer, msg))
    
    # Дедупликация
    analyzer.df, stats = deduplication_handler.deduplicate_exact(analyzer.df, lambda msg: print_stage(analyzer, msg))
    analyzer.deduplicator = deduplication_handler.deduplicator
    
    analyzer.df, adv_stats = deduplication_handler.deduplicate_advanced(analyzer.df, lambda msg: print_stage(analyzer, msg))
    analyzer.removed_implicit_duplicates = deduplication_handler.removed_implicit_duplicates
    analyzer.advanced_deduplicator = deduplication_handler.advanced_deduplicator
    
    # Запуск обработки географии
    _start_geo_processing(analyzer, print_stage)
    
    # Сохранение результатов
    if not getattr(analyzer, 'loaded_from_cache', False):
        CacheHandler.save_filtered_to_csv(args, analyzer, lambda msg: print_stage(analyzer, msg))
    
    # Сохранение в кэш
    if hasattr(analyzer, 'query_cache') and hasattr(analyzer, 'current_group') and analyzer.current_group:
        if not getattr(analyzer, 'loaded_from_cache', False):
            total_duplicates = stats['total_duplicates_removed'] + adv_stats['total_duplicates_removed']
            CacheHandler.save_to_cache(analyzer, total_duplicates, lambda msg: print_stage(analyzer, msg))
            
            # Debug: проверяем что колонки созданы
            print_stage(analyzer, f"🔍 DEBUG: Колонки на выходе: {list(analyzer.df.columns)[:15]}...")
            if 'lemmatized' in analyzer.df.columns and 'normalized' in analyzer.df.columns:
                print_stage(analyzer, f"✅ DEBUG: Колонки lemmatized и normalized созданы успешно")
            else:
                print_stage(analyzer, f"❌ DEBUG: ОШИБКА! Колонки lemmatized/normalized НЕ созданы!")
        else:
            print_stage(analyzer, f"\n⚡ Данные загружены из кэша - предобработка пропущена")
    
    print()


def _handle_cached_data(args, analyzer, print_stage):
    """Обработка данных загруженных из кэша"""
    print_stage(analyzer, "⚡ Данные загружены из кэша")
    print_stage(analyzer, "✓ Предобработка пропущена (normalized, lemmatized, NER, key phrases уже в кэше)")
    print_stage(analyzer, f"✓ Уникальных запросов: {len(analyzer.df)}")
    
    if hasattr(analyzer, 'query_cache') and hasattr(analyzer, 'current_group'):
        stats = analyzer.query_cache.get_group_stats(analyzer.current_group.name)
        if stats:
            print_stage(analyzer, f"✓ Дубликатов удалено при импорте: {stats['duplicates_removed']}")
    
    # Запускаем обработку географии
    print_stage(analyzer, "\n🌍 Запуск обработки географии запросов в фоновом режиме...")
    _start_geo_processing(analyzer, print_stage)
    
    # Проверяем критичные колонки
    missing_critical = []
    if 'normalized' not in analyzer.df.columns:
        missing_critical.append('normalized')
    if 'lemmatized' not in analyzer.df.columns:
        missing_critical.append('lemmatized')
    
    if missing_critical:
        print_stage(analyzer, f"\n⚠️  Критичные колонки отсутствуют: {', '.join(missing_critical)}")
        print_stage(analyzer, "🔄 Создание отсутствующих колонок...")
        
        normalizer = QueryNormalizer()
        normalized_results = normalizer.normalize_batch(analyzer.df['keyword'].tolist())
        
        if 'normalized' not in analyzer.df.columns:
            analyzer.df['normalized'] = [r['normalized'] for r in normalized_results]
        if 'lemmatized' not in analyzer.df.columns:
            analyzer.df['lemmatized'] = [r['lemmatized'] for r in normalized_results]
        
        print_stage(analyzer, "✓ Колонки созданы")
        
        # Пересохраняем кэш
        if hasattr(analyzer, 'query_cache') and hasattr(analyzer, 'current_group'):
            print_stage(analyzer, "💾 Обновление кэша...")
            analyzer.query_cache.save_queries(
                group_name=analyzer.current_group.name,
                csv_path=analyzer.current_group.input_file,
                df=analyzer.df,
                duplicates_removed=stats.get('duplicates_removed', 0) if stats else 0
            )
            print_stage(analyzer, "✓ Кэш обновлен")
    
    # Синхронизация CSV
    CacheHandler.sync_csv_from_cache_if_needed(args, analyzer, lambda msg: print_stage(analyzer, msg))
    print()


def _start_geo_processing(analyzer, print_stage):
    """Запустить обработку географии в фоновом режиме"""
    from seo_analyzer.clustering.semantic_checker import SemanticClusterChecker
    
    semantic_checker = SemanticClusterChecker(geo_dicts=analyzer.geo_dicts)
    analyzer.geo_processor = AsyncGeoProcessor(
        semantic_checker=semantic_checker,
        max_workers=4
    )
    
    queries_list = analyzer.df['keyword'].tolist()
    analyzer.geo_processor.start_processing(queries_list)
    
    print_stage(analyzer, f"✓ Обработка {len(queries_list)} запросов запущена (параллельно с другими этапами)")
    print_stage(analyzer, "  💡 География будет готова к моменту кластеризации")


# Для обратной совместимости
def _sync_csv_from_cache_if_needed(args, analyzer, print_stage):
    """Синхронизирует CSV файл с данными из кэша если это требуется"""
    CacheHandler.sync_csv_from_cache_if_needed(args, analyzer, lambda msg: print_stage(analyzer, msg))
