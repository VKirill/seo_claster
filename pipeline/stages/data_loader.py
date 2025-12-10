"""Этап 1: Загрузка данных"""

from pathlib import Path
from seo_analyzer.core.helpers import load_all_data, load_csv_data, normalize_dataframe_columns, load_intent_weights
from seo_analyzer.core.query_groups import QueryGroupManager
from .stage_logger import get_group_prefix, print_stage


async def load_data_stage(args, analyzer):
    """
    Загрузка всех данных
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в analyzer)
    """
    prefix = get_group_prefix(analyzer)
    print(f"{prefix}📚 ЭТАП 1: Загрузка данных")
    print(f"{prefix}{'-' * 80}")
    
    # Загружаем словари
    analyzer.keyword_dicts, analyzer.geo_dicts, analyzer.stopwords = await load_all_data()
    
    # Загружаем веса интентов
    analyzer.intent_weights = await load_intent_weights()
    
    # Инициализация менеджера групп
    group_manager = QueryGroupManager()
    group_manager.discover_groups()
    
    # Проверяем: работа с группами или обычный режим
    if hasattr(args, 'group') and args.group:
        # Режим работы с группой
        print_stage(analyzer, f"📁 Работа с группой: {args.group}")
        group = group_manager.get_group(args.group)
        
        if not group:
            raise ValueError(f"Группа '{args.group}' не найдена в semantika/")
        
        analyzer.current_group = group
        analyzer.group_manager = group_manager
        
        print_stage(analyzer, f"✓ Группа: {group.name}")
        print_stage(analyzer, f"✓ Файл: {group.input_file}")
        print_stage(analyzer, f"✓ Output: {group.output_dir}")
        
        # Проверяем кэш в Master DB (единственный источник данных)
        if not getattr(args, 'force_refresh', False):
            # Пробуем загрузить из Master DB
            try:
                from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
                master_db = MasterQueryDatabase()
                
                if master_db.group_exists(group.name):
                    # Получаем статистику из Master DB один раз
                    master_db_stats = master_db.get_statistics(group.name)
                    master_db_queries_count = master_db_stats['total_queries']
                    
                    # Проверяем количество запросов в CSV перед загрузкой из Master DB
                    csv_has_more_queries = False
                    
                    if group.input_file and group.input_file.exists():
                        # Быстро загружаем CSV только для подсчёта запросов
                        import pandas as pd
                        import asyncio
                        
                        def count_csv_queries(csv_path):
                            """Быстро подсчитывает количество запросов в CSV"""
                            try:
                                # Пробуем разные кодировки и разделители
                                for encoding in ['utf-8-sig', 'utf-8', 'cp1251', 'windows-1251']:
                                    for delimiter in [',', ';', '\t']:
                                        try:
                                            df = pd.read_csv(csv_path, delimiter=delimiter, encoding=encoding, nrows=0)
                                            if len(df.columns) > 1:
                                                # Определили формат, теперь читаем полностью
                                                df = pd.read_csv(csv_path, delimiter=delimiter, encoding=encoding)
                                                # Определяем колонку с запросами (обычно 'keyword' или первая)
                                                keyword_col = 'keyword' if 'keyword' in df.columns else df.columns[0]
                                                # Считаем уникальные запросы
                                                return df[keyword_col].nunique() if keyword_col in df.columns else len(df)
                                        except (UnicodeDecodeError, pd.errors.ParserError):
                                            continue
                                return 0
                            except Exception:
                                return 0
                        
                        csv_queries_count = await asyncio.to_thread(count_csv_queries, group.input_file)
                        
                        if csv_queries_count > master_db_queries_count:
                            csv_has_more_queries = True
                            print_stage(analyzer, f"🔄 В CSV файле больше запросов ({csv_queries_count} > {master_db_queries_count}), загрузка из CSV...")
                    
                    if not csv_has_more_queries:
                        print_stage(analyzer, f"🚀 Загрузка из Master DB (все данные включая SERP + интент)...")
                        master_df = master_db.load_queries(group.name, include_serp_urls=True)
                        
                        if master_df is not None and not master_df.empty:
                            # Используем уже полученную статистику
                            stats = master_db_stats
                            
                            # Проверяем что в Master DB есть достаточно данных
                            total = stats['total_queries']
                            has_enough_data = (stats['with_intent'] > total * 0.5) or (stats['with_serp'] > total * 0.5)
                            
                            if has_enough_data:
                                # Данные в Master DB полные - используем их
                                raw_df = master_df
                                analyzer.loaded_from_cache = True
                                analyzer.loaded_from_master_db = True
                                
                                print_stage(analyzer, f"  ✓ Запросов: {total:,}")
                                print_stage(analyzer, f"  ✓ С интентом: {stats['with_intent']:,}")
                                print_stage(analyzer, f"  ✓ С SERP: {stats['with_serp']:,}")
                                print_stage(analyzer, f"  💡 Можно сразу экспериментировать с кластеризацией!")
                            else:
                                # Master DB пустая или неполная - загружаем из CSV
                                print_stage(analyzer, f"⚠️  Master DB неполная (интент: {stats['with_intent']}/{total}, SERP: {stats['with_serp']}/{total})")
                                print_stage(analyzer, f"🔄 Загрузка из CSV...")
                                import asyncio
                                raw_df = await asyncio.to_thread(group_manager.load_queries, group)
                                analyzer.loaded_from_cache = False
                                analyzer.loaded_from_master_db = False
                        else:
                            # Master DB вернул пустой DataFrame - загружаем из CSV
                            print_stage(analyzer, f"⚠️  Master DB вернул пустой DataFrame, загрузка из CSV...")
                            import asyncio
                            raw_df = await asyncio.to_thread(group_manager.load_queries, group)
                            analyzer.loaded_from_cache = False
                            analyzer.loaded_from_master_db = False
                    else:
                        # В CSV больше запросов - загружаем из CSV
                        import asyncio
                        raw_df = await asyncio.to_thread(group_manager.load_queries, group)
                        analyzer.loaded_from_cache = False
                        analyzer.loaded_from_master_db = False
                else:
                    # Группы нет в Master DB - загружаем из CSV
                    print_stage(analyzer, f"🔄 Группа не найдена в Master DB, загрузка из CSV...")
                    import asyncio
                    raw_df = await asyncio.to_thread(group_manager.load_queries, group)
                    analyzer.loaded_from_cache = False
                    analyzer.loaded_from_master_db = False
                    
            except Exception as e:
                # Master DB недоступен - загружаем из CSV
                print_stage(analyzer, f"⚠️  Master DB недоступен ({e}), загрузка из CSV...")
                import asyncio
                raw_df = await asyncio.to_thread(group_manager.load_queries, group)
                analyzer.loaded_from_cache = False
                analyzer.loaded_from_master_db = False
        else:
            # Принудительное обновление - загружаем из CSV
            print_stage(analyzer, f"🔄 Принудительное обновление, загрузка из CSV...")
            import asyncio
            raw_df = await asyncio.to_thread(group_manager.load_queries, group)
            analyzer.loaded_from_cache = False
            analyzer.loaded_from_master_db = False
        
    elif args.input_file:
        # Обычный режим (один файл по пути)
        csv_path = Path(args.input_file)
        if not csv_path.is_absolute():
            csv_path = Path.cwd() / csv_path
        
        # Читаем CSV в отдельном потоке чтобы не блокировать event loop
        import asyncio
        raw_df = await asyncio.to_thread(load_csv_data, csv_path)
        
        analyzer.current_group = None
        analyzer.group_manager = None
    else:
        raise ValueError("Не указан файл для обработки")
    
    if raw_df.empty:
        raise ValueError("Не удалось загрузить данные из CSV")
    
    # Нормализуем колонки ТОЛЬКО если данные загружены из CSV
    # Если из кэша - там уже все колонки нормализованы и готовы к использованию
    if getattr(analyzer, 'loaded_from_cache', False):
        # Данные из кэша уже содержат все нужные колонки
        analyzer.df = raw_df
        print_stage(analyzer, f"✓ Загружено {len(analyzer.df)} запросов (с колонками из кэша)")
    else:
        # Данные из CSV - нужно нормализовать колонки
        analyzer.df = normalize_dataframe_columns(raw_df)
        print_stage(analyzer, f"✓ Загружено {len(analyzer.df)} запросов")
    
    print_stage(analyzer, f"🔍 DEBUG data_loader: Колонки после загрузки = {list(analyzer.df.columns)[:10]}...")
    print_stage(analyzer, f"🔍 DEBUG data_loader: loaded_from_cache = {getattr(analyzer, 'loaded_from_cache', 'не установлено')}")
    print()

