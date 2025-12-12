"""Этап 4: SERP анализ через xmlstock"""

import os
from seo_analyzer.analysis.serp_analyzer import SERPAnalyzer
from seo_analyzer.core.config import SERP_CONFIG
from .stage_logger import get_group_prefix, print_stage



def get_api_key(args):
    """
    Получает API ключ из разных источников
    
    Args:
        args: Аргументы командной строки
        
    Returns:
        API ключ или None
    """
    api_key = args.xmlstock_api_key
    
    # Если нет в аргументах, пробуем config_local.py
    if not api_key:
        try:
            import config_local
            api_key = getattr(config_local, 'XMLSTOCK_API_KEY', None)
            if api_key:
                print("✓ API ключ загружен из config_local.py")
        except ImportError:
            pass
    
    # Если нет, пробуем переменную окружения
    if not api_key:
        api_key = os.getenv('XMLSTOCK_API_KEY')
        if api_key:
            print("✓ API ключ загружен из переменной окружения")
    
    return api_key


async def analyze_serp_stage(args, analyzer):
    """
    SERP анализ через xmlstock
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в analyzer)
    """
    prefix = get_group_prefix(analyzer)
    print_stage(analyzer, "🔍 ЭТАП 3: SERP анализ (база для кластеризации и классификации)")
    print_stage(analyzer, "-" * 80)
    
    # Проверяем что DataFrame не пустой
    if len(analyzer.df) == 0:
        print_stage(analyzer, "⚠️  DataFrame пустой, пропускаем SERP анализ")
        print()
        return
    
    # Получаем API ключ
    api_key = get_api_key(args)
    
    if not api_key:
        print_stage(analyzer, "⚠️  API ключ xmlstock не найден!")
        print_stage(analyzer, "   Способ 1: --xmlstock-api-key user:key")
        print_stage(analyzer, "   Способ 2: создайте config_local.py (см. config_local.py.example)")
        print_stage(analyzer, "   Способ 3: export XMLSTOCK_API_KEY=user:key")
        return
    
    # Инициализация анализатора
    print_stage(analyzer, f"🔄 Инициализация SERP анализатора...")
    
    # Определяем группу запросов
    query_group = None
    if hasattr(analyzer, 'current_group') and analyzer.current_group:
        query_group = analyzer.current_group.name
        print_stage(analyzer, f"📁 Группа запросов: {query_group}")
    
    # Проверяем флаг batch_async режима (по умолчанию True)
    use_batch_async = getattr(args, 'serp_batch_async', True)
    
    # Получаем параметры SERP из args
    serp_region = getattr(args, 'serp_region', SERP_CONFIG['api']['lr'])
    serp_device = getattr(args, 'serp_device', 'desktop')
    serp_site = getattr(args, 'serp_site', None)
    
    analyzer.serp_analyzer = SERPAnalyzer(
        api_key=api_key,
        lr=serp_region,
        max_retries=SERP_CONFIG['api']['max_retries'],
        retry_delay=SERP_CONFIG['api']['retry_delay'],
        timeout=SERP_CONFIG['api']['timeout'],
        query_group=query_group,
        max_concurrent=SERP_CONFIG['api']['max_concurrent'],  # Глобальный лимит для всех групп
        use_master_db=True,  # Используем только Master DB
        use_batch_async=use_batch_async,  # 🚀 МАССОВЫЙ ASYNC РЕЖИМ (по умолчанию)
        device=serp_device,
        site=serp_site
    )
    
    if use_batch_async:
        print_stage(analyzer, "🚀 Режим: BATCH ASYNC (массовая отправка → параллельное получение)")
    
    # Получаем список запросов
    all_queries = analyzer.df['keyword'].tolist()
    
    print_stage(analyzer, f"📊 Анализ SERP для {len(all_queries)} запросов (кэш проверяется автоматически)...")
    
    # Прогресс
    def progress_callback(current, total, query, status=None):
        if current % 100 == 0 or current == total:
            status_text = f" {status}" if status else ""
            print_stage(analyzer, f"  [{current}/{total}]{status_text} {query[:60]}...")
    
    # Анализируем пакетом (кэш проверяется внутри - мгновенная загрузка закэшированных)
    serp_results = await analyzer.serp_analyzer.analyze_queries_batch(
        all_queries,
        max_concurrent=SERP_CONFIG['api']['max_concurrent'],
        progress_callback=progress_callback
    )
    
    # Добавляем результаты в DataFrame
    serp_dict = {result['query']: result for result in serp_results}
    
    # SERP метрики
    analyzer.df['serp_docs_count'] = analyzer.df['keyword'].map(
        lambda x: serp_dict.get(x, {}).get('metrics', {}).get('found_docs', 0)
    )
    # Добавляем serp_found_docs для совместимости с БД
    analyzer.df['serp_found_docs'] = analyzer.df['serp_docs_count']
    
    analyzer.df['serp_main_pages'] = analyzer.df['keyword'].map(
        lambda x: serp_dict.get(x, {}).get('metrics', {}).get('main_pages_count', 0)
    )
    # Кол-во главных страниц (для Excel)
    analyzer.df['serp_main_pages_count'] = analyzer.df['serp_main_pages']
    # Кол-во внутренних страниц (общее кол-во документов минус главные)
    analyzer.df['serp_internal_pages_count'] = (
        analyzer.df['serp_docs_count'] - analyzer.df['serp_main_pages']
    ).clip(lower=0)  # Не допускаем отрицательных значений
    
    analyzer.df['serp_titles_count'] = analyzer.df['keyword'].map(
        lambda x: serp_dict.get(x, {}).get('metrics', {}).get('titles_with_keyword', 0)
    )
    # Добавляем serp_titles_with_keyword для совместимости с БД
    analyzer.df['serp_titles_with_keyword'] = analyzer.df['serp_titles_count']
    
    analyzer.df['serp_commercial_domains'] = analyzer.df['keyword'].map(
        lambda x: serp_dict.get(x, {}).get('metrics', {}).get('commercial_domains', 0)
    )
    analyzer.df['serp_info_domains'] = analyzer.df['keyword'].map(
        lambda x: serp_dict.get(x, {}).get('metrics', {}).get('info_domains', 0)
    )
    
    # LSI фразы
    analyzer.df['lsi_phrases'] = analyzer.df['keyword'].map(
        lambda x: serp_dict.get(x, {}).get('lsi_phrases', [])
    )
    
    # Домены из SERP (для кластеризации) - ТОП-20
    # ВАЖНО: Сначала проверяем, есть ли serp_top_urls из Master DB
    if 'serp_top_urls' in analyzer.df.columns:
        # Данные уже загружены из Master DB - создаем serp_urls из serp_top_urls
        def extract_urls_from_top_urls(serp_top_urls):
            """Извлекает список URL из serp_top_urls для кластеризации"""
            if not serp_top_urls or not isinstance(serp_top_urls, list):
                return []
            
            urls = []
            for item in serp_top_urls[:20]:  # TOP-20
                if isinstance(item, dict):
                    url = item.get('url', '')
                elif isinstance(item, str):
                    url = item
                else:
                    continue
                
                if url:
                    urls.append(url)
            
            return urls
        
        analyzer.df['serp_urls'] = analyzer.df['serp_top_urls'].apply(extract_urls_from_top_urls)
        print_stage(analyzer, "   ✓ serp_urls созданы из serp_top_urls (Master DB)")
    else:
        # Данных из Master DB нет - извлекаем из serp_dict
        def extract_domains(query):
            result = serp_dict.get(query, {})
            documents = result.get('documents', [])
            if not documents:
                return []
            
            # Если documents - строка (JSON из кэша), парсим её
            if isinstance(documents, str):
                import json
                try:
                    documents = json.loads(documents)
                except:
                    return []
            
            # Если documents - список словарей
            if isinstance(documents, list) and len(documents) > 0:
                # Проверяем тип первого элемента
                if isinstance(documents[0], dict):
                    # Берём только TOP-20 URL из документов (даже если их больше)
                    return [doc.get('url', '') for doc in documents[:20] if doc.get('url')]
                elif isinstance(documents[0], str):
                    # Если это уже список URL
                    return documents[:20]
            
            return []
        
        analyzer.df['serp_urls'] = analyzer.df['keyword'].map(extract_domains)
    
    # Полные данные документов SERP (для экспорта с title, snippet и URL)
    # Проверяем: если serp_top_urls уже есть (из Master DB), используем его
    if 'serp_top_urls' in analyzer.df.columns:
        # Данные уже загружены из Master DB и нормализованы
        analyzer.df['serp_documents'] = analyzer.df['serp_top_urls']
        print_stage(analyzer, "   ✓ serp_documents загружены из Master DB (с title и snippet)")
    else:
        # Извлекаем из свежих данных API
        def extract_documents(query):
            result = serp_dict.get(query, {})
            documents = result.get('documents', [])
            if not documents:
                return []
            
            # Если documents - строка (JSON из кэша), парсим её
            if isinstance(documents, str):
                import json
                try:
                    documents = json.loads(documents)
                except:
                    return []
            
            # Если documents - список словарей
            if isinstance(documents, list) and len(documents) > 0:
                # Проверяем тип первого элемента
                if isinstance(documents[0], dict):
                    # Берём TOP-20 документов с полной информацией
                    return documents[:20]
            
            return []
        
        analyzer.df['serp_documents'] = analyzer.df['keyword'].map(extract_documents)
        print_stage(analyzer, "   ✓ serp_documents извлечены из SERP API (с title и snippet)")
    
    # XML ответы (для классификации по offer_info)
    analyzer.df['xml_response'] = analyzer.df['keyword'].map(
        lambda x: serp_dict.get(x, {}).get('xml_response', '')
    )
    
    # SERP статус и req_id (если есть в результатах)
    # Эти поля нужны для отслеживания статуса загрузки в БД
    analyzer.df['serp_status'] = analyzer.df['keyword'].map(
        lambda x: serp_dict.get(x, {}).get('status', 'completed')
    )
    analyzer.df['serp_req_id'] = analyzer.df['keyword'].map(
        lambda x: serp_dict.get(x, {}).get('req_id')
    )
    
    # Отладочная информация
    urls_filled = (analyzer.df['serp_urls'].apply(lambda x: isinstance(x, list) and len(x) > 0)).sum()
    urls_empty = (analyzer.df['serp_urls'].apply(lambda x: not isinstance(x, list) or len(x) == 0)).sum()
    
    if len(analyzer.df) > 0:
        print_stage(analyzer, f"  SERP URLs заполнено: {urls_filled} запросов ({urls_filled/len(analyzer.df)*100:.1f}%)")
        print_stage(analyzer, f"  SERP URLs пусто: {urls_empty} запросов ({urls_empty/len(analyzer.df)*100:.1f}%)")
    else:
        print_stage(analyzer, f"  ⚠️  DataFrame пустой после фильтрации - нет запросов для анализа")
    
    # Заполняем нули для не проанализированных
    for col in ['serp_docs_count', 'serp_main_pages', 'serp_main_pages_count', 
                'serp_internal_pages_count', 'serp_titles_count', 
                'serp_commercial_domains', 'serp_info_domains']:
        analyzer.df[col] = analyzer.df[col].fillna(0).astype(int)
    
    # Статистика
    stats = analyzer.serp_analyzer.get_statistics()
    print_stage(analyzer, f"✓ SERP анализ завершен:")
    print_stage(analyzer, f"  Всего запросов: {stats['total_queries']}")
    print_stage(analyzer, f"  Из кэша (Master DB): {stats['cached_from_master']}")
    print_stage(analyzer, f"  API запросов: {stats['api_requests']}")
    print_stage(analyzer, f"  Ошибок: {stats['errors']}")
    if 'cache_hit_rate' in stats:
        print_stage(analyzer, f"  Cache hit rate: {stats['cache_hit_rate']:.1f}%")
    
    # Проверяем результаты
    queries_with_serp = len(analyzer.df[analyzer.df['serp_docs_count'] > 0])
    if queries_with_serp > 0:
        print_stage(analyzer, f"✓ Получены SERP данные для {queries_with_serp} запросов")
        avg_docs = analyzer.df[analyzer.df['serp_docs_count'] > 0]['serp_docs_count'].mean()
        median_docs = analyzer.df[analyzer.df['serp_docs_count'] > 0]['serp_docs_count'].median()
        print_stage(analyzer, f"  Среднее документов на запрос: {int(avg_docs):,}")
        print_stage(analyzer, f"  Медиана документов на запрос: {int(median_docs):,}")
    else:
        print_stage(analyzer, f"⚠️  ВНИМАНИЕ: Не получено ни одного результата SERP!")
        print_stage(analyzer, f"   Проверьте API ключ и настройки xmlstock")
    
    # Автоматическое восстановление данных для запросов со статусом completed, но без URL/LSI
    if query_group:
        print_stage(analyzer, "")
        print_stage(analyzer, "🔄 Проверка и восстановление недостающих данных...")
        try:
            updated_count = await analyzer.serp_analyzer.recover_missing_lsi_from_urls(group_name=query_group)
            if updated_count > 0:
                print_stage(analyzer, f"✓ Восстановлено данных для {updated_count} запросов")
                # Перезагружаем данные из Master DB для обновлённых запросов
                print_stage(analyzer, "🔄 Обновление данных в DataFrame...")
                # Обновляем только те запросы, которые были восстановлены
                for idx, row in analyzer.df.iterrows():
                    keyword = row['keyword']
                    # Проверяем обновлённые данные в Master DB
                    if analyzer.serp_analyzer.master_db:
                        updated_data = analyzer.serp_analyzer._get_from_master_db(keyword)
                        if updated_data and updated_data.get('documents'):
                            # Обновляем LSI фразы
                            if updated_data.get('lsi_phrases'):
                                analyzer.df.at[idx, 'lsi_phrases'] = updated_data['lsi_phrases']
                            # Обновляем URL
                            if updated_data.get('documents'):
                                analyzer.df.at[idx, 'serp_urls'] = [
                                    doc.get('url', '') for doc in updated_data['documents'][:20] 
                                    if doc.get('url')
                                ]
            else:
                print_stage(analyzer, "✓ Все запросы имеют необходимые данные")
        except Exception as e:
            print_stage(analyzer, f"⚠️  Ошибка при восстановлении данных: {e}")
            # Не критично - продолжаем работу
    
    print()

