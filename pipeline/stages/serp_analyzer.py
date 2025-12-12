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
    
    # Получаем прокси из args или config_local.py
    serp_proxies = getattr(args, 'serp_proxies', None)
    serp_proxy_file = getattr(args, 'serp_proxy_file', None)
    
    # Если прокси не указаны в args, пробуем config_local.py
    if not serp_proxies and not serp_proxy_file:
        try:
            import config_local
            serp_proxies = getattr(config_local, 'SERP_PROXIES', None)
            serp_proxy_file = getattr(config_local, 'SERP_PROXY_FILE', None)
            if serp_proxies or serp_proxy_file:
                print("✓ Прокси загружены из config_local.py")
        except ImportError:
            pass
    
    # Если прокси все еще не указаны, пробуем загрузить из socks_working.txt по умолчанию
    if not serp_proxies and not serp_proxy_file:
        from pathlib import Path
        default_proxy_file = Path('socks_working.txt')
        if default_proxy_file.exists():
            serp_proxy_file = str(default_proxy_file)
            print(f"✓ Автоматически загружен файл прокси: {serp_proxy_file}")
    
    # Если прокси указаны как строка (через запятую), преобразуем в список
    if isinstance(serp_proxies, str):
        serp_proxies = [p.strip() for p in serp_proxies.split(',') if p.strip()]
    
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
        site=serp_site,
        proxies=serp_proxies,
        proxy_file=serp_proxy_file
    )
    
    # Показываем информацию о прокси
    if serp_proxies or serp_proxy_file:
        proxy_count = len(serp_proxies) if serp_proxies else 0
        if serp_proxy_file:
            try:
                from pathlib import Path
                proxy_path = Path(serp_proxy_file)
                if proxy_path.exists():
                    with open(proxy_path, 'r', encoding='utf-8') as f:
                        file_count = len([line.strip() for line in f if line.strip() and not line.strip().startswith('#')])
                    proxy_count += file_count
            except:
                pass
        print_stage(analyzer, f"🌐 Используется {proxy_count} прокси для ротации IP")
    
    if use_batch_async:
        print_stage(analyzer, "🚀 Режим: BATCH ASYNC (массовая отправка → параллельное получение)")
    
    # Получаем список запросов
    all_queries = analyzer.df['keyword'].tolist()
    
    # ВАЖНО: Проверяем наличие реальных данных в serp_top_urls
    # Если данные загружены из БД, но serp_top_urls пустые - нужно собрать заново
    queries_without_urls = []
    if 'serp_top_urls' in analyzer.df.columns:
        for idx, row in analyzer.df.iterrows():
            keyword = row.get('keyword')
            serp_top_urls = row.get('serp_top_urls')
            
            # Проверяем что serp_top_urls не пустой
            has_urls = False
            if serp_top_urls is not None:
                if isinstance(serp_top_urls, list):
                    has_urls = len(serp_top_urls) > 0
                elif isinstance(serp_top_urls, str):
                    serp_top_urls_str = serp_top_urls.strip()
                    if serp_top_urls_str and serp_top_urls_str not in ('', '[]', 'null', 'NULL', 'None'):
                        try:
                            import json
                            parsed = json.loads(serp_top_urls_str)
                            has_urls = isinstance(parsed, list) and len(parsed) > 0
                        except:
                            has_urls = False
            
            if not has_urls and keyword:
                queries_without_urls.append(keyword)
        
        if queries_without_urls:
            print_stage(analyzer, f"⚠️  Обнаружено {len(queries_without_urls)} запросов с пустым serp_top_urls")
            print_stage(analyzer, f"   Будет выполнена повторная загрузка через XMLStock API...")
    
    print_stage(analyzer, f"📊 Анализ SERP для {len(all_queries)} запросов (кэш проверяется автоматически)...")
    
    # Прогресс
    def progress_callback(current, total, query, status=None):
        if current % 100 == 0 or current == total:
            status_text = f" {status}" if status else ""
            print_stage(analyzer, f"  [{current}/{total}]{status_text} {query[:60]}...")
    
    # Анализируем пакетом (кэш проверяется внутри - мгновенная загрузка закэшированных)
    # ВАЖНО: Если есть запросы без URL, они будут автоматически загружены через API
    
    # Если есть query_to_group_map (объединенная обработка всех групп), используем его
    query_to_group_map = getattr(analyzer, 'query_to_group_map', None)
    
    serp_results = await analyzer.serp_analyzer.analyze_queries_batch(
        all_queries,
        max_concurrent=SERP_CONFIG['api']['max_concurrent'],
        progress_callback=progress_callback,
        query_to_group_map=query_to_group_map
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
    
    # ВАЖНО: Обновляем serp_top_urls из результатов анализа (включая закэшированные)
    # Это нужно для того, чтобы закэшированные данные тоже попали в DataFrame
    def update_serp_top_urls(query):
        """Обновляет serp_top_urls из результатов SERP анализа"""
        result = serp_dict.get(query, {})
        documents = result.get('documents', [])
        
        if not documents:
            return None  # Возвращаем None чтобы не перезаписать существующие данные
        
        # Если documents - список словарей с полной информацией (title, snippet, url)
        if isinstance(documents, list) and len(documents) > 0:
            if isinstance(documents[0], dict):
                # Возвращаем TOP-20 документов с полной информацией
                return documents[:20]
            elif isinstance(documents[0], str):
                # Если это список URL строк - преобразуем в формат с dict
                return [{'url': url} for url in documents[:20]]
        
        return None
    
    # Обновляем serp_top_urls из результатов анализа
    # Создаем колонку если её нет, или обновляем существующую
    updated_top_urls = analyzer.df['keyword'].map(update_serp_top_urls)
    
    # Обновляем только те записи, где есть новые данные
    if 'serp_top_urls' not in analyzer.df.columns:
        analyzer.df['serp_top_urls'] = updated_top_urls
    else:
        # Обновляем только те, где есть новые данные (не None)
        mask = updated_top_urls.notna()
        analyzer.df.loc[mask, 'serp_top_urls'] = updated_top_urls[mask]
    
    # Домены из SERP (для кластеризации) - ТОП-20
    # Создаем serp_urls из serp_top_urls (который теперь обновлен из результатов анализа)
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
    
    # Используем обновленный serp_top_urls для создания serp_urls
    if 'serp_top_urls' in analyzer.df.columns:
        analyzer.df['serp_urls'] = analyzer.df['serp_top_urls'].apply(extract_urls_from_top_urls)
        print_stage(analyzer, "   ✓ serp_urls созданы из serp_top_urls (обновлено из результатов анализа)")
    else:
        # Если serp_top_urls нет, извлекаем напрямую из serp_dict
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
    # Используем обновленный serp_top_urls (который теперь содержит данные из кэша и API)
    if 'serp_top_urls' in analyzer.df.columns:
        # Данные обновлены из результатов анализа (включая закэшированные)
        analyzer.df['serp_documents'] = analyzer.df['serp_top_urls']
        print_stage(analyzer, "   ✓ serp_documents обновлены из результатов анализа (включая кэш)")
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
        
        # КРИТИЧНО: Проверяем что все запросы имеют заполненный serp_top_urls перед переходом к кластеризации
        if 'serp_top_urls' in analyzer.df.columns:
            queries_without_top_urls = []
            for idx, row in analyzer.df.iterrows():
                keyword = row.get('keyword')
                serp_top_urls = row.get('serp_top_urls')
                
                # Проверяем что serp_top_urls не пустой
                has_urls = False
                if serp_top_urls is not None:
                    if isinstance(serp_top_urls, list):
                        has_urls = len(serp_top_urls) > 0
                    elif isinstance(serp_top_urls, str):
                        serp_top_urls_str = serp_top_urls.strip()
                        if serp_top_urls_str and serp_top_urls_str not in ('', '[]', 'null', 'NULL', 'None'):
                            try:
                                import json
                                parsed = json.loads(serp_top_urls_str)
                                has_urls = isinstance(parsed, list) and len(parsed) > 0
                            except:
                                has_urls = False
                
                if not has_urls and keyword:
                    queries_without_top_urls.append(keyword)
            
            if queries_without_top_urls:
                print_stage(analyzer, "")
                print_stage(analyzer, f"⚠️  КРИТИЧНО: Обнаружено {len(queries_without_top_urls)} запросов БЕЗ serp_top_urls!")
                print_stage(analyzer, f"   Система НЕ перейдет к кластеризации пока все запросы не будут обработаны.")
                print_stage(analyzer, f"   Повторная попытка загрузки через API...")
                
                # Повторно загружаем только те запросы, у которых нет serp_top_urls
                retry_results = await analyzer.serp_analyzer.analyze_queries_batch(
                    queries_without_top_urls,
                    max_concurrent=SERP_CONFIG['api']['max_concurrent'],
                    progress_callback=progress_callback
                )
                
                # Обновляем результаты
                retry_dict = {result['query']: result for result in retry_results}
                for query, result in retry_dict.items():
                    if query in serp_dict:
                        serp_dict[query] = result
                
                # Обновляем serp_top_urls для повторно загруженных запросов
                updated_top_urls_retry = analyzer.df['keyword'].map(update_serp_top_urls)
                mask_retry = updated_top_urls_retry.notna()
                analyzer.df.loc[mask_retry, 'serp_top_urls'] = updated_top_urls_retry[mask_retry]
                
                # Обновляем serp_urls
                analyzer.df['serp_urls'] = analyzer.df['serp_top_urls'].apply(extract_urls_from_top_urls)
                
                # Проверяем результат повторной загрузки
                final_urls_filled = (analyzer.df['serp_urls'].apply(lambda x: isinstance(x, list) and len(x) > 0)).sum()
                final_urls_empty = (analyzer.df['serp_urls'].apply(lambda x: not isinstance(x, list) or len(x) == 0)).sum()
                
                print_stage(analyzer, f"   После повторной загрузки:")
                print_stage(analyzer, f"   ✓ SERP URLs заполнено: {final_urls_filled} запросов ({final_urls_filled/len(analyzer.df)*100:.1f}%)")
                print_stage(analyzer, f"   ⚠️  SERP URLs пусто: {final_urls_empty} запросов ({final_urls_empty/len(analyzer.df)*100:.1f}%)")
                
                if final_urls_empty > 0:
                    print_stage(analyzer, f"   ⚠️  ВНИМАНИЕ: {final_urls_empty} запросов все еще без SERP данных!")
                    print_stage(analyzer, f"   Кластеризация будет выполнена только для запросов с данными.")
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

