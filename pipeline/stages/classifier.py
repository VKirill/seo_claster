"""Этап 3: Классификация запросов"""

import pandas as pd
from seo_analyzer.classification.intent_classifier import IntentClassifier
from seo_analyzer.classification.brand_detector import BrandDetector
from seo_analyzer.classification.funnel_classifier import FunnelClassifier
from seo_analyzer.clustering.structure_clusterer import StructureClusterer
from seo_analyzer.classification.page_mapper import PageMapper
from seo_analyzer.classification.intent.serp_offer_classifier import SERPOfferClassifier
from .stage_logger import get_group_prefix, print_stage, print_stage_header


async def classification_stage(args, analyzer):
    """
    Классификация запросов
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в analyzer)
    """
    prefix = get_group_prefix(analyzer)
    print(f"{prefix}🏷️  ЭТАП 6: Классификация (с учетом SERP + кластеров)")
    print(f"{prefix}{'-' * 80}")
    
    # Проверяем что DataFrame не пустой
    if len(analyzer.df) == 0:
        print_stage(analyzer, "⚠️  DataFrame пустой, пропускаем классификацию")
        print()
        return
    
    # ВСЕГДА выполняем классификацию (даже если данные из Master DB)
    # Это нужно для исправления неправильных интентов и применения новых правил
    # Убрана проверка на пропуск классификации при загрузке из Master DB
    # loaded_from_master_db = getattr(analyzer, 'loaded_from_master_db', False)
    # if loaded_from_master_db and 'main_intent' in analyzer.df.columns:
    #     # Проверяем что классификация действительно заполнена
    #     total = len(analyzer.df)
    #     with_intent = analyzer.df['main_intent'].notna().sum()
    #     
    #     # Если больше 50% запросов классифицированы - используем из кэша
    #     if with_intent > total * 0.5:
    #         print_stage(analyzer, "✅ Классификация из Master DB (уже готово)")
    #         
    #         with_brands = analyzer.df['detected_brand'].notna().sum() if 'detected_brand' in analyzer.df.columns else 0
    #         with_funnel = analyzer.df['funnel_stage'].notna().sum() if 'funnel_stage' in analyzer.df.columns else 0
    #         
    #         print_stage(analyzer, f"  ✓ Интент: {with_intent}/{total}")
    #         print_stage(analyzer, f"  ✓ Бренды: {with_brands}/{total}")
    #         print_stage(analyzer, f"  ✓ Воронка: {with_funnel}/{total}")
    #         print()
    #         return
    #     else:
    #         print_stage(analyzer, f"⚠️  Классификация из Master DB неполная ({with_intent}/{total}), перезапускаем...")
    
    # Проверяем наличие нужных колонок и создаем их если отсутствуют
    # (могут отсутствовать если данные загружены из старого кэша или напрямую из БД)
    missing_columns = []
    if 'lemmatized' not in analyzer.df.columns:
        missing_columns.append('lemmatized')
    if 'normalized' not in analyzer.df.columns:
        missing_columns.append('normalized')
    
    if missing_columns:
        # Создаем обе колонки одним проходом (эффективнее)
        from seo_analyzer.core.normalizer import QueryNormalizer
        normalizer = QueryNormalizer()
        print_stage(analyzer, f"🔄 Нормализация запросов ({', '.join(missing_columns)} отсутствовали)...")
        
        normalized_results = normalizer.normalize_batch(analyzer.df['keyword'].tolist())
        
        if 'normalized' not in analyzer.df.columns:
            analyzer.df['normalized'] = [r['normalized'] for r in normalized_results]
        if 'lemmatized' not in analyzer.df.columns:
            analyzer.df['lemmatized'] = [r['lemmatized'] for r in normalized_results]
        
        print_stage(analyzer, "✓ Нормализация завершена")
    
    # Классификация интента (с использованием кэша)
    intent_columns = ['main_intent', 'commercial_score', 'informational_score', 'navigational_score']
    has_cached_intent = all(col in analyzer.df.columns and analyzer.df[col].notna().any() for col in intent_columns)
    
    if has_cached_intent:
        # Проверяем сколько запросов уже классифицированы
        cached_count = analyzer.df['main_intent'].notna().sum()
        total_count = len(analyzer.df)
        
        if cached_count == total_count:
            print_stage(analyzer, f"✓ Интент загружен из кэша ({cached_count} запросов)")
            # Восстанавливаем intent_results из DataFrame для дальнейшего использования
            # (нужно для флагов и гео-информации)
            intent_results = []
            for idx in analyzer.df.index:
                result = {
                    'main_intent': analyzer.df.at[idx, 'main_intent'],
                    'commercial_score': analyzer.df.at[idx, 'commercial_score'],
                    'informational_score': analyzer.df.at[idx, 'informational_score'],
                    'navigational_score': analyzer.df.at[idx, 'navigational_score'],
                    'has_geo': analyzer.df.at[idx, 'has_geo'] if 'has_geo' in analyzer.df.columns else False,
                    'geo_type': analyzer.df.at[idx, 'geo_type'] if 'geo_type' in analyzer.df.columns else None,
                    'geo_country': analyzer.df.at[idx, 'geo_country'] if 'geo_country' in analyzer.df.columns else None,
                    'geo_city': analyzer.df.at[idx, 'geo_city'] if 'geo_city' in analyzer.df.columns else None,
                }
                # Добавляем флаги из keyword_dicts
                for dict_key in analyzer.keyword_dicts.keys():
                    flag_name = analyzer.keyword_dicts[dict_key].get('flag')
                    if flag_name:
                        result[flag_name] = analyzer.df.at[idx, flag_name] if flag_name in analyzer.df.columns else False
                intent_results.append(result)
        else:
            print_stage(analyzer, f"🔄 Классификация интента ({cached_count}/{total_count} из кэша)...")
            
            # Классифицируем только те, у которых нет интента
            analyzer.intent_classifier = IntentClassifier(
                analyzer.keyword_dicts, 
                analyzer.geo_dicts,
                analyzer.intent_weights
            )
            
            # Находим индексы без интента
            missing_intent = analyzer.df['main_intent'].isna()
            if missing_intent.any():
                queries_to_classify = analyzer.df.loc[missing_intent, 'keyword'].tolist()
                lemmatized_queries = None
                if 'lemmatized' in analyzer.df.columns:
                    lemmatized_queries = analyzer.df.loc[missing_intent, 'lemmatized'].tolist()
                
                new_intent_results = analyzer.intent_classifier.classify_batch(
                    queries_to_classify,
                    lemmatized_queries=lemmatized_queries
                )
                
                # Обновляем только новые записи
                for i, idx in enumerate(analyzer.df[missing_intent].index):
                    for key in intent_columns:
                        analyzer.df.at[idx, key] = new_intent_results[i][key]
                
                print_stage(analyzer, f"✓ Интент классифицирован ({len(queries_to_classify)} новых)")
            
            # Восстанавливаем intent_results из DataFrame (объединяем кэш + новые)
            intent_results = []
            for idx in analyzer.df.index:
                result = {
                    'main_intent': analyzer.df.at[idx, 'main_intent'],
                    'commercial_score': analyzer.df.at[idx, 'commercial_score'],
                    'informational_score': analyzer.df.at[idx, 'informational_score'],
                    'navigational_score': analyzer.df.at[idx, 'navigational_score'],
                    'has_geo': analyzer.df.at[idx, 'has_geo'] if 'has_geo' in analyzer.df.columns else False,
                    'geo_type': analyzer.df.at[idx, 'geo_type'] if 'geo_type' in analyzer.df.columns else None,
                    'geo_country': analyzer.df.at[idx, 'geo_country'] if 'geo_country' in analyzer.df.columns else None,
                    'geo_city': analyzer.df.at[idx, 'geo_city'] if 'geo_city' in analyzer.df.columns else None,
                }
                # Добавляем флаги из keyword_dicts
                for dict_key in analyzer.keyword_dicts.keys():
                    flag_name = analyzer.keyword_dicts[dict_key].get('flag')
                    if flag_name:
                        result[flag_name] = analyzer.df.at[idx, flag_name] if flag_name in analyzer.df.columns else False
                intent_results.append(result)
    else:
        # Кэша нет - классифицируем все
        print_stage(analyzer, "🔄 Классификация интента...")
        analyzer.intent_classifier = IntentClassifier(
            analyzer.keyword_dicts, 
            analyzer.geo_dicts,
            analyzer.intent_weights
        )
        
        # Используем лемматизированные запросы для точного поиска городов
        lemmatized_queries = None
        if 'lemmatized' in analyzer.df.columns:
            lemmatized_queries = analyzer.df['lemmatized'].tolist()
        
        intent_results = analyzer.intent_classifier.classify_batch(
            analyzer.df['keyword'].tolist(),
            lemmatized_queries=lemmatized_queries
        )
        
        for key in intent_columns:
            analyzer.df[key] = [r[key] for r in intent_results]
        
        print_stage(analyzer, f"✓ Интент классифицирован")
    
    # Флаги
    for key in analyzer.keyword_dicts.keys():
        flag_name = analyzer.keyword_dicts[key].get('flag')
        if flag_name:
            analyzer.df[flag_name] = [r.get(flag_name, False) for r in intent_results]
    
    # Гео-информация
    for key in ['has_geo', 'geo_type', 'geo_country', 'geo_city']:
        analyzer.df[key] = [r.get(key) for r in intent_results]
    
    # Нормализуем названия городов для красивого экспорта
    # (спб → Санкт-Петербург, мск → Москва, москва → Москва)
    from seo_analyzer.core.city_alias_loader import normalize_city_name
    analyzer.df['geo_city'] = analyzer.df['geo_city'].apply(
        lambda city: normalize_city_name(city) if city else city
    )
    
    print_stage(analyzer, f"✓ Интент классифицирован")
    
    # Счетчик изменений интентов (для обновления БД в конце)
    changed_count = 0
    
    # Корректировка интента по SERP offer_info (ПРИОРИТЕТНЫЙ метод)
    if 'serp_xml' in analyzer.df.columns or 'xml_response' in analyzer.df.columns:
        print_stage(analyzer, "🔄 Корректировка интента по SERP offer_info...")
        
        # Определяем колонку с XML
        xml_column = 'serp_xml' if 'serp_xml' in analyzer.df.columns else 'xml_response'
        
        # Подсчитываем сколько запросов имеют XML данные
        has_xml = analyzer.df[xml_column].notna().sum()
        
        if has_xml > 0:
            serp_classifier = SERPOfferClassifier(
                top_n=20,
                commercial_threshold=7,  # >= 7 документов с offer_info → commercial
                commercial_ratio=0.4      # или >= 40% документов
            )
            
            # Классифицируем по XML
            xml_list = analyzer.df[xml_column].fillna('').tolist()
            queries_list = analyzer.df['keyword'].tolist()
            serp_results = serp_classifier.classify_batch(xml_list, queries_list)
            
            # Добавляем результаты в DataFrame
            for key in ['serp_intent', 'serp_confidence', 'serp_docs_with_offers', 
                       'serp_total_docs', 'serp_offer_ratio',
                       'serp_avg_price', 'serp_min_price', 'serp_max_price', 
                       'serp_median_price', 'serp_currency', 'serp_offers_count',
                       'serp_offers_with_discount', 'serp_avg_discount_percent']:
                analyzer.df[key] = [r[key] for r in serp_results]
            
            # ПРИОРИТЕТ: Коммерческие слова (купить, заказать) > SERP offer_info
            # Если в запросе есть явное коммерческое слово - интент ВСЕГДА commercial
            commercial_keywords = analyzer.keyword_dicts.get('commercial', {}).get('words', set())
            
            import re
            # Создаем pattern для поиска целых слов (с границами слов)
            commercial_patterns = [
                re.compile(r'\b' + re.escape(word.lower()) + r'\b', re.IGNORECASE)
                for word in commercial_keywords
            ]
            
            for idx in analyzer.df.index:
                if analyzer.df.at[idx, xml_column] and pd.notna(analyzer.df.at[idx, xml_column]):
                    serp_intent = analyzer.df.at[idx, 'serp_intent']
                    current_intent = analyzer.df.at[idx, 'main_intent']
                    query = analyzer.df.at[idx, 'keyword'].lower()
                    
                    # Проверяем есть ли коммерческое слово в запросе (целое слово, не подстрока)
                    has_commercial_word = any(pattern.search(query) for pattern in commercial_patterns)
                    
                    # ВАЖНО: Если в запросе есть коммерческое слово (купить, заказать и т.д.),
                    # то интент должен быть commercial, независимо от текущего интента и SERP данных
                    if has_commercial_word:
                        # Определяем правильный коммерческий интент (с учетом гео)
                        if current_intent in ['informational_geo', 'commercial_geo']:
                            correct_intent = 'commercial_geo'
                        else:
                            correct_intent = 'commercial'
                        
                        # Если текущий интент не коммерческий - исправляем
                        if current_intent != correct_intent:
                            analyzer.df.at[idx, 'main_intent'] = correct_intent
                            changed_count += 1
                    # Если нет коммерческого слова - используем SERP данные
                    elif serp_intent != current_intent:
                        # Проверяем есть ли гео в запросе
                        has_geo = analyzer.df.at[idx, 'has_geo']
                        
                        # Определяем правильный интент с учетом гео
                        if has_geo:
                            # Если есть гео, добавляем суффикс _geo
                            if serp_intent == 'commercial':
                                correct_intent = 'commercial_geo'
                            elif serp_intent == 'informational':
                                correct_intent = 'informational_geo'
                            else:
                                correct_intent = serp_intent  # navigational и другие без _geo
                        else:
                            correct_intent = serp_intent
                        
                        # Обновляем интент только если он изменился
                        if current_intent != correct_intent:
                            analyzer.df.at[idx, 'main_intent'] = correct_intent
                            changed_count += 1
            
            print_stage(analyzer, f"✓ Интент скорректирован по SERP offer_info (изменено: {changed_count} из {has_xml})")
            print_stage(analyzer, f"  📊 Средняя доля документов с offer_info: {analyzer.df['serp_offer_ratio'].mean():.1%}")
            
            # Статистика по ценам (только для коммерческих)
            commercial_queries = analyzer.df[analyzer.df['serp_intent'] == 'commercial']
            if len(commercial_queries) > 0:
                avg_prices = commercial_queries['serp_avg_price'].dropna()
                if len(avg_prices) > 0:
                    print_stage(analyzer, f"  💰 Коммерческих запросов с ценами: {len(avg_prices)}")
                    print_stage(analyzer, f"  💰 Средняя цена в выдаче: {avg_prices.mean():.0f} {commercial_queries['serp_currency'].mode().iloc[0] if not commercial_queries['serp_currency'].mode().empty else 'RUR'}")
                    print_stage(analyzer, f"  💰 Диапазон: {avg_prices.min():.0f} - {avg_prices.max():.0f}")
        else:
            print_stage(analyzer, "⚠️  SERP XML данные отсутствуют, корректировка пропущена")
    else:
        print_stage(analyzer, "⚠️  Колонка с SERP XML не найдена, корректировка пропущена")
    
    # Определение брендов (ОТКЛЮЧЕНО - слишком долго)
    # print_stage(analyzer, "🔄 Определение брендов...")
    # analyzer.brand_detector = BrandDetector(analyzer.geo_dicts)
    # brand_results = analyzer.brand_detector.detect_batch(analyzer.df['keyword'].tolist())
    
    # analyzer.df['detected_brand'] = [r['detected_brand'] for r in brand_results]
    # analyzer.df['brand_confidence'] = [r['brand_confidence'] for r in brand_results]
    # analyzer.df['is_brand_query'] = [r['is_brand_query'] for r in brand_results]
    
    # print_stage(analyzer, f"✓ Найдено {sum(analyzer.df['is_brand_query'])} брендовых запросов")
    
    # Создаем пустые колонки для совместимости
    analyzer.df['detected_brand'] = None
    analyzer.df['brand_confidence'] = 0.0
    analyzer.df['is_brand_query'] = False
    
    print_stage(analyzer, "⚠️  Определение брендов отключено (для ускорения)")
    
    # Воронка продаж
    print_stage(analyzer, "🔄 Классификация по воронке...")
    analyzer.funnel_classifier = FunnelClassifier()
    funnel_results = analyzer.funnel_classifier.classify_batch(analyzer.df['keyword'].tolist())
    
    analyzer.df['funnel_stage'] = [r['funnel_stage'] for r in funnel_results]
    analyzer.df['funnel_priority'] = [r['funnel_priority'] for r in funnel_results]
    
    print_stage(analyzer, f"✓ Воронка классифицирована")
    
    # Структурные паттерны
    print_stage(analyzer, "🔄 Анализ структуры...")
    analyzer.structure_clusterer = StructureClusterer()
    analyzer.df = analyzer.structure_clusterer.extract_structural_features(analyzer.df)
    
    # Целевые страницы
    print_stage(analyzer, "🔄 Определение целевых страниц...")
    analyzer.page_mapper = PageMapper()
    
    # Проверяем что DataFrame не пустой
    if len(analyzer.df) == 0:
        print_stage(analyzer, "⚠️  DataFrame пустой, пропускаем определение целевых страниц")
        # Создаем пустые колонки для совместимости
        analyzer.df['target_page_type'] = pd.Series(dtype=str)
        analyzer.df['suggested_url'] = pd.Series(dtype=str)
    else:
        # Используем apply вместо iterrows (в 5-10 раз быстрее)
        def map_row_to_page(row):
            page_info = analyzer.page_mapper.map_query_to_page(
                row['keyword'],
                intent=row.get('main_intent'),
                has_brand=row.get('is_brand_query', False),
                has_geo=row.get('has_geo', False),
                funnel_stage=row.get('funnel_stage'),
                brand=row.get('detected_brand'),
                city=row.get('geo_city')
            )
            return pd.Series({
                'target_page_type': page_info['target_page_type'],
                'suggested_url': page_info['suggested_url']
            })
        
        # Применяем векторизованно
        page_results = analyzer.df.apply(map_row_to_page, axis=1)
        analyzer.df['target_page_type'] = page_results['target_page_type']
        analyzer.df['suggested_url'] = page_results['suggested_url']
        
        print_stage(analyzer, f"✓ Целевые страницы определены")
    
    # Обновляем интенты в БД (если они были скорректированы)
    if changed_count > 0 and hasattr(analyzer, 'current_group') and analyzer.current_group:
        try:
            from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
            master_db = MasterQueryDatabase()
            group_name = analyzer.current_group.name
            
            # Обновляем только те запросы, у которых изменился интент
            updated = master_db.update_intents_from_dataframe(group_name, analyzer.df)
            if updated > 0:
                print_stage(analyzer, f"✓ Обновлено {updated} интентов в БД")
        except Exception as e:
            print_stage(analyzer, f"⚠️  Ошибка обновления интентов в БД: {e}")
    
    print()

