"""
Тестовый скрипт для диагностики кластеризации запросов.

Проверяет почему два запроса попали в один кластер:
- "краткая биография николай чудотворец"
- "часть мощей николая чудотворца"

Использование:
    python scripts/tests/test_cluster_debug.py
"""

import json
import os
import sys
from pathlib import Path

# Добавляем корневую директорию в путь
root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir))

import pandas as pd
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
from seo_analyzer.clustering.serp_advanced_clusterer import AdvancedSERPClusterer
from seo_analyzer.core.helpers import load_all_data
from seo_analyzer.core.serp_config import SERP_CONFIG
from seo_analyzer.core.query_groups import QueryGroupManager

# Пробуем импортировать config_local
try:
    import config_local
except ImportError:
    config_local = None


async def main():
    """Основная функция теста"""
    
    # Запросы для проверки
    query1 = "краткая биография николай чудотворец"
    query2 = "часть мощей николая чудотворца"
    group_name = "николай_чудотворец"
    clustering_threshold = 7
    max_cluster_size = 0
    
    print("=" * 80)
    print("DIAGNOSTIKA KLUSTERIZACII")
    print("=" * 80)
    print(f"\nГруппа: {group_name}")
    print(f"Параметры: min_common_urls={clustering_threshold}, max_cluster_size={max_cluster_size}")
    print(f"Запрос 1: '{query1}'")
    print(f"Запрос 2: '{query2}'")
    print()
    
    # Проверяем результаты из output файлов
    print("=" * 80)
    print("PROVERKA REZULTATOV IZ OUTPUT FAILOV")
    print("=" * 80)
    
    group_manager = QueryGroupManager()
    group_manager.discover_groups()
    group = group_manager.get_group(group_name)
    
    if not group:
        print(f"WARNING: Gruppa '{group_name}' ne naydena cherez QueryGroupManager")
        print("  Prodolzhaem s Master DB...")
        group = None
    
    # Ищем CSV файл с результатами
    use_output_file = False
    csv_path = None
    
    if group:
        file_suffix = f"_{clustering_threshold}_{max_cluster_size}" if max_cluster_size else f"_{clustering_threshold}"
        csv_path = group.output_dir / f"seo_analysis_full{file_suffix}.csv"
        
        if not csv_path.exists():
            print(f"WARNING: Fail rezultatov ne nayden: {csv_path}")
            print(f"  Zapustite snachala: python main.py {group_name} {clustering_threshold} {max_cluster_size}")
            print("\n  Prodolzhaem s analizom cherez Master DB...")
        else:
            print(f"OK: Nayden fail rezultatov: {csv_path.name}")
            use_output_file = True
    else:
        # Пробуем найти через стандартный путь
        output_dir = Path("output") / "groups" / group_name
        file_suffix = f"_{clustering_threshold}_{max_cluster_size}" if max_cluster_size else f"_{clustering_threshold}"
        csv_path = output_dir / f"seo_analysis_full{file_suffix}.csv"
        
        if csv_path.exists():
            print(f"OK: Nayden fail rezultatov: {csv_path.name}")
            use_output_file = True
        else:
            print(f"WARNING: Fail rezultatov ne nayden: {csv_path}")
            print("  Prodolzhaem s analizom cherez Master DB...")
    
    # Загружаем данные
    print("\n📊 Загрузка данных...")
    master_db = MasterQueryDatabase()
    
    if not master_db.group_exists(group_name):
        print(f"❌ Группа '{group_name}' не найдена в Master DB")
        return
    
    df = master_db.load_queries(group_name, include_serp_urls=True)
    
    if df is None or df.empty:
        print(f"❌ Не удалось загрузить данные для группы '{group_name}'")
        return
    
    print(f"✓ Загружено {len(df)} запросов из Master DB")
    
    # Проверяем наличие запросов
    if query1 not in df['keyword'].values:
        print(f"❌ Запрос '{query1}' не найден в данных")
        return
    
    if query2 not in df['keyword'].values:
        print(f"❌ Запрос '{query2}' не найден в данных")
        return
    
    # Если есть output файл - проверяем результаты оттуда
    if use_output_file:
        try:
            output_df = pd.read_csv(csv_path, encoding='utf-8-sig', low_memory=False)
            
            if query1 in output_df['keyword'].values and query2 in output_df['keyword'].values:
                row1_output = output_df[output_df['keyword'] == query1].iloc[0]
                row2_output = output_df[output_df['keyword'] == query2].iloc[0]
                
                cluster_id1 = row1_output.get('semantic_cluster_id', -1)
                cluster_id2 = row2_output.get('semantic_cluster_id', -1)
                cluster_name1 = row1_output.get('cluster_name', 'N/A')
                cluster_name2 = row2_output.get('cluster_name', 'N/A')
                
                print(f"\n📋 Результаты из output файла:")
                print(f"  Запрос 1: кластер ID = {cluster_id1}, имя = '{cluster_name1}'")
                print(f"  Запрос 2: кластер ID = {cluster_id2}, имя = '{cluster_name2}'")
                
                if cluster_id1 == cluster_id2 and cluster_id1 != -1:
                    print(f"\n  ⚠️  ОБА ЗАПРОСА В ОДНОМ КЛАСТЕРЕ!")
                    
                    # Находим все запросы в этом кластере
                    cluster_queries = output_df[output_df['semantic_cluster_id'] == cluster_id1]['keyword'].tolist()
                    print(f"\n  Всего запросов в кластере: {len(cluster_queries)}")
                    print(f"  Первые 15 запросов в кластере:")
                    for i, q in enumerate(cluster_queries[:15], 1):
                        marker = " <--" if q in [query1, query2] else ""
                        print(f"    {i}. {q}{marker}")
                    
                    if len(cluster_queries) > 15:
                        print(f"    ... и еще {len(cluster_queries) - 15} запросов")
                else:
                    print(f"\n  ✓ Запросы в разных кластерах")
                    print(f"\n  (Возможно, проблема была исправлена или данные изменились)")
                    return
        except Exception as e:
            print(f"⚠️  Ошибка чтения output файла: {e}")
            print("   Продолжаем с анализом через Master DB...")
    
    print()
    
    # Извлекаем данные по запросам
    row1 = df[df['keyword'] == query1].iloc[0]
    row2 = df[df['keyword'] == query2].iloc[0]
    
    # Проверяем и загружаем SERP данные если нужно
    print("=" * 80)
    print("ПРОВЕРКА И ЗАГРУЗКА SERP ДАННЫХ")
    print("=" * 80)
    
    # Получаем API ключ
    api_key = None
    if config_local and hasattr(config_local, 'XMLSTOCK_API_KEY'):
        api_key = config_local.XMLSTOCK_API_KEY
    if not api_key:
        api_key = os.getenv('XMLSTOCK_API_KEY')
    
    if not api_key:
        print("\n⚠️  API ключ xmlstock не найден!")
        print("   Установите через:")
        print("   1. config_local.py: XMLSTOCK_API_KEY = 'user:key'")
        print("   2. Переменную окружения: XMLSTOCK_API_KEY=user:key")
    else:
        print(f"\n✓ API ключ найден: {api_key[:20]}...")
        
        # Проверяем наличие SERP данных
        def has_serp_data(serp_data):
            if serp_data is None:
                return False
            if isinstance(serp_data, str):
                try:
                    parsed = json.loads(serp_data)
                    return isinstance(parsed, list) and len(parsed) > 0
                except:
                    return False
            return isinstance(serp_data, list) and len(serp_data) > 0
        
        need_load = []
        serp_data1 = row1.get('serp_top_urls')
        serp_data2 = row2.get('serp_top_urls')
        
        if not has_serp_data(serp_data1):
            print(f"\n⚠️  У запроса '{query1}' нет SERP данных")
            need_load.append(query1)
        
        if not has_serp_data(serp_data2):
            print(f"⚠️  У запроса '{query2}' нет SERP данных")
            need_load.append(query2)
        
        if need_load:
            print(f"\n🔄 Загружаем SERP данные для {len(need_load)} запросов...")
            
            from seo_analyzer.analysis.serp.analyzer import SERPAnalyzer
            
            serp_analyzer = SERPAnalyzer(
                api_key=api_key,
                lr=SERP_CONFIG['api']['lr'],
                max_retries=SERP_CONFIG['api']['max_retries'],
                retry_delay=SERP_CONFIG['api']['retry_delay'],
                timeout=SERP_CONFIG['api']['timeout'],
                query_group=group_name,
                max_concurrent=SERP_CONFIG['api']['max_concurrent'],
                use_master_db=True,
                use_batch_async=True
            )
            
            # Загружаем данные
            serp_results = await serp_analyzer.analyze_queries_batch(need_load)
            
            print(f"✓ Загружено {len(serp_results)} результатов")
            
            # Обновляем DataFrame из Master DB
            print("🔄 Обновляем данные из Master DB...")
            df = master_db.load_queries(group_name, include_serp_urls=True)
            
            # Обновляем строки
            row1 = df[df['keyword'] == query1].iloc[0]
            row2 = df[df['keyword'] == query2].iloc[0]
            
            # Закрываем анализатор
            await serp_analyzer.close()
        else:
            print("\n✓ У обоих запросов есть SERP данные")
    
    print()
    
    # Извлекаем SERP URL
    print("=" * 80)
    print("АНАЛИЗ SERP ДАННЫХ")
    print("=" * 80)
    
    # Создаем кластеризатор для извлечения URL
    clusterer = AdvancedSERPClusterer(
        min_common_urls=clustering_threshold,
        top_positions=30,
        mode='balanced'
    )
    
    # Парсим serp_top_urls (может быть JSON строка или уже список)
    def parse_serp_urls(serp_data):
        """Парсит serp_top_urls из JSON формата"""
        if serp_data is None:
            return []
        
        # Если это уже список словарей
        if isinstance(serp_data, list):
            urls = []
            for item in serp_data:
                if isinstance(item, dict):
                    url = item.get('url', '')
                elif isinstance(item, str):
                    url = item
                else:
                    continue
                if url:
                    urls.append(url)
            return urls
        
        # Если это строка - пробуем парсить как JSON
        if isinstance(serp_data, str):
            try:
                parsed = json.loads(serp_data)
                if isinstance(parsed, list):
                    urls = []
                    for item in parsed:
                        if isinstance(item, dict):
                            url = item.get('url', '')
                        elif isinstance(item, str):
                            url = item
                        else:
                            continue
                        if url:
                            urls.append(url)
                    return urls
            except (json.JSONDecodeError, TypeError):
                pass
        
        return []
    
    raw_urls1 = parse_serp_urls(row1.get('serp_top_urls'))
    raw_urls2 = parse_serp_urls(row2.get('serp_top_urls'))
    
    # Нормализуем URL через кластеризатор
    serp_urls1 = [clusterer._normalize_url(url) for url in raw_urls1 if url]
    serp_urls2 = [clusterer._normalize_url(url) for url in raw_urls2 if url]
    
    print(f"\nЗапрос 1: '{query1}'")
    print(f"  Найдено URL: {len(serp_urls1)}")
    if serp_urls1:
        print(f"  Первые 10 URL:")
        for i, url in enumerate(serp_urls1[:10], 1):
            print(f"    {i}. {url}")
    else:
        print(f"  ⚠️  Нет SERP данных!")
    
    print(f"\nЗапрос 2: '{query2}'")
    print(f"  Найдено URL: {len(serp_urls2)}")
    if serp_urls2:
        print(f"  Первые 10 URL:")
        for i, url in enumerate(serp_urls2[:10], 1):
            print(f"    {i}. {url}")
    else:
        print(f"  ⚠️  Нет SERP данных!")
    
    print()
    
    # Проверяем схожесть (если есть данные у обоих)
    print("=" * 80)
    print("АНАЛИЗ СХОЖЕСТИ URL")
    print("=" * 80)
    
    common_count = 0
    weighted_score = 0.0
    common_urls = set()
    
    if serp_urls1 and serp_urls2:
        # Вычисляем схожесть
        common_count, weighted_score = clusterer.calculate_weighted_similarity(
            serp_urls1,
            serp_urls2
        )
        
        print(f"\nОбщие URL: {common_count}")
        print(f"Взвешенный score: {weighted_score:.2f}")
        print(f"Порог (min_common_urls): {clustering_threshold}")
        print()
        
        # Находим общие URL
        urls1_set = set(serp_urls1[:30])
        urls2_set = set(serp_urls2[:30])
        common_urls = urls1_set & urls2_set
        
        print(f"Общие URL ({len(common_urls)}):")
        if common_urls:
            for i, url in enumerate(sorted(common_urls), 1):
                # Находим позиции в обоих запросах
                pos1 = serp_urls1.index(url) + 1 if url in serp_urls1 else None
                pos2 = serp_urls2.index(url) + 1 if url in serp_urls2 else None
                
                print(f"  {i}. {url}")
                print(f"     Позиция в запросе 1: {pos1}")
                print(f"     Позиция в запросе 2: {pos2}")
        else:
            print("  (нет общих URL)")
        
        print(f"\nВывод:")
        if common_count >= clustering_threshold:
            print(f"  ✅ Порог преодолен ({common_count} >= {clustering_threshold})")
            print(f"  → Запросы ДОЛЖНЫ быть в одном кластере")
        else:
            print(f"  ❌ Порог НЕ преодолен ({common_count} < {clustering_threshold})")
            print(f"  → Запросы НЕ должны быть в одном кластере")
            print(f"  → Возможно, они объединились через транзитивную связь с другими запросами")
    else:
        print("\n⚠️  Недостаточно SERP данных для прямого сравнения:")
        print(f"  Запрос 1: {len(serp_urls1)} URL")
        print(f"  Запрос 2: {len(serp_urls2)} URL")
        print("\n  ⚠️  Если у запроса нет SERP данных, он может попасть в кластер")
        print("     через транзитивную связь с другими запросами!")
    
    print()
    
    # Запускаем кластеризацию на всех данных для проверки
    print("=" * 80)
    print("ТЕСТ КЛАСТЕРИЗАЦИИ (на всех данных)")
    print("=" * 80)
    
    print(f"\nЗапускаем кластеризацию на всех {len(df)} запросах...")
    print("(это может занять некоторое время)")
    
    # Преобразуем serp_top_urls в serp_urls (список URL) для кластеризатора
    def convert_serp_urls(row):
        """Конвертирует serp_top_urls в список URL"""
        serp_data = row.get('serp_top_urls')
        if serp_data is None:
            return []
        
        # Если это уже список словарей
        if isinstance(serp_data, list):
            urls = []
            for item in serp_data:
                if isinstance(item, dict):
                    url = item.get('url', '')
                elif isinstance(item, str):
                    url = item
                else:
                    continue
                if url:
                    urls.append(url)
            return urls
        
        # Если это строка - пробуем парсить как JSON
        if isinstance(serp_data, str):
            try:
                parsed = json.loads(serp_data)
                if isinstance(parsed, list):
                    urls = []
                    for item in parsed:
                        if isinstance(item, dict):
                            url = item.get('url', '')
                        elif isinstance(item, str):
                            url = item
                        else:
                            continue
                        if url:
                            urls.append(url)
                    return urls
            except (json.JSONDecodeError, TypeError):
                pass
        
        return []
    
    # Применяем конвертацию ко всем данным
    df['serp_urls'] = df.apply(lambda row: convert_serp_urls(row), axis=1)
    
    # Запускаем кластеризацию на всех данных
    clustered_df = await clusterer.cluster_by_serp(
        df,
        serp_column='serp_urls'
    )
    
    # Находим кластеры для наших запросов
    cluster_row1 = clustered_df[clustered_df['keyword'] == query1]
    cluster_row2 = clustered_df[clustered_df['keyword'] == query2]
    
    if cluster_row1.empty:
        print(f"❌ Запрос '{query1}' не найден после кластеризации")
        return
    
    if cluster_row2.empty:
        print(f"❌ Запрос '{query2}' не найден после кластеризации")
        return
    
    cluster_id1 = cluster_row1['semantic_cluster_id'].iloc[0]
    cluster_id2 = cluster_row2['semantic_cluster_id'].iloc[0]
    cluster_name1 = cluster_row1['cluster_name'].iloc[0]
    cluster_name2 = cluster_row2['cluster_name'].iloc[0]
    
    print(f"\nРезультат кластеризации:")
    print(f"  Запрос 1: '{query1}'")
    print(f"    → Кластер ID: {cluster_id1}")
    print(f"    → Имя кластера: '{cluster_name1}'")
    
    print(f"\n  Запрос 2: '{query2}'")
    print(f"    → Кластер ID: {cluster_id2}")
    print(f"    → Имя кластера: '{cluster_name2}'")
    
    if cluster_id1 == cluster_id2:
        print(f"\n  ⚠️  ОБА ЗАПРОСА В ОДНОМ КЛАСТЕРЕ!")
        
        # Находим все запросы в этом кластере
        cluster_queries = clustered_df[clustered_df['semantic_cluster_id'] == cluster_id1]['keyword'].tolist()
        print(f"\n  Всего запросов в кластере: {len(cluster_queries)}")
        print(f"  Первые 15 запросов в кластере:")
        for i, q in enumerate(cluster_queries[:15], 1):
            marker = " <--" if q in [query1, query2] else ""
            print(f"    {i}. {q}{marker}")
        
        if len(cluster_queries) > 15:
            print(f"    ... и еще {len(cluster_queries) - 15} запросов")
        
        print(f"\n  Причина объединения:")
        if serp_urls1 and serp_urls2:
            print(f"    - Общих URL: {common_count} (порог: {clustering_threshold})")
            print(f"    - Взвешенный score: {weighted_score:.2f} (порог: {clustering_threshold})")
            if common_count >= clustering_threshold:
                print(f"    - ✅ Порог преодолен ({common_count} >= {clustering_threshold})")
            else:
                print(f"    - ❌ Порог НЕ преодолен ({common_count} < {clustering_threshold})")
                print(f"    - ⚠️  Запросы объединились через транзитивную связь!")
                print(f"    - Возможно, есть промежуточный запрос, который связан с обоими")
        else:
            print(f"    - ⚠️  У одного из запросов нет SERP данных!")
            print(f"    - Запрос 1 имеет {len(serp_urls1)} URL")
            print(f"    - Запрос 2 имеет {len(serp_urls2)} URL")
            print(f"    - Запросы объединились через транзитивную связь с другими запросами")
    else:
        print(f"\n  ✓ Запросы в разных кластерах")
        print(f"\n  (Возможно, проблема была исправлена или данные изменились)")
    
    print()
    print("=" * 80)
    print("ДИАГНОСТИКА ЗАВЕРШЕНА")
    print("=" * 80)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
