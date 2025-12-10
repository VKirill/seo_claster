"""
Тест кластеризации БЕЗ транзитивного замыкания
Проверяет, какие кластеры соберутся при требовании прямой связи со ВСЕМИ запросами
"""

import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Set
from collections import defaultdict

DB_PATH = Path("output/master_queries.db")
GROUP_NAME = "скуд"
MIN_COMMON_URLS = 7

QUERIES = [
    "система скуд",
    "контроль доступа скуд",
    "скуд система контроля",
    "система контроля доступа скуд",
    "скуд система контроля и управления доступом",
    "оборудование скуд",
    "скуд в офис",
    "скуд охранная сигнализация",
    "системы безопасности скуд",
    "скуд накладной",
    "современный скуд",
    "система скуд для дверей",
    "автоматизированная скуд",
    "оснащение скуд",
    "система скуд в офис",
    "скуд сова",
    "станция скуд",
    "система учета контроля скуд",
    "система контроля учета доступа скуд",
    "система контроля удаленного доступа скуд",
    "системы скуд для предприятий",
    "ростаб скуд",
    "электроника скуд",
    "ред скуд",
    "скуд обои",
    "скуд рим",
]


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.replace("https://", "").replace("http://", "")
    url = url.replace("www.", "")
    url = url.split("?")[0].split("#")[0]
    return url.rstrip("/").lower()


def extract_urls_from_json(serp_top_urls_json: str) -> List[str]:
    if not serp_top_urls_json:
        return []
    try:
        data = json.loads(serp_top_urls_json)
        if isinstance(data, list):
            urls = []
            for item in data:
                if isinstance(item, dict):
                    url = item.get('url', '')
                elif isinstance(item, str):
                    url = item
                else:
                    continue
                if url:
                    urls.append(normalize_url(url))
            return urls
        return []
    except (json.JSONDecodeError, TypeError):
        return []


def get_query_urls(db_path: Path, group_name: str, query: str) -> List[str]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT serp_top_urls
        FROM master_queries
        WHERE group_name = ? AND keyword = ?
    ''', (group_name, query))
    row = cursor.fetchone()
    conn.close()
    if row and row[0]:
        return extract_urls_from_json(row[0])
    return []


def calculate_url_overlap(urls1: List[str], urls2: List[str], top_n: int = 20) -> int:
    set1 = set(urls1[:top_n])
    set2 = set(urls2[:top_n])
    return len(set1 & set2)


def can_add_to_cluster(
    query: str,
    cluster_queries: List[str],
    query_urls_dict: Dict[str, List[str]],
    min_common_urls: int
) -> bool:
    """
    Проверяет может ли запрос быть добавлен в кластер
    БЕЗ транзитивного замыкания - требуется прямая связь со ВСЕМИ запросами в кластере
    
    ВАЖНО: Если кластер состоит из двух запросов со связью >= min_common_urls * 2,
    то новый запрос может быть добавлен только если у него тоже есть связь >= min_common_urls * 2
    с обоими запросами в кластере (защита от добавления слабых связей в сильные кластеры).
    """
    if not cluster_queries:
        return True
    
    query_urls = query_urls_dict.get(query, [])
    strong_bond_threshold = min_common_urls * 2
    
    # Специальная проверка для кластеров из двух запросов с сильной связью
    if len(cluster_queries) == 2:
        # Проверяем связь между двумя запросами в кластере
        cluster_query1_urls = query_urls_dict.get(cluster_queries[0], [])
        cluster_query2_urls = query_urls_dict.get(cluster_queries[1], [])
        cluster_bond = calculate_url_overlap(cluster_query1_urls, cluster_query2_urls, top_n=20)
        
        # Если связь между запросами в кластере очень сильная (>= strong_bond_threshold),
        # то новый запрос может быть добавлен только если у него тоже очень сильная связь
        if cluster_bond >= strong_bond_threshold:
            overlap1 = calculate_url_overlap(query_urls, cluster_query1_urls, top_n=20)
            overlap2 = calculate_url_overlap(query_urls, cluster_query2_urls, top_n=20)
            
            # Оба должны быть >= strong_bond_threshold
            if overlap1 < strong_bond_threshold or overlap2 < strong_bond_threshold:
                return False
    
    # Проверяем связь со ВСЕМИ запросами в кластере
    for cluster_query in cluster_queries:
        cluster_query_urls = query_urls_dict.get(cluster_query, [])
        overlap = calculate_url_overlap(query_urls, cluster_query_urls, top_n=20)
        
        # Если хотя бы с одним запросом недостаточно общих URL - отказ
        if overlap < min_common_urls:
            return False
    
    # Все проверки пройдены - запрос связан со ВСЕМИ запросами в кластере
    return True


def calculate_cluster_max_score(
    query: str,
    cluster_queries: List[str],
    query_urls_dict: Dict[str, List[str]]
) -> float:
    """
    Вычисляет МАКСИМАЛЬНОЕ количество общих URL между запросом и запросами в кластере
    Используется для выбора кластера с наибольшими связями
    """
    if not cluster_queries:
        return 0.0
    
    query_urls = query_urls_dict.get(query, [])
    max_overlap = 0
    
    for cluster_query in cluster_queries:
        cluster_query_urls = query_urls_dict.get(cluster_query, [])
        overlap = calculate_url_overlap(query_urls, cluster_query_urls, top_n=20)
        if overlap > max_overlap:
            max_overlap = overlap
    
    return max_overlap


def cluster_queries_iterative(
    queries: List[str],
    query_urls_dict: Dict[str, List[str]],
    min_threshold: int = 4,
    max_threshold: int = 20,
    verbose: bool = False
) -> tuple[List[List[str]], Dict[str, Dict]]:
    """
    Итеративная кластеризация от большего к меньшему порогу
    
    Алгоритм:
    1. Начинаем с максимального порога (20 общих URL)
    2. На каждой итерации пытаемся добавить запросы в существующие кластеры
       или создать новые кластеры с текущим порогом
    3. Постепенно снижаем порог до минимума (4 общих URL)
    4. На каждой итерации обрабатываем только необработанные запросы
    
    Это гарантирует, что сначала формируются самые сильные связи,
    а затем к ним присоединяются запросы с меньшими связями.
    
    Args:
        queries: Список запросов для кластеризации
        query_urls_dict: Словарь запрос -> список URL
        min_threshold: Минимальный порог общих URL (по умолчанию 4)
        max_threshold: Максимальный порог общих URL (по умолчанию 20)
        verbose: Выводить отладочную информацию
    
    Returns:
        (clusters, debug_info) - список кластеров и отладочная информация
    """
    clusters = []
    processed = set()  # Запросы, которые уже попали в кластеры
    query_to_cluster = {}  # query -> cluster_idx
    debug_info = {}  # query -> {'threshold': ..., 'chosen': ...}
    
    # Итерации от максимального порога к минимальному
    for threshold in range(max_threshold, min_threshold - 1, -1):
        if verbose:
            unprocessed_count = len(queries) - len(processed)
            if unprocessed_count > 0:
                print(f"\n🔍 Итерация: порог = {threshold} общих URL (необработано: {unprocessed_count})")
        
        # Обрабатываем только необработанные запросы
        unprocessed_queries = [q for q in queries if q not in processed and query_urls_dict.get(q)]
        
        if not unprocessed_queries:
            if verbose:
                print(f"   ✓ Все запросы обработаны, переход к следующему порогу")
            continue
        
        # Находим пары запросов с текущим порогом общих URL
        pairs = []
        for i, query1 in enumerate(unprocessed_queries):
            query1_urls = query_urls_dict.get(query1, [])
            if not query1_urls:
                continue
            
            for query2 in unprocessed_queries[i+1:]:
                query2_urls = query_urls_dict.get(query2, [])
                if not query2_urls:
                    continue
                
                overlap = calculate_url_overlap(query1_urls, query2_urls, top_n=20)
                if overlap >= threshold:
                    pairs.append((query1, query2, overlap))
        
        # Сортируем пары по убыванию общих URL
        pairs.sort(key=lambda x: x[2], reverse=True)
        
        if verbose and pairs:
            print(f"   📊 Найдено пар с >= {threshold} общих URL: {len(pairs)}")
        
        # Обрабатываем пары, начиная с самых сильных связей
        for query1, query2, overlap in pairs:
            # Пропускаем, если оба запроса уже обработаны
            if query1 in processed and query2 in processed:
                continue
            
            # Если оба запроса не обработаны - создаем новый кластер
            if query1 not in processed and query2 not in processed:
                new_cluster = [query1, query2]
                clusters.append(new_cluster)
                cluster_idx = len(clusters) - 1
                query_to_cluster[query1] = cluster_idx
                query_to_cluster[query2] = cluster_idx
                processed.add(query1)
                processed.add(query2)
                
                if verbose:
                    debug_info.setdefault(query1, {})['threshold'] = threshold
                    debug_info.setdefault(query1, {})['chosen'] = 'new_cluster'
                    debug_info.setdefault(query2, {})['threshold'] = threshold
                    debug_info.setdefault(query2, {})['chosen'] = 'new_cluster'
                    print(f"   ✅ Создан кластер {cluster_idx + 1}: '{query1}' + '{query2}' ({overlap} URL)")
                continue
            
            # Если один запрос обработан, а другой нет - пытаемся добавить в кластер
            # ВАЖНО: проверяем связь со ВСЕМИ запросами в кластере, а не только с одним
            if query1 in processed and query2 not in processed:
                cluster_idx = query_to_cluster[query1]
                cluster = clusters[cluster_idx]
                # Проверяем, что query2 связан со ВСЕМИ запросами в кластере с текущим порогом
                if can_add_to_cluster(query2, cluster, query_urls_dict, threshold):
                    cluster.append(query2)
                    query_to_cluster[query2] = cluster_idx
                    processed.add(query2)
                    if verbose:
                        debug_info.setdefault(query2, {})['threshold'] = threshold
                        debug_info.setdefault(query2, {})['chosen'] = 'added_to_existing'
                        print(f"   ✅ Добавлен в кластер {cluster_idx + 1}: '{query2}' (связь с '{query1}': {overlap} URL, порог: {threshold})")
                elif verbose:
                    print(f"   ⚠️  Не добавлен в кластер {cluster_idx + 1}: '{query2}' (недостаточная связь со всеми запросами в кластере)")
            
            elif query2 in processed and query1 not in processed:
                cluster_idx = query_to_cluster[query2]
                cluster = clusters[cluster_idx]
                # Проверяем, что query1 связан со ВСЕМИ запросами в кластере с текущим порогом
                if can_add_to_cluster(query1, cluster, query_urls_dict, threshold):
                    cluster.append(query1)
                    query_to_cluster[query1] = cluster_idx
                    processed.add(query1)
                    if verbose:
                        debug_info.setdefault(query1, {})['threshold'] = threshold
                        debug_info.setdefault(query1, {})['chosen'] = 'added_to_existing'
                        print(f"   ✅ Добавлен в кластер {cluster_idx + 1}: '{query1}' (связь с '{query2}': {overlap} URL, порог: {threshold})")
                elif verbose:
                    print(f"   ⚠️  Не добавлен в кластер {cluster_idx + 1}: '{query1}' (недостаточная связь со всеми запросами в кластере)")
    
    # Добавляем оставшиеся запросы как отдельные кластеры
    for query in queries:
        if query not in processed and query_urls_dict.get(query):
            clusters.append([query])
            query_to_cluster[query] = len(clusters) - 1
            processed.add(query)
            if verbose:
                debug_info.setdefault(query, {})['threshold'] = 0
                debug_info.setdefault(query, {})['chosen'] = 'singleton'
    
    return clusters, debug_info


def cluster_queries_no_transitive(
    queries: List[str],
    query_urls_dict: Dict[str, List[str]],
    min_common_urls: int,
    verbose: bool = False
) -> tuple[List[List[str]], Dict[str, Dict]]:
    """
    Кластеризует запросы БЕЗ транзитивного замыкания
    Каждый запрос должен быть напрямую связан со ВСЕМИ запросами в кластере
    
    Алгоритм:
    1. Для каждого запроса находим запрос с МАКСИМАЛЬНЫМ количеством общих URL
    2. Если этот запрос уже в кластере, проверяем можем ли добавить текущий запрос
       (со всеми запросами в кластере должно быть >= min_common_urls)
    3. Если можем - добавляем в этот кластер
    4. Если нет - создаем новый кластер с запросом, с которым максимальная связь
    
    Приоритет: кластеры с МАКСИМАЛЬНЫМ количеством общих URL (не средним!)
    
    Returns:
        (clusters, debug_info) - список кластеров и отладочная информация
    """
    clusters = []
    processed = set()
    query_to_cluster = {}  # query -> cluster_idx
    debug_info = {}  # query -> {'best_match': ..., 'candidates': [...], 'chosen': ...}
    
    for query in queries:
        if query in processed:
            continue
        
        query_urls = query_urls_dict.get(query, [])
        if not query_urls:
            continue
        
        # ШАГ 1: Находим запрос с МАКСИМАЛЬНЫМ количеством общих URL
        # Ищем среди ВСЕХ запросов (включая уже обработанные)
        best_match_query = None
        best_match_overlap = -1
        
        for other_query in queries:
            if other_query == query:
                continue
            
            other_urls = query_urls_dict.get(other_query, [])
            if not other_urls:
                continue
            
            overlap = calculate_url_overlap(query_urls, other_urls, top_n=20)
            if overlap > best_match_overlap:
                best_match_overlap = overlap
                best_match_query = other_query
        
        # ШАГ 2: Если нашли запрос с максимальной связью
        if best_match_query and best_match_overlap >= min_common_urls:
            # Проверяем, в каком кластере находится этот запрос
            best_match_cluster_idx = query_to_cluster.get(best_match_query)
            
            if best_match_cluster_idx is not None:
                # Запрос уже в кластере
                cluster = clusters[best_match_cluster_idx]
                
                # ВАЖНО: Если максимальная связь очень сильная (>= min_common_urls * 2),
                # создаем отдельный кластер только с этими двумя запросами
                # Это приоритетнее, чем добавление в существующий кластер
                strong_bond_threshold = min_common_urls * 2  # >= 14 для min=7
                if best_match_overlap >= strong_bond_threshold:
                    # Удаляем best_match_query из старого кластера
                    old_cluster = clusters[best_match_cluster_idx]
                    old_cluster.remove(best_match_query)
                    query_to_cluster.pop(best_match_query)
                    
                    # Убираем best_match_query из processed, чтобы он мог быть добавлен в новый кластер
                    processed.discard(best_match_query)
                    
                    # Если старый кластер стал пустым - удаляем его
                    if len(old_cluster) == 0:
                        clusters.pop(best_match_cluster_idx)
                        # Обновляем индексы для других запросов
                        for q, idx in list(query_to_cluster.items()):
                            if idx > best_match_cluster_idx:
                                query_to_cluster[q] = idx - 1
                        best_match_cluster_idx = None
                    else:
                        # Обновляем индексы для запросов в старом кластере
                        for q in old_cluster:
                            query_to_cluster[q] = best_match_cluster_idx
                    
                    # Создаем новый кластер только с этими двумя запросами
                    # Этот кластер будет изолированным - другие запросы не смогут в него попасть
                    # если у них нет такой же сильной связи (>= strong_bond_threshold)
                    new_cluster = [best_match_query, query]
                    clusters.append(new_cluster)
                    new_cluster_idx = len(clusters) - 1
                    query_to_cluster[best_match_query] = new_cluster_idx
                    query_to_cluster[query] = new_cluster_idx
                    processed.add(best_match_query)
                    processed.add(query)
                    
                    # Помечаем этот кластер как "сильный" (strong bond cluster)
                    # для предотвращения добавления других запросов с меньшей связью
                    # Это будет использоваться в can_add_to_cluster
                    
                    if verbose:
                        debug_info[query] = {
                            'best_match': best_match_query,
                            'best_match_overlap': best_match_overlap,
                            'chosen': 'new_cluster_strong_bond',
                            'cluster_idx': new_cluster_idx,
                            'threshold': strong_bond_threshold,
                            'note': f'Сильная связь ({best_match_overlap} URL >= {strong_bond_threshold}) - отдельный кластер из двух запросов'
                        }
                    continue
                
                # Если связь не настолько сильная - проверяем можем ли добавить в существующий кластер
                if can_add_to_cluster(query, cluster, query_urls_dict, min_common_urls):
                    # Добавляем в существующий кластер
                    cluster.append(query)
                    query_to_cluster[query] = best_match_cluster_idx
                    processed.add(query)
                    
                    if verbose:
                        debug_info[query] = {
                            'best_match': best_match_query,
                            'best_match_overlap': best_match_overlap,
                            'chosen': 'existing_cluster',
                            'cluster_idx': best_match_cluster_idx,
                            'cluster_size': len(cluster)
                        }
                    continue
            
            # ШАГ 3: Если запрос не в кластере или не можем добавить - ищем другие варианты
            # Ищем ВСЕ кластеры, куда можем добавить запрос
            candidates = []
            
            for cluster_idx, cluster in enumerate(clusters):
                if can_add_to_cluster(query, cluster, query_urls_dict, min_common_urls):
                    # Вычисляем МАКСИМАЛЬНОЕ количество общих URL с запросами в кластере
                    max_score = calculate_cluster_max_score(query, cluster, query_urls_dict)
                    candidates.append({
                        'cluster_idx': cluster_idx,
                        'cluster': cluster,
                        'max_score': max_score,
                        'size': len(cluster)
                    })
            
            # Выбираем кластер с МАКСИМАЛЬНЫМ количеством общих URL
            best_candidate = None
            if candidates:
                best_candidate = max(candidates, key=lambda x: x['max_score'])
            
            if best_candidate:
                # Добавляем в лучший кластер
                best_candidate['cluster'].append(query)
                query_to_cluster[query] = best_candidate['cluster_idx']
                processed.add(query)
                
                if verbose:
                    debug_info[query] = {
                        'best_match': best_match_query,
                        'best_match_overlap': best_match_overlap,
                        'candidates': candidates,
                        'chosen': 'best_candidate',
                        'cluster_idx': best_candidate['cluster_idx'],
                        'max_score': best_candidate['max_score']
                    }
            else:
                # Не нашли подходящий кластер - создаем новый с best_match_query
                if best_match_query not in processed:
                    # Создаем кластер из двух запросов
                    new_cluster = [best_match_query, query]
                    clusters.append(new_cluster)
                    query_to_cluster[best_match_query] = len(clusters) - 1
                    query_to_cluster[query] = len(clusters) - 1
                    processed.add(best_match_query)
                    processed.add(query)
                    
                    if verbose:
                        debug_info[query] = {
                            'best_match': best_match_query,
                            'best_match_overlap': best_match_overlap,
                            'chosen': 'new_cluster_with_match',
                            'cluster_idx': len(clusters) - 1
                        }
                else:
                    # best_match_query уже обработан - создаем одиночный кластер
                    clusters.append([query])
                    query_to_cluster[query] = len(clusters) - 1
                    processed.add(query)
                    
                    if verbose:
                        debug_info[query] = {
                            'best_match': best_match_query,
                            'best_match_overlap': best_match_overlap,
                            'chosen': 'singleton',
                            'cluster_idx': len(clusters) - 1,
                            'note': 'best_match already processed'
                        }
        else:
            # Не нашли запрос с достаточным количеством общих URL - создаем одиночный кластер
            clusters.append([query])
            query_to_cluster[query] = len(clusters) - 1
            processed.add(query)
            
            if verbose:
                debug_info[query] = {
                    'best_match': best_match_query,
                    'best_match_overlap': best_match_overlap if best_match_query else 0,
                    'chosen': 'singleton',
                    'cluster_idx': len(clusters) - 1,
                    'note': 'no sufficient matches'
                }
    
    return clusters, debug_info


def main():
    print("=" * 80)
    print("ИТЕРАТИВНАЯ КЛАСТЕРИЗАЦИЯ ОТ БОЛЬШЕГО К МЕНЬШЕМУ")
    print("=" * 80)
    print(f"\nДиапазон порогов: от 20 до 4 общих URL")
    print("Требование: каждый запрос должен быть связан со ВСЕМИ запросами в кластере")
    print("Алгоритм: сначала формируются кластеры с максимальными связями (20 URL),")
    print("          затем постепенно снижается порог до 4 URL")
    print()
    
    # Загружаем URL
    print("📥 Загрузка URL...")
    query_urls_dict = {}
    for query in QUERIES:
        urls = get_query_urls(DB_PATH, GROUP_NAME, query)
        if urls:
            query_urls_dict[query] = urls
    
    print(f"✓ Загружено: {len(query_urls_dict)} запросов\n")
    
    # Кластеризуем итеративно от большего к меньшему
    print("🔗 Итеративная кластеризация...")
    print("   Пороги: 20 → 19 → 18 → ... → 4 общих URL")
    print("   На каждой итерации обрабатываются только необработанные запросы")
    clusters, debug_info = cluster_queries_iterative(
        list(query_urls_dict.keys()),
        query_urls_dict,
        min_threshold=4,
        max_threshold=20,
        verbose=True
    )
    
    print(f"\n✓ Создано кластеров: {len(clusters)}\n")
    
    # Показываем примеры выбора кластеров
    print("=" * 80)
    print("ПРИМЕРЫ ВЫБОРА КЛАСТЕРОВ")
    print("=" * 80)
    
    examples_shown = 0
    for query, info in debug_info.items():
        if examples_shown >= 10:  # Показываем первые 10 примеров
            break
        
        examples_shown += 1
        print(f"\n📌 {query}:")
        
        if info.get('best_match'):
            print(f"   🎯 Лучшая связь: '{info['best_match']}' ({info['best_match_overlap']} общих URL)")
        
        if info['chosen'] == 'existing_cluster':
            print(f"   ✅ Добавлен в существующий кластер {info['cluster_idx']+1} (размер: {info['cluster_size']})")
            print(f"      → Запрос '{info['best_match']}' уже был в этом кластере")
        elif info['chosen'] == 'best_candidate':
            print(f"   ✅ Выбран кластер {info['cluster_idx']+1} из {len(info['candidates'])} кандидатов")
            print(f"      → Максимальная связь в кластере: {info['max_score']} общих URL")
            if info.get('candidates'):
                print(f"      Кандидаты:")
                for cand in sorted(info['candidates'], key=lambda x: x['max_score'], reverse=True)[:3]:
                    marker = "✅" if cand['cluster_idx'] == info['cluster_idx'] else "  "
                    print(f"      {marker} Кластер {cand['cluster_idx']+1}: макс. связь = {cand['max_score']} URL")
        elif info['chosen'] == 'new_cluster_with_match':
            print(f"   ✅ Создан новый кластер {info['cluster_idx']+1} вместе с '{info['best_match']}'")
            print(f"      → Связь между ними: {info['best_match_overlap']} общих URL")
        elif info['chosen'] == 'singleton':
            print(f"   ⚠️  Создан одиночный кластер {info['cluster_idx']+1}")
            if info.get('note'):
                print(f"      → {info['note']}")
            elif info.get('best_match_overlap', 0) > 0:
                print(f"      → Лучшая связь ({info['best_match_overlap']} URL) недостаточна для объединения")
    
    print()
    
    # Выводим результаты
    print("=" * 80)
    print("РЕЗУЛЬТАТЫ КЛАСТЕРИЗАЦИИ")
    print("=" * 80)
    
    # Сортируем кластеры по размеру
    clusters_sorted = sorted(clusters, key=len, reverse=True)
    
    for i, cluster in enumerate(clusters_sorted, 1):
        print(f"\n📦 Кластер {i} (размер: {len(cluster)}):")
        
        for query in sorted(cluster):
            print(f"   • {query}")
        
        # Показываем точное количество общих URL для каждой пары запросов
        if len(cluster) > 1:
            print(f"\n   Точное количество общих URL между запросами:")
            all_connected = True
            for j, query1 in enumerate(cluster):
                for query2 in cluster[j+1:]:
                    overlap = calculate_url_overlap(
                        query_urls_dict[query1],
                        query_urls_dict[query2],
                        top_n=20
                    )
                    # Используем минимальный порог 4 для проверки (так как мы используем пороги от 4 до 20)
                    min_threshold_for_check = 4
                    status = "✅" if overlap >= min_threshold_for_check else "❌"
                    print(f"     {status} {query1[:30]:<30} ↔ {query2[:30]:<30} : {overlap} общих URL")
                    if overlap < min_threshold_for_check:
                        all_connected = False
            
            if all_connected:
                print(f"   ✓ Все запросы связаны между собой (>= {min_threshold_for_check} общих URL)")
            else:
                print(f"   ⚠️  ОШИБКА: Найдены запросы с недостаточным пересечением!")
    
    # Статистика
    print("\n" + "=" * 80)
    print("СТАТИСТИКА")
    print("=" * 80)
    
    cluster_sizes = [len(c) for c in clusters_sorted]
    print(f"\nВсего кластеров: {len(clusters_sorted)}")
    print(f"Средний размер: {sum(cluster_sizes) / len(cluster_sizes):.1f}")
    print(f"Мин/Макс размер: {min(cluster_sizes)} / {max(cluster_sizes)}")
    
    # Проверяем "скуд обои"
    target_query = "скуд обои"
    print(f"\n{'=' * 80}")
    print(f"АНАЛИЗ: '{target_query}'")
    print("=" * 80)
    
    target_cluster = None
    for cluster in clusters_sorted:
        if target_query in cluster:
            target_cluster = cluster
            break
    
    if target_cluster:
        print(f"\n✅ '{target_query}' находится в кластере размером {len(target_cluster)}")
        print(f"\nЗапросы в этом кластере:")
        for query in sorted(target_cluster):
            if query == target_query:
                print(f"   • {query} ← ТЕКУЩИЙ")
            else:
                overlap = calculate_url_overlap(
                    query_urls_dict[target_query],
                    query_urls_dict[query],
                    top_n=20
                )
                print(f"   • {query} ({overlap} общих URL)")
    else:
        print(f"\n❌ '{target_query}' не найден в кластерах (возможно, нет данных в БД)")


if __name__ == "__main__":
    main()

