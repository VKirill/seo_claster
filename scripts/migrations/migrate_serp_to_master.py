"""
Миграция из serp_data.db в Master Query Database
Парсит XML из serp_results и добавляет в master_queries

Что делает:
1. Читает serp_results (XML ответы от xmlstock)
2. Парсит XML → извлекает top_urls, offer_info, LSI
3. Объединяет с query_cache.db (intent, normalized и т.д.)
4. Сохраняет в master_queries.db
"""

import sqlite3
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
from tqdm import tqdm


def parse_serp_xml(xml_response: str) -> Dict[str, Any]:
    """
    Парсит XML ответ от xmlstock
    
    Returns:
        Dict с top_urls, offer_info, lsi_phrases
    """
    if not xml_response:
        return {
            'top_urls': [],
            'lsi_phrases': [],
            'has_offers': False,
            'docs_with_offers': 0,
            'total_offers': 0,
            'total_docs': 0
        }
    
    try:
        root = ET.fromstring(xml_response)
        
        # Извлекаем документы (top URLs)
        top_urls = []
        docs_with_offers = 0
        total_offers = 0
        
        for i, group in enumerate(root.findall('.//group'), 1):
            doc = group.find('doc')
            if doc is not None:
                url_elem = doc.find('url')
                domain_elem = doc.find('domain')
                title_elem = doc.find('title')
                
                url = url_elem.text if url_elem is not None else ''
                domain = domain_elem.text if domain_elem is not None else ''
                title = title_elem.text if title_elem is not None else ''
                
                # Проверяем наличие offer_info (коммерческая выдача)
                offers = doc.findall('.//offer_info')
                has_offer = len(offers) > 0
                
                if has_offer:
                    docs_with_offers += 1
                    total_offers += len(offers)
                
                top_urls.append({
                    'url': url,
                    'domain': domain,
                    'position': i,
                    'title': title,
                    'is_commercial': has_offer,
                    'offers_count': len(offers)
                })
                
                # Ограничиваем 20 URL
                if len(top_urls) >= 20:
                    break
        
        # LSI фразы (из пассажей)
        lsi_phrases = []
        seen_phrases = set()
        
        for passage in root.findall('.//passage'):
            if passage.text:
                # Простая экстракция фраз (можно улучшить)
                text = passage.text.strip()
                if text and text not in seen_phrases and len(text) > 3:
                    seen_phrases.add(text)
                    lsi_phrases.append(text)
        
        return {
            'top_urls': top_urls,
            'lsi_phrases': lsi_phrases[:50],  # TOP-50 LSI
            'has_offers': docs_with_offers > 0,
            'docs_with_offers': docs_with_offers,
            'total_offers': total_offers,
            'total_docs': len(top_urls)
        }
    
    except ET.ParseError as e:
        print(f"⚠️  XML parse error: {e}")
        return {
            'top_urls': [],
            'lsi_phrases': [],
            'has_offers': False,
            'docs_with_offers': 0,
            'total_offers': 0,
            'total_docs': 0
        }


def migrate_group(
    group_name: str,
    serp_db_path: Path,
    query_cache_path: Path,
    master_db_path: Path
):
    """
    Миграция одной группы
    
    Args:
        group_name: Название группы
        serp_db_path: output/serp_data.db
        query_cache_path: output/query_cache.db
        master_db_path: output/master_queries.db
    """
    print(f"\n{'='*80}")
    print(f"Миграция группы: {group_name}")
    print(f"{'='*80}")
    
    # 1. Загружаем данные из query_cache.db
    print("\n1. Загрузка из query_cache.db...")
    
    if not query_cache_path.exists():
        print(f"❌ {query_cache_path} не найдена")
        return
    
    conn_cache = sqlite3.connect(query_cache_path)
    
    # Загружаем базовые данные + intent
    cache_query = """
        SELECT 
            keyword, frequency_world, frequency_exact,
            normalized, lemmatized, words_count,
            main_words, key_phrase,
            ner_entities, ner_locations,
            main_intent, commercial_score, informational_score, navigational_score
        FROM cached_queries
        WHERE group_name = ?
    """
    
    df_cache = pd.read_sql_query(cache_query, conn_cache, params=(group_name,))
    conn_cache.close()
    
    if df_cache.empty:
        print(f"⚠️  Группа '{group_name}' не найдена в query_cache.db")
        return
    
    print(f"✓ Загружено {len(df_cache)} запросов из кэша")
    
    # 2. Загружаем SERP данные
    print("\n2. Загрузка и парсинг SERP данных...")
    
    if not serp_db_path.exists():
        print(f"⚠️  {serp_db_path} не найдена, пропускаем SERP данные")
        serp_data = {}
    else:
        conn_serp = sqlite3.connect(serp_db_path)
        
        serp_query = """
            SELECT 
                query, xml_response, found_docs, main_pages_count,
                titles_with_keyword, commercial_domains, info_domains,
                created_at
            FROM serp_results
            WHERE query_group = ?
        """
        
        cursor = conn_serp.execute(serp_query, (group_name,))
        
        serp_data = {}
        for row in tqdm(cursor.fetchall(), desc="Парсинг XML"):
            query = row[0]
            xml_response = row[1]
            
            # Парсим XML
            parsed = parse_serp_xml(xml_response)
            
            serp_data[query] = {
                'found_docs': row[2],
                'main_pages_count': row[3],
                'titles_with_keyword': row[4],
                'commercial_domains': row[5],
                'info_domains': row[6],
                'created_at': row[7],
                'top_urls': json.dumps(parsed['top_urls'], ensure_ascii=False),
                'lsi_phrases': json.dumps(parsed['lsi_phrases'], ensure_ascii=False),
                'has_offers': parsed['has_offers'],
                'docs_with_offers': parsed['docs_with_offers'],
                'total_offers': parsed['total_offers'],
                'total_docs': parsed['total_docs']
            }
        
        conn_serp.close()
        print(f"✓ Загружено {len(serp_data)} SERP записей")
    
    # 3. Объединяем данные
    print("\n3. Объединение данных...")
    
    # Добавляем SERP колонки в DataFrame
    df_cache['serp_found_docs'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('found_docs')
    )
    df_cache['serp_main_pages_count'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('main_pages_count')
    )
    df_cache['serp_titles_with_keyword'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('titles_with_keyword')
    )
    df_cache['serp_commercial_domains'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('commercial_domains')
    )
    df_cache['serp_info_domains'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('info_domains')
    )
    df_cache['serp_created_at'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('created_at')
    )
    df_cache['serp_top_urls'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('top_urls')
    )
    df_cache['serp_lsi_phrases'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('lsi_phrases')
    )
    
    # Подсчитываем offer_ratio
    df_cache['serp_docs_with_offers'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('docs_with_offers', 0)
    )
    df_cache['serp_total_docs'] = df_cache['keyword'].map(
        lambda x: serp_data.get(x, {}).get('total_docs', 0)
    )
    
    df_cache['serp_offer_ratio'] = (
        df_cache['serp_docs_with_offers'] / df_cache['serp_total_docs'].replace(0, 1)
    ).fillna(0.0)
    
    # Определяем serp_intent по offer_ratio
    df_cache['serp_intent'] = df_cache['serp_offer_ratio'].apply(
        lambda x: 'commercial' if x >= 0.4 else 'informational' if pd.notna(x) else None
    )
    
    print(f"✓ Данные объединены")
    print(f"  • С SERP данными: {df_cache['serp_found_docs'].notna().sum()}")
    print(f"  • С offer_info: {df_cache['serp_docs_with_offers'].gt(0).sum()}")
    
    # 4. Сохраняем в Master DB
    print("\n4. Сохранение в Master DB...")
    
    from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
    
    master_db = MasterQueryDatabase(master_db_path)
    master_db.save_queries(
        group_name=group_name,
        df=df_cache,
        csv_path=None,
        csv_hash=None
    )
    
    # Статистика
    stats = master_db.get_statistics(group_name)
    
    print(f"\n✅ Миграция завершена!")
    print(f"  • Всего запросов: {stats['total_queries']:,}")
    print(f"  • С интентом: {stats['with_intent']:,}")
    print(f"  • С SERP: {stats['with_serp']:,}")
    print(f"  • Средний offer_ratio: {stats['avg_offer_ratio']:.2%}")


def main():
    """Главная функция миграции"""
    print("=" * 80)
    print("Миграция: serp_data.db + query_cache.db → Master Query Database")
    print("=" * 80)
    
    # Пути к БД
    serp_db_path = Path("output/serp_data.db")
    query_cache_path = Path("output/query_cache.db")
    master_db_path = Path("output/master_queries.db")
    
    # Получаем список групп
    if not query_cache_path.exists():
        print(f"\n❌ {query_cache_path} не найдена")
        print("   Сначала запустите анализ для создания кэша")
        return
    
    conn = sqlite3.connect(query_cache_path)
    cursor = conn.execute("SELECT group_name FROM query_groups ORDER BY group_name")
    groups = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    if not groups:
        print("\n❌ Группы не найдены в query_cache.db")
        return
    
    print(f"\nНайдено групп: {len(groups)}")
    for group in groups:
        print(f"  • {group}")
    
    print("\nВыберите действие:")
    print("  1. Мигрировать все группы")
    print("  2. Мигрировать одну группу")
    print("  0. Выход")
    
    choice = input("\nВаш выбор: ").strip()
    
    if choice == "1":
        # Миграция всех групп
        for group in groups:
            try:
                migrate_group(group, serp_db_path, query_cache_path, master_db_path)
            except Exception as e:
                print(f"\n❌ Ошибка миграции группы '{group}': {e}")
                continue
    
    elif choice == "2":
        # Миграция одной группы
        print("\nДоступные группы:")
        for i, group in enumerate(groups, 1):
            print(f"  {i}. {group}")
        
        idx = input("\nНомер группы: ").strip()
        try:
            group_idx = int(idx) - 1
            if 0 <= group_idx < len(groups):
                selected_group = groups[group_idx]
                migrate_group(selected_group, serp_db_path, query_cache_path, master_db_path)
            else:
                print("❌ Неверный номер")
        except ValueError:
            print("❌ Введите число")
    
    elif choice == "0":
        print("\nВыход...")
        return
    
    else:
        print("\n❌ Неверный выбор")
        return
    
    # Финальная статистика
    print(f"\n{'='*80}")
    print("Итоговая статистика Master DB")
    print(f"{'='*80}")
    
    from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
    
    master_db = MasterQueryDatabase(master_db_path)
    
    for group in groups:
        if master_db.group_exists(group):
            stats = master_db.get_statistics(group)
            print(f"\n{group}:")
            print(f"  Запросов: {stats['total_queries']:,}")
            print(f"  С SERP: {stats['with_serp']:,} ({stats['with_serp']/stats['total_queries']*100:.1f}%)")
            print(f"  Средний KEI: {stats['avg_kei']:.2f}")


if __name__ == "__main__":
    main()

