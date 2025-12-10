"""
Диагностика LSI фраз для конкретного запроса

Проверяет:
1. Есть ли LSI фразы у запроса в БД
2. Есть ли у запроса кластер
3. Есть ли LSI фразы у кластера после агрегации
4. Как они экспортируются в Excel
"""

import sys
from pathlib import Path
import pandas as pd
import json

# Добавляем корневую директорию в путь
root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir))

from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
from seo_analyzer.core.config_paths import OUTPUT_DIR


def check_query_lsi(query: str, group_name: str = None):
    """
    Проверить LSI фразы для конкретного запроса
    
    Args:
        query: Поисковый запрос
        group_name: Название группы (опционально)
    """
    print("=" * 80)
    print(f"🔍 ДИАГНОСТИКА LSI ФРАЗ ДЛЯ ЗАПРОСА")
    print("=" * 80)
    print(f"Запрос: '{query}'")
    if group_name:
        print(f"Группа: '{group_name}'")
    print()
    
    # Инициализируем Master DB
    db_path = OUTPUT_DIR / "master_queries.db"
    if not db_path.exists():
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    master_db = MasterQueryDatabase(db_path=db_path)
    
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Ищем запрос в БД (semantic_cluster_id не хранится в БД, добавляется динамически)
    if group_name:
        cursor.execute('''
            SELECT keyword, group_name, serp_lsi_phrases, serp_top_urls, serp_status
            FROM master_queries
            WHERE keyword = ? AND group_name = ?
        ''', (query, group_name))
        row = cursor.fetchone()
    else:
        # Ищем точное совпадение
        cursor.execute('''
            SELECT keyword, group_name, serp_lsi_phrases, serp_top_urls, serp_status
            FROM master_queries
            WHERE keyword = ?
            LIMIT 1
        ''', (query,))
        row = cursor.fetchone()
        
        # Если не найдено, ищем похожие запросы
        if not row:
            print(f"⚠️  Точное совпадение не найдено, ищем похожие запросы...")
            cursor.execute('''
                SELECT keyword, group_name
                FROM master_queries
                WHERE keyword LIKE ? OR keyword LIKE ?
                LIMIT 10
            ''', (f'%{query[:20]}%', f'%{query[-20:]}%'))
            similar = cursor.fetchall()
            if similar:
                print(f"   Найдено {len(similar)} похожих запросов:")
                for i, (kw, grp) in enumerate(similar[:5], 1):
                    print(f"      {i}. '{kw}' (группа: {grp})")
                print()
                print("   Попробуйте запустить с точным запросом:")
                print(f"   python check_query_lsi.py '{similar[0][0]}' '{similar[0][1]}'")
            else:
                # Ищем по части запроса
                words = query.split()
                if len(words) > 1:
                    # Пробуем найти по слову "скуд" или "система"
                    search_terms = []
                    for word in words:
                        if len(word) > 3:  # Игнорируем короткие слова
                            search_terms.append(f'%{word}%')
                    
                    if search_terms:
                        # Ищем в группе "скуд" если есть слово "скуд" в запросе
                        if 'скуд' in query.lower():
                            cursor.execute('''
                                SELECT keyword, group_name
                                FROM master_queries
                                WHERE group_name = 'скуд' AND keyword LIKE ?
                                LIMIT 10
                            ''', (f'%{query[:15]}%',))
                            similar = cursor.fetchall()
                            if similar:
                                print(f"   Найдено {len(similar)} запросов в группе 'скуд', содержащих похожие слова:")
                                for i, (kw, grp) in enumerate(similar[:5], 1):
                                    print(f"      {i}. '{kw}' (группа: {grp})")
                                print()
                                print("   Попробуйте запустить с точным запросом:")
                                print(f"   python check_query_lsi.py '{similar[0][0]}' '{similar[0][1]}'")
                        else:
                            # Ищем по первому длинному слову
                            search_term = search_terms[0]
                            cursor.execute('''
                                SELECT keyword, group_name
                                FROM master_queries
                                WHERE keyword LIKE ?
                                LIMIT 10
                            ''', (search_term,))
                            similar = cursor.fetchall()
                            if similar:
                                print(f"   Найдено {len(similar)} запросов, содержащих '{words[0]}':")
                                for i, (kw, grp) in enumerate(similar[:5], 1):
                                    print(f"      {i}. '{kw}' (группа: {grp})")
    
    if not row:
        print(f"❌ Запрос '{query}' не найден в базе данных")
        if group_name:
            print(f"   Группа: {group_name}")
            # Ищем похожие запросы в этой группе
            print(f"\n   Ищем похожие запросы в группе '{group_name}'...")
            words = query.split()
            search_queries = []
            for word in words:
                if len(word) > 3:
                    search_queries.append(f'%{word}%')
            
            if search_queries:
                # Ищем запросы, содержащие несколько слов из запроса
                placeholders = ','.join(['?'] * len(search_queries))
                sql = f'''
                    SELECT keyword
                    FROM master_queries
                    WHERE group_name = ? AND ({' OR '.join(['keyword LIKE ?'] * len(search_queries))})
                    LIMIT 20
                '''
                cursor.execute(sql, (group_name,) + tuple(search_queries))
                similar = cursor.fetchall()
                if similar:
                    print(f"   Найдено {len(similar)} похожих запросов:")
                    for i, (kw,) in enumerate(similar[:10], 1):
                        print(f"      {i}. '{kw}'")
                    print()
                    print("   Попробуйте запустить с точным запросом:")
                    print(f"   python check_query_lsi.py '{similar[0][0]}' '{group_name}'")
        else:
            print("   Попробуйте указать группу: python check_query_lsi.py '<запрос>' '<группа>'")
            # Показываем список всех групп
            cursor.execute('SELECT DISTINCT group_name FROM master_queries ORDER BY group_name')
            groups = cursor.fetchall()
            if groups:
                print(f"\n   Доступные группы ({len(groups)}):")
                for i, (grp,) in enumerate(groups[:10], 1):
                    print(f"      {i}. {grp}")
                if len(groups) > 10:
                    print(f"      ... и еще {len(groups) - 10} групп")
        conn.close()
        return
    
    keyword, found_group, lsi_phrases_json, top_urls_json, serp_status = row
    
    print(f"✓ Запрос найден в группе: '{found_group}'")
    print(f"   Статус SERP: {serp_status}")
    
    # Проверяем наличие req_id
    cursor.execute('''
        SELECT serp_req_id
        FROM master_queries
        WHERE keyword = ? AND group_name = ?
    ''', (keyword, found_group))
    req_id_row = cursor.fetchone()
    req_id = req_id_row[0] if req_id_row else None
    
    if req_id:
        print(f"   SERP req_id: {req_id}")
    else:
        print(f"   ⚠️  SERP req_id отсутствует")
    print()
    
    # Проверяем LSI фразы запроса
    print("📋 LSI фразы запроса (из БД):")
    if lsi_phrases_json:
        try:
            lsi_phrases = json.loads(lsi_phrases_json) if isinstance(lsi_phrases_json, str) else lsi_phrases_json
            if isinstance(lsi_phrases, list) and len(lsi_phrases) > 0:
                print(f"   ✓ Найдено {len(lsi_phrases)} LSI фраз")
                for i, item in enumerate(lsi_phrases[:5], 1):
                    if isinstance(item, dict):
                        phrase = item.get('phrase', '')
                        freq = item.get('frequency', 0)
                        source = item.get('source', 'unknown')
                        print(f"      {i}. {phrase} (частота: {freq}, источник: {source})")
                if len(lsi_phrases) > 5:
                    print(f"      ... и еще {len(lsi_phrases) - 5} фраз")
            else:
                print(f"   ⚠️  LSI фразы пустые или в неправильном формате")
                print(f"      Тип: {type(lsi_phrases)}, Значение: {str(lsi_phrases)[:100]}")
        except Exception as e:
            print(f"   ❌ Ошибка парсинга LSI фраз: {e}")
            print(f"      Сырые данные: {str(lsi_phrases_json)[:200]}")
    else:
        print("   ❌ LSI фразы отсутствуют в БД")
        if req_id and serp_status == 'completed':
            print("   ⚠️  ПРОБЛЕМА: Статус 'completed', но LSI фразы отсутствуют!")
            print("   → Возможно, данные не были получены от xmlstock")
            print("   → Или данные были получены, но не сохранены")
            print("   → Решение: пересоберите SERP данные для этого запроса")
    print()
    
    # Проверяем URL
    print("📋 URL данные:")
    if top_urls_json:
        try:
            top_urls = json.loads(top_urls_json) if isinstance(top_urls_json, str) else top_urls_json
            if isinstance(top_urls, list):
                print(f"   ✓ Найдено {len(top_urls)} URL")
                # Проверяем формат
                if len(top_urls) > 0:
                    first_item = top_urls[0]
                    if isinstance(first_item, dict):
                        has_title = bool(first_item.get('title'))
                        print(f"   Формат: словари {'с title' if has_title else 'без title'}")
                    elif isinstance(first_item, str):
                        print(f"   Формат: только строки URL")
            else:
                print(f"   ⚠️  URL в неправильном формате")
        except Exception as e:
            print(f"   ❌ Ошибка парсинга URL: {e}")
    else:
        print("   ❌ URL отсутствуют")
    print()
    
    # Загружаем данные через MasterQueryDatabase для получения кластеризации
    print("📋 Загрузка данных группы для проверки кластеризации...")
    try:
        df = master_db.load_queries(found_group, include_serp_urls=True)
        if df is None or len(df) == 0:
            print(f"❌ Не удалось загрузить данные группы '{found_group}'")
            return
        
        # Находим наш запрос в DataFrame
        query_row = df[df['keyword'] == query]
        if len(query_row) == 0:
            print(f"❌ Запрос '{query}' не найден в загруженных данных")
            return
        
        cluster_id = query_row['semantic_cluster_id'].iloc[0] if 'semantic_cluster_id' in query_row.columns else None
        
        if cluster_id is None or pd.isna(cluster_id):
            print("⚠️  У запроса нет кластера (semantic_cluster_id = NULL)")
            print("   LSI фразы кластера не могут быть заполнены без кластера")
            print("   → Запустите кластеризацию для этой группы")
            return
        
        if cluster_id == -1:
            print("⚠️  Запрос не кластеризован (semantic_cluster_id = -1)")
            print("   LSI фразы кластера не могут быть заполнены для некластеризованных запросов")
            return
        
        print(f"📋 Проверка кластера {cluster_id}:")
        
        # Находим все запросы в этом кластере из DataFrame
        cluster_df = df[df['semantic_cluster_id'] == cluster_id]
    
        print(f"   Запросов в кластере: {len(cluster_df)}")
        
        # Проверяем LSI фразы всех запросов кластера
        queries_with_lsi = 0
        queries_without_lsi = 0
        all_lsi_phrases = []
        
        for idx, row in cluster_df.iterrows():
            q_lsi = row.get('lsi_phrases', [])
            if q_lsi:
                if isinstance(q_lsi, str):
                    try:
                        q_lsi = json.loads(q_lsi)
                    except:
                        queries_without_lsi += 1
                        continue
                
                if isinstance(q_lsi, list) and len(q_lsi) > 0:
                    queries_with_lsi += 1
                    all_lsi_phrases.extend(q_lsi)
                else:
                    queries_without_lsi += 1
            else:
                queries_without_lsi += 1
    
        print(f"   Запросов с LSI: {queries_with_lsi}/{len(cluster_df)}")
        print(f"   Запросов без LSI: {queries_without_lsi}/{len(cluster_df)}")
        
        if queries_with_lsi == 0:
            print()
            print("❌ ПРОБЛЕМА: У всех запросов кластера нет LSI фраз!")
            print("   Это означает, что LSI фразы не были извлечены из SERP данных")
            print("   Решение: запустите дособор LSI или пересоберите SERP данные")
            conn.close()
            return
        
        if len(all_lsi_phrases) > 0:
            print(f"   Всего LSI фраз в кластере: {len(all_lsi_phrases)}")
            print()
            print("📋 Примеры LSI фраз из кластера (первые 10):")
            phrase_counter = {}
            for item in all_lsi_phrases:
                if isinstance(item, dict):
                    phrase = item.get('phrase', '')
                    if phrase:
                        phrase_counter[phrase] = phrase_counter.get(phrase, 0) + item.get('frequency', 1)
            
            sorted_phrases = sorted(phrase_counter.items(), key=lambda x: x[1], reverse=True)[:10]
            for i, (phrase, freq) in enumerate(sorted_phrases, 1):
                print(f"      {i}. {phrase} (частота: {freq})")
        
        conn.close()
        
        print()
        print("=" * 80)
        print("💡 ВЫВОДЫ:")
        if queries_with_lsi > 0:
            print("   ✓ У кластера есть LSI фразы для агрегации")
            print("   ✓ Проблема может быть в агрегации или экспорте")
            print("   → Запустите пересоздание экспорта: python scripts/utils/rebuild_exports.py <группа>")
        else:
            print("   ❌ У кластера нет LSI фраз для агрегации")
            print("   → Нужно дособрать LSI фразы для запросов кластера")
        print("=" * 80)
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        import traceback
        traceback.print_exc()
        conn.close()
        return


def main():
    if len(sys.argv) < 2:
        print("❌ Укажите запрос: python check_query_lsi.py '<запрос>' [группа]")
        print("   Например: python check_query_lsi.py 'система скуда что это такой'")
        print("   Или: python check_query_lsi.py 'система скуда что это такой' 'скуд'")
        sys.exit(1)
    
    query = sys.argv[1]
    group_name = sys.argv[2] if len(sys.argv) >= 3 else None
    
    # Если группа не указана, но в запросе есть "скуд", пробуем группу "скуд"
    if not group_name and 'скуд' in query.lower():
        print(f"💡 Обнаружено слово 'скуд' в запросе, пробуем группу 'скуд'...")
        print()
        check_query_lsi(query, 'скуд')
    else:
        check_query_lsi(query, group_name)


if __name__ == '__main__':
    main()

