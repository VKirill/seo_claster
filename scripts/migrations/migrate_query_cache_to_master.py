"""
Миграция данных из query_cache.db в master_queries.db

Переносит:
- normalized, lemmatized
- main_words, key_phrase
- ner_entities, ner_locations
- intent данные (если есть)

После миграции query_cache.db становится необязательной (legacy fallback).
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime


def migrate_query_cache_to_master():
    """
    Миграция всех данных из query_cache в Master DB
    """
    query_cache_path = Path("output/query_cache.db")
    master_db_path = Path("output/master_queries.db")
    
    if not query_cache_path.exists():
        print("❌ query_cache.db не найден")
        return
    
    if not master_db_path.exists():
        print("❌ master_queries.db не найден - создайте его сначала")
        return
    
    print("=" * 80)
    print("МИГРАЦИЯ: query_cache.db → master_queries.db")
    print("=" * 80)
    print()
    
    # Подключаемся
    cache_conn = sqlite3.connect(query_cache_path)
    cache_conn.row_factory = sqlite3.Row
    master_conn = sqlite3.connect(master_db_path)
    
    try:
        # Получаем список групп из query_cache
        cache_cursor = cache_conn.cursor()
        cache_cursor.execute("SELECT DISTINCT group_name FROM cached_queries")
        groups = [row[0] for row in cache_cursor.fetchall()]
        
        print(f"📦 Найдено групп в query_cache: {len(groups)}")
        print()
        
        total_migrated = 0
        total_updated = 0
        total_skipped = 0
        
        for group_name in groups:
            print(f"🔄 Группа: {group_name}")
            
            # Получаем все запросы из query_cache
            cache_cursor.execute("""
                SELECT 
                    keyword,
                    normalized,
                    lemmatized,
                    main_words,
                    key_phrase,
                    entities_json,
                    main_intent,
                    commercial_score,
                    informational_score,
                    navigational_score,
                    is_commercial,
                    is_wholesale,
                    is_urgent,
                    is_diy,
                    is_review,
                    is_brand_query,
                    has_geo,
                    geo_type,
                    geo_country,
                    geo_city
                FROM cached_queries
                WHERE group_name = ?
            """, (group_name,))
            
            cached_queries = cache_cursor.fetchall()
            print(f"   Запросов в кэше: {len(cached_queries)}")
            
            master_cursor = master_conn.cursor()
            
            migrated = 0
            updated = 0
            skipped = 0
            
            for row in cached_queries:
                keyword = row['keyword']
                
                # Проверяем есть ли уже в Master DB
                master_cursor.execute("""
                    SELECT id, normalized, lemmatized, main_intent
                    FROM master_queries
                    WHERE group_name = ? AND keyword = ?
                """, (group_name, keyword))
                
                existing = master_cursor.fetchone()
                
                if existing:
                    # Запрос уже есть - проверяем нужно ли обновить
                    existing_id = existing[0]
                    existing_normalized = existing[1]
                    existing_lemmatized = existing[2]
                    existing_intent = existing[3]
                    
                    # Обновляем только если данных нет
                    needs_update = False
                    
                    if not existing_normalized and row['normalized']:
                        needs_update = True
                    if not existing_lemmatized and row['lemmatized']:
                        needs_update = True
                    if not existing_intent and row['main_intent']:
                        needs_update = True
                    
                    if needs_update:
                        # Обновляем
                        master_cursor.execute("""
                            UPDATE master_queries
                            SET
                                normalized = COALESCE(normalized, ?),
                                lemmatized = COALESCE(lemmatized, ?),
                                main_words = COALESCE(main_words, ?),
                                key_phrase = COALESCE(key_phrase, ?),
                                ner_entities = COALESCE(ner_entities, ?),
                                main_intent = COALESCE(main_intent, ?),
                                commercial_score = COALESCE(commercial_score, ?),
                                informational_score = COALESCE(informational_score, ?),
                                navigational_score = COALESCE(navigational_score, ?),
                                is_commercial = COALESCE(is_commercial, ?),
                                is_wholesale = COALESCE(is_wholesale, ?),
                                is_urgent = COALESCE(is_urgent, ?),
                                is_diy = COALESCE(is_diy, ?),
                                is_review = COALESCE(is_review, ?),
                                is_brand_query = COALESCE(is_brand_query, ?),
                                has_geo = COALESCE(has_geo, ?),
                                geo_type = COALESCE(geo_type, ?),
                                geo_country = COALESCE(geo_country, ?),
                                geo_city = COALESCE(geo_city, ?)
                            WHERE id = ?
                        """, (
                            row['normalized'],
                            row['lemmatized'],
                            row['main_words'],
                            row['key_phrase'],
                            row['entities_json'],
                            row['main_intent'],
                            row['commercial_score'],
                            row['informational_score'],
                            row['navigational_score'],
                            row['is_commercial'],
                            row['is_wholesale'],
                            row['is_urgent'],
                            row['is_diy'],
                            row['is_review'],
                            row['is_brand_query'],
                            row['has_geo'],
                            row['geo_type'],
                            row['geo_country'],
                            row['geo_city'],
                            existing_id
                        ))
                        updated += 1
                    else:
                        skipped += 1
                
                else:
                    # Запроса нет - добавляем
                    master_cursor.execute("""
                        INSERT INTO master_queries (
                            group_name, keyword,
                            normalized, lemmatized, main_words, key_phrase,
                            ner_entities,
                            main_intent, commercial_score, informational_score, navigational_score,
                            is_commercial, is_wholesale, is_urgent, is_diy, is_review, is_brand_query,
                            has_geo, geo_type, geo_country, geo_city
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        group_name, keyword,
                        row['normalized'],
                        row['lemmatized'],
                        row['main_words'],
                        row['key_phrase'],
                        row['entities_json'],
                        row['main_intent'],
                        row['commercial_score'],
                        row['informational_score'],
                        row['navigational_score'],
                        row['is_commercial'],
                        row['is_wholesale'],
                        row['is_urgent'],
                        row['is_diy'],
                        row['is_review'],
                        row['is_brand_query'],
                        row['has_geo'],
                        row['geo_type'],
                        row['geo_country'],
                        row['geo_city']
                    ))
                    migrated += 1
            
            master_conn.commit()
            
            print(f"   ✓ Мигрировано: {migrated}")
            print(f"   ✓ Обновлено: {updated}")
            print(f"   ⚠️  Пропущено (уже есть): {skipped}")
            print()
            
            total_migrated += migrated
            total_updated += updated
            total_skipped += skipped
        
        print("=" * 80)
        print("ИТОГО:")
        print(f"  Групп обработано: {len(groups)}")
        print(f"  Записей добавлено: {total_migrated}")
        print(f"  Записей обновлено: {total_updated}")
        print(f"  Записей пропущено: {total_skipped}")
        print("=" * 80)
        print()
        
        if total_migrated > 0 or total_updated > 0:
            print("✅ Миграция завершена успешно!")
            print()
            print("📝 Что дальше:")
            print("   1. Проверь данные: python -c \"from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase; db = MasterQueryDatabase(); print(db.get_all_statistics())\"")
            print("   2. query_cache.db теперь необязательна (legacy fallback)")
            print("   3. Можешь удалить query_cache.db если уверен что всё работает")
            print()
        else:
            print("ℹ️  Миграция не требуется - все данные уже в Master DB")
    
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        cache_conn.close()
        master_conn.close()


if __name__ == "__main__":
    migrate_query_cache_to_master()

