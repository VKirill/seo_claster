"""
Скрипт для дозаполнения БД с переобработкой XML
Используется когда обновился код обработки и нужно обновить данные в БД
"""

import asyncio
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional

from seo_analyzer.core.serp_database import SERPDatabase
from seo_analyzer.core.serp_data_enricher import SERPDataEnricher
from seo_analyzer.core.lsi_extractor import LSIExtractor


def get_all_queries_with_xml(limit: Optional[int] = None) -> list[tuple]:
    """Получить все запросы с XML из БД"""
    db_path = Path("output/serp_data.db")
    
    if not db_path.exists():
        return []
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        if limit:
            cursor.execute("""
                SELECT id, query, lr, xml_response
                FROM serp_results
                WHERE xml_response IS NOT NULL AND xml_response != ''
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
        else:
            cursor.execute("""
                SELECT id, query, lr, xml_response
                FROM serp_results
                WHERE xml_response IS NOT NULL AND xml_response != ''
                ORDER BY created_at DESC
            """)
        return cursor.fetchall()


def reprocess_xml_data(xml_response: str, query: str) -> Dict[str, Any]:
    """
    Повторно обработать XML с использованием обновленного кода
    
    Args:
        xml_response: Исходный XML от xmlstock
        query: Поисковый запрос
        
    Returns:
        Dict с обработанными данными
    """
    enricher = SERPDataEnricher()
    lsi_extractor = LSIExtractor()
    
    # Обрабатываем XML
    enriched = enricher.enrich_from_serp(xml_response, query)
    
    if enriched.get('error'):
        return None
    
    # Извлекаем LSI
    lsi_phrases = lsi_extractor.extract_from_serp_documents(
        enriched['documents'],
        query
    )
    
    return {
        'metrics': enriched['metrics'],
        'documents': enriched['documents'],
        'lsi_phrases': lsi_phrases
    }


async def refill_database(test_mode: bool = False):
    """Переобработать XML из БД с обновленным кодом"""
    
    print("=" * 80)
    print("ПЕРЕОБРАБОТКА ДАННЫХ В БД С ОБНОВЛЕННЫМ КОДОМ")
    print("=" * 80)
    print()
    
    # Получаем все запросы с XML из БД
    limit = 100 if test_mode else None
    queries_with_xml = get_all_queries_with_xml(limit)
    
    if not queries_with_xml:
        print("❌ В БД нет записей с XML для переобработки")
        return
    
    print(f"✓ Найдено записей с XML: {len(queries_with_xml)}")
    print()
    
    # Инициализируем БД
    db = SERPDatabase()
    
    # Статистика
    stats = {
        'total': len(queries_with_xml),
        'processed': 0,
        'updated': 0,
        'errors': 0
    }
    
    print("🔄 Начинаем переобработку...")
    print()
    
    # Открываем одно соединение для всех операций
    db_path = Path("output/serp_data.db")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        for i, (record_id, query, lr, xml_response) in enumerate(queries_with_xml, 1):
            # Показываем прогресс каждые 100 записей
            if i % 100 == 0:
                print(f"   Обработано: {i}/{stats['total']} "
                      f"(обновлено: {stats['updated']}, ошибок: {stats['errors']})")
                # Commit каждые 100 записей
                conn.commit()
            
            # Переобрабатываем XML с новым кодом
            try:
                reprocessed = reprocess_xml_data(xml_response, query)
                
                if not reprocessed:
                    stats['errors'] += 1
                    print(f"   ⚠️  Ошибка переобработки '{query}' - результат None")
                    continue
            except Exception as e:
                stats['errors'] += 1
                print(f"   ⚠️  Исключение при переобработке '{query}': {e}")
                continue
            
            # Обновляем запись в БД (удаляем старую и создаем новую)
            try:
                # Удаляем старые документы
                cursor.execute("DELETE FROM serp_documents WHERE serp_result_id = ?", (record_id,))
                
                # Удаляем старые LSI фразы
                cursor.execute("DELETE FROM lsi_phrases WHERE serp_result_id = ?", (record_id,))
                
                # Обновляем метрики
                cursor.execute("""
                    UPDATE serp_results
                    SET found_docs = ?,
                        main_pages_count = ?,
                        titles_with_keyword = ?,
                        commercial_domains = ?,
                        info_domains = ?,
                        yandex_ads = ?
                    WHERE id = ?
                """, (
                    reprocessed['metrics'].get('found_docs', 0),
                    reprocessed['metrics'].get('main_pages_count', 0),
                    reprocessed['metrics'].get('titles_with_keyword', 0),
                    reprocessed['metrics'].get('commercial_domains', 0),
                    reprocessed['metrics'].get('info_domains', 0),
                    reprocessed['metrics'].get('yandex_ads', 0),
                    record_id
                ))
                
                # Вставляем новые документы
                for doc in reprocessed['documents']:
                    cursor.execute("""
                        INSERT INTO serp_documents 
                        (serp_result_id, position, url, domain, title, snippet, passages, is_commercial)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record_id,
                        doc.get('position', 0),
                        doc.get('url', ''),
                        doc.get('domain', ''),
                        doc.get('title', ''),
                        doc.get('snippet', ''),
                        doc.get('passages', ''),
                        1 if doc.get('is_commercial', False) else 0
                    ))
                
                # Вставляем новые LSI фразы
                for phrase in reprocessed['lsi_phrases']:
                    cursor.execute("""
                        INSERT INTO lsi_phrases (serp_result_id, phrase, frequency, source)
                        VALUES (?, ?, ?, ?)
                    """, (
                        record_id,
                        phrase.get('phrase', ''),
                        phrase.get('frequency', 0),
                        phrase.get('source', '')
                    ))
                
                stats['updated'] += 1
                stats['processed'] += 1
                
            except Exception as e:
                print(f"   ⚠️  Ошибка обновления БД для '{query}': {e}")
                stats['errors'] += 1
        
        # Финальный commit
        conn.commit()
    
    print()
    print("=" * 80)
    print("ПЕРЕОБРАБОТКА ЗАВЕРШЕНА")
    print("=" * 80)
    print()
    print(f"📊 Статистика:")
    print(f"   Всего записей: {stats['total']}")
    print(f"   Обработано успешно: {stats['processed']}")
    print(f"   Обновлено в БД: {stats['updated']}")
    print(f"   Ошибок: {stats['errors']}")
    print()
    
    # Показываем статистику БД
    db_stats = db.get_statistics()
    print(f"💾 База данных после обновления:")
    print(f"   Всего запросов: {db_stats.get('total_queries', 0)}")
    print(f"   Всего документов: {db_stats.get('total_documents', 0)}")
    print(f"   Размер БД: {db_stats.get('db_size_mb', 0):.2f} MB")
    print()


if __name__ == '__main__':
    import sys
    
    # Проверяем аргументы командной строки
    test_mode = '--test' in sys.argv or '-t' in sys.argv
    
    if test_mode:
        print("🧪 ТЕСТОВЫЙ РЕЖИМ: будет обработано только 100 записей")
        print()
    
    asyncio.run(refill_database(test_mode))

