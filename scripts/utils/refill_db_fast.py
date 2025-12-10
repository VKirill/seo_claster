"""
Быстрая переобработка БД с параллельной обработкой XML
"""

import asyncio
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

from seo_analyzer.core.serp_data_enricher import SERPDataEnricher
from seo_analyzer.core.lsi_extractor import LSIExtractor  # Теперь все экстракторы быстрые!


def process_xml_chunk(chunk_data: list[tuple]) -> list[Dict[str, Any]]:
    """
    Обработать пакет XML в отдельном процессе
    
    Args:
        chunk_data: Список (record_id, query, lr, xml_response)
        
    Returns:
        Список обработанных данных
    """
    enricher = SERPDataEnricher()
    lsi_extractor = LSIExtractor()  # Теперь все используют кэшированную лемматизацию!
    
    results = []
    
    for record_id, query, lr, xml_response in chunk_data:
        try:
            # Обрабатываем XML
            enriched = enricher.enrich_from_serp(xml_response, query)
            
            if enriched.get('error'):
                results.append({
                    'record_id': record_id,
                    'query': query,
                    'success': False,
                    'error': enriched['error']
                })
                continue
            
            # Извлекаем LSI с кэшированием
            lsi_phrases = lsi_extractor.extract_from_serp_documents(
                enriched['documents'],
                query
            )
            
            results.append({
                'record_id': record_id,
                'query': query,
                'success': True,
                'metrics': enriched['metrics'],
                'documents': enriched['documents'],
                'lsi_phrases': lsi_phrases
            })
            
        except Exception as e:
            results.append({
                'record_id': record_id,
                'query': query,
                'success': False,
                'error': str(e)
            })
    
    return results


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


def chunk_list(lst: list, chunk_size: int):
    """Разбить список на чанки"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


async def refill_database_fast(test_mode: bool = False, workers: int = None):
    """
    Быстрая переобработка с параллельной обработкой
    
    Args:
        test_mode: Обработать только 100 записей
        workers: Количество процессов (по умолчанию = CPU cores)
    """
    
    print("=" * 80)
    print("БЫСТРАЯ ПЕРЕОБРАБОТКА ДАННЫХ В БД (ПАРАЛЛЕЛЬНАЯ)")
    print("=" * 80)
    print()
    
    # Определяем количество воркеров
    if workers is None:
        workers = max(1, mp.cpu_count() - 1)  # Оставляем 1 ядро свободным
    
    print(f"🚀 Используем {workers} параллельных процессов")
    print()
    
    # Получаем все запросы с XML из БД
    limit = 100 if test_mode else None
    queries_with_xml = get_all_queries_with_xml(limit)
    
    if not queries_with_xml:
        print("❌ В БД нет записей с XML для переобработки")
        return
    
    print(f"✓ Найдено записей с XML: {len(queries_with_xml)}")
    print()
    
    # Разбиваем на чанки для параллельной обработки
    chunk_size = 50  # Обрабатываем по 50 записей в каждом процессе
    chunks = list(chunk_list(queries_with_xml, chunk_size))
    
    print(f"📦 Разбито на {len(chunks)} пакетов по {chunk_size} записей")
    print()
    
    # Статистика
    stats = {
        'total': len(queries_with_xml),
        'processed': 0,
        'updated': 0,
        'errors': 0
    }
    
    print("🔄 Начинаем параллельную обработку...")
    print()
    
    # Обрабатываем пакеты параллельно
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Запускаем обработку всех чанков
        loop = asyncio.get_event_loop()
        
        for chunk_idx, chunk in enumerate(chunks):
            # Обрабатываем чанк в отдельном процессе
            future = loop.run_in_executor(executor, process_xml_chunk, chunk)
            processed_results = await future
            
            # Обновляем БД
            db_path = Path("output/serp_data.db")
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                for result in processed_results:
                    if not result['success']:
                        stats['errors'] += 1
                        print(f"   ⚠️  Ошибка '{result['query']}': {result.get('error', 'Unknown')}")
                        continue
                    
                    try:
                        record_id = result['record_id']
                        
                        # Удаляем старые данные
                        cursor.execute("DELETE FROM serp_documents WHERE serp_result_id = ?", (record_id,))
                        cursor.execute("DELETE FROM serp_lsi_mapping WHERE serp_result_id = ?", (record_id,))
                        
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
                            result['metrics'].get('found_docs', 0),
                            result['metrics'].get('main_pages_count', 0),
                            result['metrics'].get('titles_with_keyword', 0),
                            result['metrics'].get('commercial_domains', 0),
                            result['metrics'].get('info_domains', 0),
                            result['metrics'].get('yandex_ads', 0),
                            record_id
                        ))
                        
                        # Batch insert документов
                        docs_data = [
                            (
                                record_id,
                                doc.get('position', 0),
                                doc.get('url', ''),
                                doc.get('domain', ''),
                                doc.get('title', ''),
                                doc.get('snippet', ''),
                                doc.get('passages', ''),
                                1 if doc.get('is_commercial', False) else 0
                            )
                            for doc in result['documents']
                        ]
                        
                        if docs_data:
                            cursor.executemany("""
                                INSERT INTO serp_documents 
                                (serp_result_id, position, url, domain, title, snippet, passages, is_commercial)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, docs_data)
                        
                        # Batch insert LSI фраз (НОВАЯ НОРМАЛИЗОВАННАЯ СХЕМА)
                        for phrase_data in result['lsi_phrases']:
                            phrase_text = phrase_data.get('phrase', '')
                            frequency = phrase_data.get('frequency', 1)
                            source = phrase_data.get('source', 'unknown')
                            
                            # 1. Вставляем или получаем phrase_id из unique_lsi_phrases
                            cursor.execute("""
                                INSERT OR IGNORE INTO unique_lsi_phrases (phrase, total_frequency)
                                VALUES (?, 0)
                            """, (phrase_text,))
                            
                            cursor.execute("""
                                SELECT id FROM unique_lsi_phrases WHERE phrase = ?
                            """, (phrase_text,))
                            
                            phrase_id = cursor.fetchone()[0]
                            
                            # 2. Обновляем total_frequency
                            cursor.execute("""
                                UPDATE unique_lsi_phrases 
                                SET total_frequency = total_frequency + ?
                                WHERE id = ?
                            """, (frequency, phrase_id))
                            
                            # 3. Создаем связь в serp_lsi_mapping
                            cursor.execute("""
                                INSERT OR REPLACE INTO serp_lsi_mapping (
                                    serp_result_id, phrase_id, frequency, source
                                ) VALUES (?, ?, ?, ?)
                            """, (record_id, phrase_id, frequency, source))
                        
                        stats['updated'] += 1
                        
                    except Exception as e:
                        print(f"   ⚠️  Ошибка БД для '{result['query']}': {e}")
                        stats['errors'] += 1
                
                # Commit после каждого чанка
                conn.commit()
                stats['processed'] += len(chunk)
            
            # Показываем прогресс
            progress_pct = (stats['processed'] / stats['total']) * 100
            print(f"   Обработано: {stats['processed']}/{stats['total']} "
                  f"({progress_pct:.1f}%) | Обновлено: {stats['updated']} | Ошибок: {stats['errors']}")
    
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


if __name__ == '__main__':
    import sys
    
    # Проверяем аргументы
    test_mode = '--test' in sys.argv or '-t' in sys.argv
    
    # Количество воркеров
    workers = None
    for arg in sys.argv:
        if arg.startswith('--workers='):
            workers = int(arg.split('=')[1])
    
    if test_mode:
        print("🧪 ТЕСТОВЫЙ РЕЖИМ: будет обработано только 100 записей")
        print()
    
    asyncio.run(refill_database_fast(test_mode, workers))

