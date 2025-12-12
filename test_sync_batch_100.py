"""
Тест гибридного клиента на 100 запросах из группы "николай_чудотворец"
"""

import sys
import asyncio
import sqlite3
from typing import List

def load_queries_from_group(group_name: str, limit: int = 100) -> List[str]:
    """Загрузить запросы из Master DB"""
    
    db_path = "data/databases/master_queries.db"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем запросы из группы
        cursor.execute('''
            SELECT keyword 
            FROM master_queries 
            WHERE group_name = ?
            ORDER BY keyword
            LIMIT ?
        ''', (group_name, limit))
        
        queries = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print(f"✓ Загружено {len(queries)} запросов из группы '{group_name}'")
        return queries
        
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            print(f"⚠️  Таблица master_queries не существует")
            print(f"   Запустите сначала основной скрипт для создания БД")
            return []
        raise
    except Exception as e:
        print(f"✗ Ошибка загрузки из БД: {e}")
        return []

async def test_sync_batch_client():
    """Тест гибридного клиента"""
    
    print("="*80)
    print("ТЕСТ ГИБРИДНОГО КЛИЕНТА (SyncBatchSERPClient)")
    print("="*80)
    print()
    
    # Загружаем API ключ
    try:
        import config_local
        api_key = config_local.XMLSTOCK_API_KEY
        print(f"✓ API ключ загружен")
    except Exception as e:
        print(f"✗ Ошибка загрузки API ключа: {e}")
        return
    
    print()
    
    # Загружаем запросы
    group_name = "николай_чудотворец"
    queries = load_queries_from_group(group_name, limit=100)
    
    if not queries:
        print(f"\n⚠️  Нет запросов для теста")
        print(f"   Создаём тестовые запросы...")
        # Создаём тестовые запросы на основе темы
        queries = [
            'николай чудотворец',
            'мощи николая чудотворца',
            'мощи николая чудотворца в москве',
            'николай чудотворец икона',
            'святой николай чудотворец',
            'николай чудотворец молитва',
            'храм николая чудотворца',
            'николай чудотворец житие',
            'икона николая чудотворца',
            'николай чудотворец день',
        ]
        print(f"✓ Создано {len(queries)} тестовых запросов")
    
    print()
    print(f"📊 Будет обработано: {len(queries)} запросов")
    print()
    
    # Создаём клиент
    from seo_analyzer.analysis.serp.sync_batch_client import SyncBatchSERPClient
    
    client = SyncBatchSERPClient(
        api_key=api_key,
        lr=213,  # Москва
        max_concurrent_send=10,
        max_concurrent_fetch=20,
        initial_delay=10,
        retry_delay=10,
        requests_per_second=50.0
    )
    
    # Callback для отслеживания req_id
    req_ids = {}
    
    def on_req_id_received(query: str, req_id: str):
        req_ids[query] = req_id
        if len(req_ids) % 10 == 0:
            print(f"   💾 Получено req_id: {len(req_ids)}/{len(queries)}")
    
    try:
        # Запускаем обработку
        result = await client.process_queries_batch(
            queries=queries,
            on_req_id_received=on_req_id_received,
            batch_size=50  # По 50 запросов в батче
        )
        
        # Статистика
        stats = result['stats']
        results = result['results']
        
        print()
        print("="*80)
        print("📊 РЕЗУЛЬТАТЫ ТЕСТА")
        print("="*80)
        print(f"Всего запросов: {stats['total']}")
        print(f"✅ Успешно: {stats['completed']}")
        print(f"❌ Ошибок: {stats['failed']}")
        print(f"📈 Success rate: {stats['completed']/stats['total']*100:.1f}%")
        print()
        
        # Показываем первые 5 успешных результатов
        successful = [r for r in results if r.get('status') == 'completed']
        if successful:
            print("✅ Примеры успешных запросов:")
            for i, res in enumerate(successful[:5], 1):
                query = res['query'][:50] + "..." if len(res['query']) > 50 else res['query']
                xml_len = len(res.get('xml_response', ''))
                print(f"   {i}. '{query}'")
                print(f"      req_id: {res.get('req_id', 'N/A')}")
                print(f"      XML size: {xml_len} байт")
        
        print()
        
        # Показываем ошибки если есть
        failed = [r for r in results if r.get('status') == 'failed']
        if failed:
            print(f"❌ Ошибки ({len(failed)}):")
            for i, res in enumerate(failed[:5], 1):
                query = res['query'][:50] + "..." if len(res['query']) > 50 else res['query']
                error = res.get('error', 'Unknown')[:80]
                print(f"   {i}. '{query}': {error}")
            if len(failed) > 5:
                print(f"   ... и ещё {len(failed) - 5} ошибок")
        
        print()
        print("="*80)
        
        if stats['completed'] > stats['total'] * 0.8:
            print("✅ ТЕСТ ПРОЙДЕН УСПЕШНО (>80% успешных запросов)")
        elif stats['completed'] > 0:
            print("⚠️  ТЕСТ ПРОЙДЕН ЧАСТИЧНО (есть успешные запросы, но много ошибок)")
        else:
            print("❌ ТЕСТ НЕ ПРОЙДЕН (все запросы завершились ошибкой)")
        
        print("="*80)
        
        return stats['completed'] > 0
        
    finally:
        # Закрываем клиент
        await client.close()

if __name__ == "__main__":
    try:
        success = asyncio.run(test_sync_batch_client())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)






