"""
Тест массовой асинхронной загрузки SERP данных

Демонстрация:
1. Отправка 1000 запросов за несколько секунд
2. Параллельное получение результатов
3. Сохранение req_id в Master DB
"""

import asyncio
from seo_analyzer.analysis.serp.async_batch_client import AsyncBatchSERPClient
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase
from config_local import XMLSTOCK_API_KEY


async def main():
    """Тест асинхронного режима"""
    
    # Тестовые запросы (замени на свои)
    test_queries = [
        "купить видеонаблюдение",
        "монтаж камер",
        "установка видеонаблюдения",
        "ip камеры цена",
        "системы безопасности",
        # ... добавь ещё для теста
    ]
    
    print("=" * 80)
    print("ТЕСТ: Массовая асинхронная загрузка SERP")
    print("=" * 80)
    print(f"Запросов: {len(test_queries)}")
    print(f"API ключ: {XMLSTOCK_API_KEY[:20]}...")
    print()
    
    # Инициализируем клиент
    client = AsyncBatchSERPClient(
        api_key=XMLSTOCK_API_KEY,
        lr=213,  # Москва
        max_concurrent_send=100,  # Отправка 100 параллельно
        max_concurrent_fetch=50,  # Получение 50 параллельно
        initial_delay=10,  # Ждём 10 сек перед первой проверкой
        retry_delay=10,  # Между попытками 10 сек
        max_attempts=10  # Макс 10 попыток
    )
    
    # Master DB для сохранения req_id
    master_db = MasterQueryDatabase()
    group_name = "test_async"
    
    # Callback при получении req_id
    def on_req_id_received(query: str, req_id: str):
        """Сохраняем req_id в Master DB сразу"""
        try:
            master_db.update_serp_status(
                group_name=group_name,
                keyword=query,
                status='processing',
                req_id=req_id
            )
            print(f"   ✓ req_id сохранён: {query[:50]} → {req_id}")
        except Exception as e:
            print(f"   ⚠️  Ошибка сохранения req_id: {e}")
    
    # Progress callback
    def progress_callback(current, total, query, status):
        """Показываем прогресс"""
        if current % 10 == 0:
            print(f"   [{current}/{total}] {status}: {query[:50]}...")
    
    try:
        # Запускаем массовую обработку
        result = await client.process_queries_batch(
            queries=test_queries,
            progress_callback=progress_callback,
            on_req_id_received=on_req_id_received
        )
        
        # Результаты
        print("\n" + "=" * 80)
        print("РЕЗУЛЬТАТЫ:")
        print("=" * 80)
        
        stats = result['stats']
        print(f"Всего запросов: {stats['total']}")
        print(f"Отправлено: {stats['sent']}")
        print(f"Получено: {stats['completed']}")
        print(f"Ошибок отправки: {stats['failed_send']}")
        print(f"Ошибок получения: {stats['failed_fetch']}")
        print()
        
        # Показываем первые 5 успешных
        completed = [r for r in result['results'] if r.get('status') == 'completed']
        if completed:
            print(f"\n✅ Первые {min(5, len(completed))} успешных:")
            for i, res in enumerate(completed[:5], 1):
                xml_len = len(res.get('xml_response', ''))
                print(f"   {i}. {res['query']}")
                print(f"      req_id: {res['req_id']}")
                print(f"      XML size: {xml_len} bytes")
        
        # Показываем ошибки
        errors = [r for r in result['results'] if r.get('status') in ['error', 'failed']]
        if errors:
            print(f"\n❌ Ошибки ({len(errors)}):")
            for i, res in enumerate(errors[:5], 1):
                print(f"   {i}. {res['query']}")
                print(f"      req_id: {res.get('req_id', 'N/A')}")
                print(f"      error: {res.get('error', 'Unknown')[:100]}")
        
        # Проверяем Master DB
        print(f"\n📊 Статистика Master DB:")
        db_stats = master_db.get_serp_statistics(group_name)
        print(f"   Processing: {db_stats['processing']}")
        print(f"   Completed: {db_stats['completed']}")
        print(f"   Errors: {db_stats['error']}")
        
    finally:
        await client.close()
    
    print("\n" + "=" * 80)
    print("Тест завершён!")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

