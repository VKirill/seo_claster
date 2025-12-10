"""
Тестирование производительности оптимизированной пост-обработки кластеров.

Создает синтетические данные и измеряет время выполнения.
"""

import time
import pandas as pd
import sys
from typing import Dict, List

# Очищаем кэш модулей для использования свежего кода
for module_name in list(sys.modules.keys()):
    if 'seo_analyzer.clustering' in module_name:
        del sys.modules[module_name]

from seo_analyzer.clustering.cluster_postprocessor import ClusterPostprocessor


def generate_test_data(num_queries: int = 1000, urls_per_query: int = 20) -> pd.DataFrame:
    """
    Генерирует тестовые данные для кластеризации.
    
    Args:
        num_queries: Количество запросов
        urls_per_query: Количество URL на запрос
    
    Returns:
        DataFrame с тестовыми запросами
    """
    print(f"🔧 Генерация тестовых данных: {num_queries} запросов...")
    
    data = []
    # Создаем пул URL (имитируем реальное распределение)
    url_pool = [f"https://example{i}.com" for i in range(100)]
    
    import random
    random.seed(42)  # Фиксируем seed для воспроизводимости
    
    for i in range(num_queries):
        # Имитируем кластеры - группы запросов с похожими URL
        cluster_id = i // 10  # Примерно 10 запросов на кластер
        
        # Генерируем URL для запроса (часть общих, часть уникальных)
        if cluster_id < len(url_pool):
            # Берем базовые URL кластера
            base_urls = url_pool[cluster_id:min(cluster_id + 10, len(url_pool))]
            # Добавляем немного случайных URL
            random_urls = random.sample(url_pool, min(10, len(url_pool)))
            urls = base_urls + random_urls
        else:
            urls = random.sample(url_pool, min(urls_per_query, len(url_pool)))
        
        data.append({
            'keyword': f'запрос {i}',
            'semantic_cluster_id': cluster_id,
            'serp_urls': '|'.join(urls[:urls_per_query]),
            'frequency_world': 100 - i % 100,
        })
    
    df = pd.DataFrame(data)
    print(f"✅ Создано {len(df)} запросов в {df['semantic_cluster_id'].nunique()} кластерах")
    return df


def test_performance(df: pd.DataFrame, skip_reattach: bool = False):
    """
    Тестирует производительность пост-обработки.
    
    Args:
        df: DataFrame с данными
        skip_reattach: Пропустить прикрепление одиночек
    """
    mode = "БЕЗ прикрепления одиночек" if skip_reattach else "С прикреплением одиночек"
    print(f"\n{'='*60}")
    print(f"⚡ Тест производительности ({mode})")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        processor = ClusterPostprocessor(
            base_threshold=7,
            top_positions=30,
            max_cluster_size=12,
            threshold_step=1,
            skip_singleton_reattach=skip_reattach,
        )
        
        result_df = processor.process(df.copy())
        
        elapsed = time.time() - start_time
        
        stats = processor.get_stats()
        
        print(f"⏱️  Время выполнения: {elapsed:.2f} секунд")
        print(f"📊 Результаты:")
        print(f"   • Кластеров: {stats['total_clusters']}")
        print(f"   • Максимальный размер: {stats['max_cluster_size']}")
        print(f"   • Одиночных кластеров: {stats['singleton_clusters']}")
        print(f"   • Обработано запросов: {len(result_df)}")
        
        return elapsed, stats
        
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return None, None


def main():
    """Запуск тестов производительности."""
    print("🚀 Тестирование оптимизированной пост-обработки кластеров")
    print()
    
    # Тест 1: Маленький набор данных
    print("="*60)
    print("📦 Тест 1: Маленький набор (500 запросов)")
    print("="*60)
    df_small = generate_test_data(num_queries=500)
    
    time_without, stats_without = test_performance(df_small.copy(), skip_reattach=True)
    time_with, stats_with = test_performance(df_small.copy(), skip_reattach=False)
    
    if time_with and time_without:
        speedup = time_with / time_without if time_without > 0 else 0
        print(f"\n⚡ Замедление при прикреплении одиночек: {speedup:.2f}x")
        print(f"   (прикрепление одиночек занимает {time_with - time_without:.2f} сек)")
    
    # Тест 2: Средний набор данных
    print("\n" + "="*60)
    print("📦 Тест 2: Средний набор (2000 запросов)")
    print("="*60)
    df_medium = generate_test_data(num_queries=2000)
    
    time_without, stats_without = test_performance(df_medium.copy(), skip_reattach=True)
    time_with, stats_with = test_performance(df_medium.copy(), skip_reattach=False)
    
    if time_with and time_without:
        speedup = time_with / time_without if time_without > 0 else 0
        print(f"\n⚡ Замедление при прикреплении одиночек: {speedup:.2f}x")
        print(f"   (прикрепление одиночек занимает {time_with - time_without:.2f} сек)")
    
    # Тест 3: Большой набор данных
    print("\n" + "="*60)
    print("📦 Тест 3: Большой набор (5000 запросов)")
    print("="*60)
    df_large = generate_test_data(num_queries=5000)
    
    time_without, stats_without = test_performance(df_large.copy(), skip_reattach=True)
    time_with, stats_with = test_performance(df_large.copy(), skip_reattach=False)
    
    if time_with and time_without:
        speedup = time_with / time_without if time_without > 0 else 0
        print(f"\n⚡ Замедление при прикреплении одиночек: {speedup:.2f}x")
        print(f"   (прикрепление одиночек занимает {time_with - time_without:.2f} сек)")
    
    print("\n" + "="*60)
    print("✅ Все тесты завершены!")
    print("="*60)
    print("\n💡 Рекомендация:")
    print("   Если одиночных кластеров много (>500), используйте --skip-singleton-reattach")
    print("   для ускорения пост-обработки в 2-5 раз!")


if __name__ == "__main__":
    main()

