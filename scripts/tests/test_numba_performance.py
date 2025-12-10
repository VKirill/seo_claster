"""
Тест производительности Numba оптимизаций для кластеризации.

Сравнивает скорость вычисления схожести URL:
1. Старый метод (set intersection)
2. Новый метод (Numba JIT)
"""
import time
from typing import List
import random

# Генерация тестовых данных
def generate_test_urls(n_queries: int = 1000, n_urls_per_query: int = 30) -> List[List[str]]:
    """Генерирует тестовые URL для бенчмарка"""
    url_pool = [f"example{i}.com/page{j}" for i in range(100) for j in range(50)]
    
    queries_urls = []
    for _ in range(n_queries):
        # Случайная выборка URL для каждого запроса
        urls = random.sample(url_pool, n_urls_per_query)
        queries_urls.append(urls)
    
    return queries_urls


def benchmark_old_method(queries_urls: List[List[str]]) -> float:
    """Бенчмарк старого метода (set intersection)"""
    start = time.time()
    
    total_similarity = 0
    n_comparisons = 0
    
    # Сравниваем каждую пару запросов
    for i in range(len(queries_urls)):
        for j in range(i + 1, min(i + 50, len(queries_urls))):  # Первые 50 сравнений
            set1 = set(queries_urls[i])
            set2 = set(queries_urls[j])
            common = len(set1 & set2)
            total_similarity += common
            n_comparisons += 1
    
    elapsed = time.time() - start
    return elapsed, n_comparisons


def benchmark_numba_method(queries_urls: List[List[str]]) -> float:
    """Бенчмарк Numba метода"""
    try:
        from seo_analyzer.clustering.fast_similarity import FastSimilarityCalculator
    except ImportError:
        print("❌ Не удалось импортировать FastSimilarityCalculator")
        return 0.0, 0
    
    calculator = FastSimilarityCalculator(top_positions=30, position_weights=False)
    
    start = time.time()
    
    total_similarity = 0
    n_comparisons = 0
    
    # Сравниваем каждую пару запросов
    for i in range(len(queries_urls)):
        for j in range(i + 1, min(i + 50, len(queries_urls))):
            common = calculator.calculate_simple_similarity(
                queries_urls[i], 
                queries_urls[j]
            )
            total_similarity += common
            n_comparisons += 1
    
    elapsed = time.time() - start
    return elapsed, n_comparisons


def main():
    print("=" * 70)
    print("🚀 ТЕСТ ПРОИЗВОДИТЕЛЬНОСТИ ОПТИМИЗАЦИЙ КЛАСТЕРИЗАЦИИ")
    print("=" * 70)
    print("ℹ️  Тестируем нативный Python set intersection + алгоритмические оптимизации")
    print("   (Numba оказался медленнее, поэтому не используется)")
    
    print("\n📊 Генерация тестовых данных...")
    n_queries = 1000
    queries_urls = generate_test_urls(n_queries=n_queries, n_urls_per_query=30)
    print(f"   Создано {n_queries} запросов с {len(queries_urls[0])} URL каждый")
    
    print("\n⏱️  Тест 1: Старый метод (Python set intersection)...")
    old_time, old_comps = benchmark_old_method(queries_urls)
    print(f"   Время: {old_time:.2f} сек")
    print(f"   Сравнений: {old_comps:,}")
    print(f"   Скорость: {old_comps / old_time:.0f} сравнений/сек")
    
    print("\n⏱️  Тест 2: Новый метод (Numba JIT компиляция)...")
    new_time, new_comps = benchmark_numba_method(queries_urls)
    print(f"   Время: {new_time:.2f} сек")
    print(f"   Сравнений: {new_comps:,}")
    print(f"   Скорость: {new_comps / new_time:.0f} сравнений/сек")
    
    print("\n" + "=" * 70)
    if new_time > 0:
        speedup = old_time / new_time
        print(f"🎯 РЕЗУЛЬТАТ: Ускорение в {speedup:.1f}x раз!")
        
        if speedup > 5:
            print("   ✅ ОТЛИЧНО! Значительное ускорение")
        elif speedup > 2:
            print("   ✅ ХОРОШО! Заметное ускорение")
        elif speedup > 1.2:
            print("   ⚠️  Умеренное ускорение")
        else:
            print("   ⚠️  Слабое ускорение - возможно Numba не скомпилировался")
        
        # Экстраполяция на реальные данные
        print(f"\n📈 Экстраполяция на 56,000 запросов:")
        print(f"   Старый метод: ~{(56000 / n_queries) * old_time / 60:.1f} минут")
        print(f"   Новый метод: ~{(56000 / n_queries) * new_time / 60:.1f} минут")
        print(f"   Экономия времени: ~{((56000 / n_queries) * (old_time - new_time)) / 60:.1f} минут")
    
    print("=" * 70)


if __name__ == "__main__":
    main()

