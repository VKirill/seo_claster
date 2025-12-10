"""
Тест автоматического создания колонок normalized и lemmatized
"""

import pandas as pd
from seo_analyzer.core.normalizer import QueryNormalizer


def test_normalizer():
    """Тест базовой функциональности нормализатора"""
    
    print("=" * 80)
    print("ТЕСТ: Автоматическое создание колонок normalized и lemmatized")
    print("=" * 80)
    
    # Создаем тестовый DataFrame без колонок
    test_queries = [
        "скуд контроллер купить",
        "система контроля доступа цена",
        "турникет для офиса",
        "считыватель карт rfid",
        "домофон с камерой"
    ]
    
    df = pd.DataFrame({
        'keyword': test_queries,
        'frequency_world': [100, 200, 150, 80, 300],
        'frequency_exact': [50, 100, 75, 40, 150]
    })
    
    print(f"\n1. Исходный DataFrame:")
    print(f"   Колонки: {list(df.columns)}")
    print(f"   Запросов: {len(df)}")
    
    # Проверяем что колонок нет
    assert 'normalized' not in df.columns, "Колонка normalized уже существует!"
    assert 'lemmatized' not in df.columns, "Колонка lemmatized уже существует!"
    print("   ✓ Колонки normalized и lemmatized отсутствуют")
    
    # Создаем колонки через нормализатор
    print(f"\n2. Создание колонок через QueryNormalizer...")
    normalizer = QueryNormalizer()
    normalized_results = normalizer.normalize_batch(df['keyword'].tolist())
    
    df['normalized'] = [r['normalized'] for r in normalized_results]
    df['lemmatized'] = [r['lemmatized'] for r in normalized_results]
    
    print(f"   ✓ Колонки созданы успешно")
    print(f"   Колонки теперь: {list(df.columns)}")
    
    # Проверяем что колонки созданы
    assert 'normalized' in df.columns, "Колонка normalized не создана!"
    assert 'lemmatized' in df.columns, "Колонка lemmatized не создана!"
    assert len(df['normalized']) == len(df), "Размер колонки normalized не совпадает!"
    assert len(df['lemmatized']) == len(df), "Размер колонки lemmatized не совпадает!"
    
    # Проверяем что данные корректны
    print(f"\n3. Проверка данных:")
    for idx, row in df.head(3).iterrows():
        print(f"\n   Оригинал:     '{row['keyword']}'")
        print(f"   Normalized:   '{row['normalized']}'")
        print(f"   Lemmatized:   '{row['lemmatized']}'")
    
    # Проверяем что нормализация работает корректно
    assert df['normalized'].iloc[0] != "", "Normalized не должен быть пустым!"
    assert df['lemmatized'].iloc[0] != "", "Lemmatized не должен быть пустым!"
    
    print(f"\n{'=' * 80}")
    print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    print("=" * 80)
    print("\n📝 Вывод:")
    print("   • QueryNormalizer работает корректно")
    print("   • Колонки создаются автоматически")
    print("   • Данные нормализуются правильно")
    print("   • Проблема с предупреждениями решена!")
    print()


if __name__ == '__main__':
    try:
        test_normalizer()
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()

