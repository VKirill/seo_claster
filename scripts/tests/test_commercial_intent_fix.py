"""
Тест корректировки коммерческих интентов

Проверяет, что запросы с коммерческими словами (купить, заказать, цена и т.д.)
правильно определяются как commercial, даже если SERP данные говорят иначе.
"""

import re
import pandas as pd
from pathlib import Path
import sys

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from seo_analyzer.classification.intent_classifier import IntentClassifier
from seo_analyzer.core.helpers import KEYWORD_GROUP_DIR, GEO_DICTIONARIES, KEYWORD_DICTIONARIES
from pathlib import Path


def load_text_file(file_path: Path) -> set:
    """Загружает текстовый файл со словами"""
    if not file_path.exists():
        return set()
    
    with open(file_path, 'r', encoding='utf-8') as f:
        words = {line.strip().lower() for line in f if line.strip()}
    
    return words


def load_keyword_dicts() -> dict:
    """Загружает все словари из keyword_group"""
    result = {}
    
    for dict_name, dict_info in KEYWORD_DICTIONARIES.items():
        file_path = KEYWORD_GROUP_DIR / dict_info["file"]
        words = load_text_file(file_path)
        result[dict_name] = {
            "words": words,
            "weight": dict_info["weight"],
            "flag": dict_info["flag"],
        }
    
    return result


def load_geo_dicts() -> dict:
    """Загружает географические словари"""
    result = {}
    
    for geo_name, filename in GEO_DICTIONARIES.items():
        file_path = KEYWORD_GROUP_DIR / filename
        result[geo_name] = load_text_file(file_path)
    
    return result


def test_commercial_intent_detection():
    """Тестирует определение коммерческого интента"""
    
    print("=" * 80)
    print("ТЕСТ: Определение коммерческого интента")
    print("=" * 80)
    print()
    
    # Загружаем словари
    keyword_dicts = load_keyword_dicts()
    geo_dicts = load_geo_dicts()
    
    # Инициализируем классификатор
    classifier = IntentClassifier(keyword_dicts, geo_dicts)
    
    # Тестовые запросы с коммерческими словами
    test_queries = [
        "скуд купить",
        "заказать скуд",
        "система скуд купить",
        "продажа скуд",
        "карта скуд купить",
        "карточки доступа скуд купить",
        "скуд купить комплект",
        "система скуд комплект купить",
        "купить скуд система контроля",
        "контроль доступа скуд купить",
        "контроллер скуд купить",
        "контроллер доступа скуд цена",
        "скуд система контроля доступа купить",
        "продажа оборудования скуд",
        "карты для скуд купить",
        "карты доступа для скуд купить",
        "купить скуд система контроля и управления доступом",
        "скуд система контроля и управления доступом цена",
        "замок скуд купить",
        "скуд замок на дверь купить",
    ]
    
    print("Тестируем запросы:")
    print("-" * 80)
    
    # Получаем коммерческие слова
    commercial_keywords = keyword_dicts.get('commercial', {}).get('words', set())
    
    # Создаем pattern для поиска целых слов
    commercial_patterns = [
        re.compile(r'\b' + re.escape(word.lower()) + r'\b', re.IGNORECASE)
        for word in commercial_keywords
    ]
    
    results = []
    errors = []
    
    for query in test_queries:
        main_intent, scores, flags = classifier.classify_intent(query)
        
        # Проверяем есть ли коммерческое слово (правильная логика)
        has_commercial_word = any(pattern.search(query.lower()) for pattern in commercial_patterns)
        
        result = {
            'query': query,
            'intent': main_intent,
            'has_commercial_word': has_commercial_word,
            'commercial_score': scores.get('commercial', 0),
            'informational_score': scores.get('informational', 0),
        }
        results.append(result)
        
        # Проверяем корректность
        is_commercial = main_intent in ['commercial', 'commercial_geo']
        
        if has_commercial_word and not is_commercial:
            errors.append(f"❌ '{query}' - интент {main_intent}, должен быть commercial")
            print(f"❌ '{query}'")
            print(f"   Интент: {main_intent} (ожидается: commercial)")
            print(f"   Scores: commercial={scores.get('commercial', 0):.2f}, info={scores.get('informational', 0):.2f}")
        else:
            print(f"✅ '{query}' → {main_intent}")
    
    print()
    print("=" * 80)
    print("РЕЗУЛЬТАТЫ")
    print("=" * 80)
    
    total = len(test_queries)
    correct = total - len(errors)
    
    print(f"Всего запросов: {total}")
    print(f"Корректных: {correct}")
    print(f"Ошибок: {len(errors)}")
    print()
    
    if errors:
        print("ОШИБКИ:")
        for error in errors:
            print(f"  {error}")
        print()
        return False
    else:
        print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
        print()
        return True


def test_commercial_word_matching():
    """Тестирует правильность поиска коммерческих слов (целые слова vs подстроки)"""
    
    print("=" * 80)
    print("ТЕСТ: Поиск коммерческих слов (целые слова vs подстроки)")
    print("=" * 80)
    print()
    
    # Загружаем словари
    keyword_dicts = load_keyword_dicts()
    commercial_keywords = keyword_dicts.get('commercial', {}).get('words', set())
    
    # Создаем patterns
    commercial_patterns = [
        re.compile(r'\b' + re.escape(word.lower()) + r'\b', re.IGNORECASE)
        for word in commercial_keywords
    ]
    
    # Тестовые случаи
    test_cases = [
        ("купить скуд", True, "должен найти 'купить'"),
        ("скупить металл", False, "НЕ должен найти 'купить' в 'скупить'"),
        ("выкуп авто", False, "НЕ должен найти 'купить' в 'выкуп'"),
        ("цена товара", True, "должен найти 'цена'"),
        ("оценка стоимости", False, "НЕ должен найти 'цена' в 'оценка'"),
        ("заказать доставку", True, "должен найти 'заказать'"),
        ("показать пример", False, "НЕ должен найти 'заказать' в 'показать'"),
    ]
    
    print("Проверяем корректность поиска слов:")
    print("-" * 80)
    
    errors = []
    
    for query, should_match, description in test_cases:
        has_match = any(pattern.search(query.lower()) for pattern in commercial_patterns)
        
        if has_match == should_match:
            print(f"✅ '{query}' - {description}")
        else:
            error = f"❌ '{query}' - {description} (получили: {has_match})"
            print(error)
            errors.append(error)
    
    print()
    print("=" * 80)
    print("РЕЗУЛЬТАТЫ")
    print("=" * 80)
    
    total = len(test_cases)
    correct = total - len(errors)
    
    print(f"Всего тестов: {total}")
    print(f"Корректных: {correct}")
    print(f"Ошибок: {len(errors)}")
    print()
    
    if errors:
        print("ОШИБКИ:")
        for error in errors:
            print(f"  {error}")
        print()
        return False
    else:
        print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
        print()
        return True


if __name__ == "__main__":
    print()
    print("🧪 ТЕСТИРОВАНИЕ КОММЕРЧЕСКИХ ИНТЕНТОВ")
    print()
    
    # Тест 1: Поиск слов
    test1_passed = test_commercial_word_matching()
    
    # Тест 2: Классификация интентов
    test2_passed = test_commercial_intent_detection()
    
    # Итоговый результат
    print()
    print("=" * 80)
    print("ИТОГОВЫЙ РЕЗУЛЬТАТ")
    print("=" * 80)
    
    if test1_passed and test2_passed:
        print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
        sys.exit(0)
    else:
        print("❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ")
        if not test1_passed:
            print("  - Тест поиска коммерческих слов: ПРОВАЛЕН")
        if not test2_passed:
            print("  - Тест классификации интентов: ПРОВАЛЕН")
        sys.exit(1)

