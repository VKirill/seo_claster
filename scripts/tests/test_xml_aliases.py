"""
Тест использования алиасов из city_aliases.xml в GeoValidator
"""

from seo_analyzer.clustering.geo_validator import GeoValidator
from seo_analyzer.core.city_alias_loader import load_city_aliases
import os

# Загружаем гео-словари
geo_dicts = {}
keywords_dir = 'keyword_group'

for geo_type in ['Russian', 'Moscow', 'Kazakhstan']:
    file_path = os.path.join(keywords_dir, f'{geo_type}.txt')
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            cities = {line.strip() for line in f if line.strip()}
            if cities:
                geo_dicts[geo_type] = cities

print("=" * 80)
print("ЗАГРУЗКА АЛИАСОВ ИЗ XML")
print("=" * 80)

xml_aliases = load_city_aliases()
print(f"\nАлиасов загружено из XML: {len(xml_aliases)}")
print("\nПримеры алиасов:")
for alias, full in list(xml_aliases.items())[:10]:
    print(f"  '{alias}' → '{full}'")

print("\n" + "=" * 80)
print("ИНИЦИАЛИЗАЦИЯ GeoValidator")
print("=" * 80)

validator = GeoValidator(geo_dicts=geo_dicts)

print(f"\nvalid_cities: {len(validator.valid_cities)} форм")
print(f"city_aliases: {len(validator.city_aliases)} алиасов")

print("\n" + "=" * 80)
print("ПРОВЕРКА АЛИАСОВ")
print("=" * 80)

# Тестируем алиасы из XML
test_aliases = [
    "спб",
    "питер",
    "мск",
    "москва",
    "москве",
    "петербурге",
    "екатеринбурге",
    "казани",
]

print("\nАлиасы из XML:")
for alias in test_aliases:
    is_valid = validator.is_valid_city(alias)
    normalized = validator.normalize_city(alias) if is_valid else "N/A"
    print(f"  '{alias}': is_valid={is_valid}, normalized='{normalized}'")

print("\n" + "=" * 80)
print("ТЕСТ КЛАСТЕРИЗАЦИИ С АЛИАСАМИ")
print("=" * 80)

from seo_analyzer.clustering.semantic_checker import SemanticClusterChecker

checker = SemanticClusterChecker(geo_dicts=geo_dicts)

test_queries = [
    "видеонаблюдение спб",
    "камеры питер",
    "видеонаблюдение мск",
    "камеры в москве",
    "скуд екатеринбург",
]

print("\nОпределение географии в запросах:")
for query in test_queries:
    geo = checker.extract_geo(query)
    print(f"  '{query}' → {geo}")

# Проверяем совместимость
print("\n" + "=" * 80)
print("ПРОВЕРКА СОВМЕСТИМОСТИ ДЛЯ КЛАСТЕРИЗАЦИИ")
print("=" * 80)

test_pairs = [
    ("видеонаблюдение москва", "камеры в москве"),
    ("видеонаблюдение мск", "камеры в москве"),
    ("видеонаблюдение спб", "камеры питер"),
    ("видеонаблюдение москва", "видеонаблюдение спб"),
]

for q1, q2 in test_pairs:
    compatible, reason = checker.are_queries_compatible(q1, q2, check_geo=True)
    geo1 = checker.extract_geo(q1)
    geo2 = checker.extract_geo(q2)
    print(f"\n'{q1}' (гео: {geo1})")
    print(f"  vs '{q2}' (гео: {geo2})")
    print(f"  → {'✅ Совместимы' if compatible else '❌ Несовместимы'}: {reason}")

