# Справочник: Система групп запросов

## 📁 Структура файлов

```
seo_claster/
├── semantika/                        # Входные файлы групп
│   ├── skud.csv
│   ├── crm.csv
│   └── ...
│
├── output/
│   ├── groups/                       # Результаты по группам
│   │   ├── skud/
│   │   │   ├── serp_data.db         # БД SERP данных группы
│   │   │   ├── serp_cache/          # Кэш XML
│   │   │   ├── seo_analysis.xlsx    # Excel отчет
│   │   │   ├── seo_analysis_full.csv
│   │   │   ├── clusters_summary.csv
│   │   │   ├── brands.csv
│   │   │   └── dashboard.html
│   │   └── crm/
│   │       └── ...
│   │
│   └── global_domain_stats.db        # Общая БД классификации
│
├── main.py                           # Основной запуск
├── manage_groups.py                  # Управление группами
└── groups.bat                        # Интерактивное меню
```

## 🔧 CLI Команды

### Просмотр групп

```bash
# Список всех групп
python main.py --list-groups

# Управление через скрипт
python manage_groups.py list
```

### Обработка

```bash
# Одна группа
python main.py --group <название>

# Все группы
python main.py --process-all-groups

# С дополнительными опциями
python main.py --group skud --serp-mode strict --export-brands
```

### Статистика

```bash
# Глобальная статистика доменов
python manage_groups.py stats

# Информация о конкретном домене
python manage_groups.py domain ozon.ru
```

## 📊 База данных

### Единая БД (output/serp_data.db)

Все данные хранятся в одной БД: `output/serp_data.db`

#### Таблица: domain_group_stats
Статистика доменов по группам:

| Поле | Тип | Описание |
|------|-----|----------|
| domain | TEXT | Домен |
| query_group | TEXT | Название группы |
| commercial_count | INTEGER | Кол-во коммерческих запросов |
| informational_count | INTEGER | Кол-во информационных запросов |
| total_queries | INTEGER | Всего запросов |
| first_seen | TIMESTAMP | Первое появление |
| last_updated | TIMESTAMP | Последнее обновление |

#### Таблица: domain_global_stats
Агрегированная статистика по всем группам:

| Поле | Тип | Описание |
|------|-----|----------|
| domain | TEXT | Домен |
| total_commercial | INTEGER | Всего коммерческих |
| total_informational | INTEGER | Всего информационных |
| total_queries | INTEGER | Всего запросов |
| groups_count | INTEGER | В скольких группах встречается |
| is_commercial | BOOLEAN | Классификация |
| commercial_ratio | REAL | Коэффициент коммерциализации |
| confidence_score | REAL | Уверенность классификации |

#### Таблица: domain_stats
Старая таблица для классификации доменов (из create_domain_stats_table.py)

#### Стандартные SERP таблицы:
- `serp_results` - Запросы и XML ответы
- `serp_documents` - Документы выдачи
- `serp_lsi_phrases` - LSI фразы

### БД группы (groups/{name}/serp_data.db)

Каждая группа имеет свою БД с SERP данными:
- `serp_results` - Запросы и XML ответы
- `serp_documents` - Документы выдачи
- `serp_lsi_phrases` - LSI фразы

## 🔄 Workflow

### 1. Создание группы

```bash
# Способ 1: Вручную
echo "Запрос;frequency_world;frequency_exact" > semantika/new_group.csv
echo "запрос 1;1000;800" >> semantika/new_group.csv

# Способ 2: Через bat-скрипт
groups.bat  # Выбрать "6. Создать новую группу"

# Способ 3: Скопировать существующий
copy semantika\skud.csv semantika\new_group.csv
```

### 2. Обработка

```bash
# Сначала одна группа (тест)
python main.py --group new_group

# Затем все группы (производство)
python main.py --process-all-groups
```

### 3. Анализ результатов

```bash
# Проверка output
dir output\groups\new_group\

# Статистика доменов
python manage_groups.py stats

# Проверка конкретного домена
python manage_groups.py domain market.yandex.ru
```

## 🎓 Примеры

### Пример 1: Новая группа с нуля

```bash
# 1. Создаем файл
notepad semantika\electronics.csv

# Содержимое:
# Запрос;frequency_world;frequency_exact
# купить ноутбук;5000;4000
# смартфон цена;3000;2500
# наушники беспроводные;2000;1600

# 2. Обрабатываем
python main.py --group electronics

# 3. Результаты
explorer output\groups\electronics\
```

### Пример 2: Пакетная обработка с отчетом

```bash
# Обработать все группы
python main.py --process-all-groups > processing_log.txt 2>&1

# Посмотреть статистику
python manage_groups.py stats

# Сохранить отчет
python manage_groups.py stats > domain_stats_report.txt
```

### Пример 3: Анализ конкретного домена

```bash
# Узнать классификацию домена после нескольких групп
python manage_groups.py domain ozon.ru

# Вывод:
# 📊 Информация о домене: ozon.ru
# 
# Агрегированная статистика:
#   Классификация: Коммерческий
#   Коммерциализация: 94.2%
#   Confidence: 0.95
#   Всего запросов: 2341
#   Коммерческих: 2205
#   Информационных: 136
#   Групп: 5
# 
# Статистика по группам:
#   skud                  850 запросов (К: 810, И: 40)
#   crm                   645 запросов (К: 620, И: 25)
#   electronics           846 запросов (К: 775, И: 71)
```

## 🔍 API

### Python API

```python
from seo_analyzer.core.query_groups import (
    QueryGroupManager,
    QueryGroup,
    GroupDatabaseManager
)

# Менеджер групп
manager = QueryGroupManager()
groups = manager.discover_groups()

# Работа с группой
group = manager.get_group("skud")
df = manager.load_queries(group)

# БД группы
db_manager = GroupDatabaseManager(group.db_path)
db_manager.update_domain_stats("ozon.ru", "skud", is_commercial=True)
classification = db_manager.get_domain_classification("ozon.ru")
```

### Классификатор доменов

```python
from seo_analyzer.core.domain_classifier_enhanced import EnhancedDomainClassifier

classifier = EnhancedDomainClassifier(
    group_db_path=group.db_path,
    use_global_db=True
)

# Простая классификация
result = classifier.classify_domain("ozon.ru")  # 'commercial' | 'informational'

# Подробная информация
info = classifier.get_classification_info("ozon.ru")
# {
#     'domain': 'ozon.ru',
#     'classification': 'commercial',
#     'source': 'global_db',  # 'global_db' | 'group_db' | 'txt_file'
#     'confidence': 0.95,
#     'total_queries': 2341,
#     'groups_count': 5,
#     'commercial_ratio': 0.942
# }
```

## ⚙️ Конфигурация

### Настройки по умолчанию

```python
# Пути
SEMANTIKA_DIR = Path("semantika")
OUTPUT_DIR = Path("output")
GROUPS_DIR = Path("output/groups")
GLOBAL_DB = Path("output/global_domain_stats.db")

# Порог коммерциализации
COMMERCIAL_THRESHOLD = 0.6  # 60%

# Минимальная уверенность
MIN_CONFIDENCE = 0.5

# Количество запросов для полной уверенности
FULL_CONFIDENCE_QUERIES = 100
```

### Переопределение в config_local.py

```python
# config_local.py
GROUPS_CONFIG_OVERRIDES = {
    'commercial_threshold': 0.7,  # Строже
    'min_confidence': 0.6,
}
```

## 🚨 Troubleshooting

### Группа не обнаруживается

**Проблема:** `python main.py --list-groups` не показывает группу

**Решение:**
```bash
# Проверьте расположение файла
dir semantika\

# Проверьте расширение (должно быть .csv)
dir semantika\*.csv

# Проверьте формат имени (латиница, без пробелов)
ren "semantika\моя группа.csv" semantika\my_group.csv
```

### Ошибка при обработке

**Проблема:** Ошибка при `python main.py --group skud`

**Решение:**
```bash
# Проверьте формат CSV
type semantika\skud.csv

# Должно быть:
# Запрос;frequency_world;frequency_exact
# запрос 1;1000;800

# Проверьте кодировку (UTF-8)
# Откройте в Notepad++: Кодировка → UTF-8
```

### Глобальная БД не обновляется

**Проблема:** Статистика не изменяется после обработки

**Решение:**
```bash
# Проверьте наличие SERP данных
python -c "import pandas as pd; df = pd.read_csv('output/groups/skud/seo_analysis_full.csv'); print('serp_urls' in df.columns)"

# Проверьте права на запись
dir output\global_domain_stats.db

# Удалите и пересоздайте БД
del output\global_domain_stats.db
python main.py --group skud
```

## 📚 Дополнительно

- [Быстрый старт](QUICK_START_GROUPS.md)
- [Полная документация](guides/MULTI_GROUP_SYSTEM.md)
- [Архитектура проекта](../АРХИТЕКТУРА_ПРОЕКТА.md)

