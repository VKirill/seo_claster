# SERP → Master DB: Прямая интеграция ✅

## Что сделано 🚀

SERP Analyzer теперь **напрямую работает с Master DB**:
- ✅ Проверяет кэш в Master DB (приоритет #1)
- ✅ Обновляет статусы (pending/processing/completed/error)
- ✅ Сохраняет req_id для отслеживания
- ✅ Старый `serp_data.db` используется как резервный

## Workflow 🔄

### 1. Проверка кэша (приоритет)

```
1. Master DB (completed + данные) → мгновенно ⚡
2. serp_data.db (XML) → парсинг
3. XMLStock API → запрос
```

### 2. При запросе к API

```python
# ПЕРЕД отправкой
master_db.update_serp_status(
    group_name="видеонаблюдение",
    keyword="купить камеры",
    status="processing"
)

# API запрос
result = await fetch_from_xmlstock(query)

# ПОСЛЕ получения
master_db.update_serp_status(
    group_name="видеонаблюдение",
    keyword="купить камеры",
    status="completed",
    req_id="12345678"  # от xmlstock
)
```

### 3. При ошибке

```python
# Временная ошибка (timeout, 202, 210)
master_db.update_serp_status(
    ...,
    status="processing",  # Оставляем processing
    error_message="Timeout, попробуем снова"
)

# Постоянная ошибка
master_db.update_serp_status(
    ...,
    status="error",
    error_message="API limit exceeded"
)
```

## Использование 📝

### Обычный запуск

```python
from seo_analyzer.analysis.serp.analyzer import SERPAnalyzer

analyzer = SERPAnalyzer(
    api_key="...",
    query_group="видеонаблюдение",
    use_master_db=True  # ⭐ Включено по умолчанию
)

# Анализ запросов
results = await analyzer.analyze_queries_batch(queries)

# Статистика
print(analyzer.stats)
# {
#     'total_queries': 56923,
#     'cached_from_master': 45123,  # Из Master DB
#     'cached_from_db': 8234,        # Из serp_data.db
#     'api_requests': 3566,          # Новые запросы
#     'status_updated': 3566,        # Обновлено статусов
#     'errors': 125
# }
```

### После падения скрипта

```bash
# 1. Проверка незавершённых
python recover_serp_requests.py
# → Найдено 3,566 незавершённых (status=processing)

# 2. Докачка
python main.py --force-serp
# → Загрузит только незавершённые

# 3. Проверка
python recover_serp_requests.py
# → ✅ Все завершены!
```

## Изменения в коде 🔧

### analyzer.py

**Добавлено:**
- `use_master_db: bool = True` - флаг использования Master DB
- `self.master_db` - экземпляр MasterQueryDatabase
- `_get_from_master_db(query)` - чтение из Master DB
- `_update_master_status(...)` - обновление статуса
- Статистика: `cached_from_master`, `status_updated`

**Приоритет кэшей:**
```python
async def analyze_query(query):
    # 1. Master DB (быстрее, с данными)
    if master_db:
        cached = _get_from_master_db(query)
        if cached:
            return cached
    
    # 2. serp_data.db (резервный)
    cached = cache_manager.get_from_cache(query)
    if cached:
        return cached
    
    # 3. API запрос
    return await _fetch_from_api(query)
```

**При запросе:**
```python
async def _fetch_from_api(query):
    # Отметить как processing
    _update_master_status(query, 'processing')
    
    # API запрос
    result = await api_client.fetch_serp_data(query)
    
    if result['error']:
        # Ошибка
        _update_master_status(query, 'error', error_message=...)
    else:
        # Успех
        _update_master_status(query, 'completed', req_id=...)
        
        # Сохранить в serp_data.db (резервный)
        cache_manager.save_to_caches(...)
    
    return result
```

## Производительность ⚡

### Первый запуск (без кэша)

```
SERP анализ: 56,923 запросов
├─ Master DB проверка: 3 сек (все pending)
├─ API запросы: 25 минут (50 параллельно)
├─ Обновление статусов: 2 сек (автоматически)
└─ Сохранение в Master DB: 5 сек (в конце pipeline)

ИТОГО: ~25 минут
```

### Второй запуск (с кэшем)

```
SERP анализ: 56,923 запросов
├─ Master DB проверка: 3 сек
├─ Загружено из Master: 56,923 (completed)
└─ API запросы: 0

ИТОГО: ~3 секунды! 🚀
```

### После падения (15,000 незавершённых)

```
SERP анализ: 56,923 запросов
├─ Master DB проверка: 3 сек
├─ Загружено из Master: 41,923 (completed)
├─ API запросы: 15,000 (pending + processing)
├─ Обновление статусов: 1 сек
└─ Сохранение: 3 сек

ИТОГО: ~8 минут (только незавершённые)
```

## Сравнение: До и После ⚖️

### До (только serp_data.db)

```
❌ Нет отслеживания статусов
❌ При падении - непонятно что не загружено
❌ Приходится пересобирать всё заново
❌ XML (2 GB) тяжёлые для хранения
```

### После (Master DB + статусы)

```
✅ Отслеживание статусов (pending/processing/completed/error)
✅ req_id для каждого запроса
✅ Быстрое восстановление после падения
✅ Приоритет Master DB (мгновенная загрузка)
✅ Распарсенные данные (200 MB вместо 2 GB)
✅ serp_data.db как резервный кэш
```

## Примеры 📋

### Пример 1: Нормальная работа

```python
analyzer = SERPAnalyzer(
    api_key="...",
    query_group="видеонаблюдение",
    use_master_db=True
)

# Анализ 1000 запросов
queries = ["купить камеры", "монтаж видеонаблюдения", ...]
results = await analyzer.analyze_queries_batch(queries)

# Статистика
print(f"Из Master DB: {analyzer.stats['cached_from_master']}")
print(f"API запросов: {analyzer.stats['api_requests']}")
print(f"Статусов обновлено: {analyzer.stats['status_updated']}")
```

### Пример 2: Проверка незавершённых

```python
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase

master_db = MasterQueryDatabase()

# Получить незавершённые
pending = master_db.get_pending_serp_queries("видеонаблюдение")

print(f"Незавершённых: {len(pending)}")
for item in pending[:10]:
    print(f"  {item['keyword']} - {item['serp_status']}")
```

### Пример 3: Статистика по группе

```python
stats = master_db.get_serp_statistics("видеонаблюдение")

print(f"Всего: {stats['total']}")
print(f"Завершено: {stats['completed']} ({stats['completion_rate']:.1%})")
print(f"Pending: {stats['pending']}")
print(f"Processing: {stats['processing']}")
print(f"Ошибок: {stats['error']}")
```

## Troubleshooting 🔧

### Master DB не подключается

**Проблема:** `⚠️  Master DB недоступен: ...`

**Решение:**
```bash
# Проверить что БД создана
dir output\master_queries.db

# Создать если нет
python -c "from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase; MasterQueryDatabase()"
```

### Статусы не обновляются

**Причина:** `use_master_db=False` или `query_group=None`

**Решение:**
```python
analyzer = SERPAnalyzer(
    ...,
    query_group="видеонаблюдение",  # ⭐ Обязательно!
    use_master_db=True
)
```

### Много запросов в "processing"

**Причина:** Скрипт упал во время ожидания ответа

**Решение:**
```bash
# Вариант 1: Докачать
python main.py --force-serp

# Вариант 2: Проверить req_id в xmlstock
python recover_serp_requests.py
# → Показать список → проверить req_id вручную
```

## Миграция: Убираем serp_data.db 🔄

### Этап 1: Текущее состояние ✅

```
XMLStock API
    ↓
serp_data.db (XML, 2 GB) ← резервный кэш
    ↓
Master DB (данные + статусы, 200 MB) ← основное хранилище
```

**Преимущества:**
- Есть резервная копия XML
- Можно переп арсить если нужно

### Этап 2: Финальное состояние (будущее)

```
XMLStock API
    ↓
Master DB (данные + статусы, 200 MB)
```

**Когда убирать serp_data.db:**
- После нескольких успешных запусков
- Когда уверены что парсинг XML работает правильно
- Когда нужно освободить 2 GB

**Как убрать:**
```python
# В analyzer.py изменить:
use_database=False  # Отключить serp_data.db
use_master_db=True  # Только Master DB
```

## См. также 📚

- [SERP_STATUS_TRACKING.md](SERP_STATUS_TRACKING.md) - Отслеживание статусов
- [MASTER_DB_INTEGRATION.md](MASTER_DB_INTEGRATION.md) - Интеграция Master DB
- `recover_serp_requests.py` - Скрипт восстановления

---

**Вопросы?** Запустите `python main.py` и смотрите статистику!






