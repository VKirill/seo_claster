# Миграция с query_cache.db на Master DB

## Проблема

Изначально была двойная система кэширования:

```
query_cache.db          Master DB
    ↓                      ↓
normalized           normalized ✅
lemmatized           lemmatized ✅
ner_entities         ner_entities ✅
intent данные        intent данные ✅
```

**Дублирование!** Одни и те же данные хранятся в двух местах.

## Решение

**Master DB теперь главный источник истины** для всех данных.

`query_cache.db` → **legacy fallback** (используется только если Master DB недоступна)

## Архитектура после миграции

```
┌─────────────────────────────────────────────────────┐
│ Master DB (master_queries.db) - ЕДИНСТВЕННЫЙ ИСТОЧНИК│
├─────────────────────────────────────────────────────┤
│ ✅ Базовые данные (keyword, frequency)              │
│ ✅ Предобработка (normalized, lemmatized, NER)      │
│ ✅ Интенты (commercial_score, main_intent...)       │
│ ✅ SERP данные (req_id, статус, метрики, URLs)     │
│ ✅ Yandex Direct (bids, competition)               │
│ ✅ Кластеры (cluster_id, cluster_size)             │
│ ✅ Конкуренция (difficulty, KEI)                   │
└─────────────────────────────────────────────────────┘
              │
              │ Fallback (если Master DB недоступна)
              ▼
┌─────────────────────────────────────────────────────┐
│ query_cache.db - LEGACY (опциональный)              │
├─────────────────────────────────────────────────────┤
│ ⚠️  Предобработка (normalized, lemmatized, NER)    │
│ ⚠️  Интенты (если была миграция intent)            │
└─────────────────────────────────────────────────────┘
```

## Миграция

### Шаг 1: Проверить текущие данные

```bash
# Проверить что есть в query_cache
python -c "from seo_analyzer.core.cache import QueryCacheDatabase; db = QueryCacheDatabase(); print('Groups:', db.get_all_groups())"

# Проверить что есть в Master DB
python -c "from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase; db = MasterQueryDatabase(); print(db.get_all_statistics())"
```

### Шаг 2: Запустить миграцию

```bash
python migrate_query_cache_to_master.py
```

**Что произойдёт:**
- Все запросы из `query_cache.db` перенесутся в Master DB
- Существующие записи в Master DB обновятся (если данных не было)
- Дубликаты пропустятся

**Вывод:**
```
================================================================================
МИГРАЦИЯ: query_cache.db → master_queries.db
================================================================================

📦 Найдено групп в query_cache: 3

🔄 Группа: видеонаблюдение
   Запросов в кэше: 1000
   ✓ Мигрировано: 50
   ✓ Обновлено: 200
   ⚠️  Пропущено (уже есть): 750

🔄 Группа: окна
   Запросов в кэше: 500
   ✓ Мигрировано: 500
   ✓ Обновлено: 0
   ⚠️  Пропущено (уже есть): 0

================================================================================
ИТОГО:
  Групп обработано: 3
  Записей добавлено: 550
  Записей обновлено: 200
  Записей пропущено: 750
================================================================================

✅ Миграция завершена успешно!
```

### Шаг 3: Проверить результат

```bash
# Проверить Master DB
python check_db_performance.py
```

Или:

```python
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase

db = MasterQueryDatabase()
stats = db.get_all_statistics()

for group, data in stats.items():
    print(f"\n{group}:")
    print(f"  Всего: {data['total_queries']}")
    print(f"  С normalized: {data['with_normalized']}")
    print(f"  С lemmatized: {data['with_lemmatized']}")
    print(f"  С интентом: {data['with_intent']}")
    print(f"  С SERP: {data['with_serp']}")
```

### Шаг 4: Тестовый запуск

```bash
python main.py видеонаблюдение
```

**Ожидаемый вывод:**
```
📚 ЭТАП 1: Загрузка данных
────────────────────────────────────────────────────────────────────────────────
🚀 Загрузка из Master DB (все данные включая SERP + интент)...
  ✓ Запросов: 1,000
  ✓ С интентом: 950
  ✓ С SERP: 900
  💡 Можно сразу экспериментировать с кластеризацией!
```

Если видишь **"Загрузка из query_cache"** - значит Master DB не используется (проверь миграцию).

### Шаг 5: Удалить query_cache.db (опционально)

**ТОЛЬКО после проверки что всё работает!**

```bash
# Создай бэкап на всякий случай
copy output\query_cache.db output\query_cache.db.backup

# Удали
del output\query_cache.db
```

**Проверь что всё работает:**
```bash
python main.py видеонаблюдение
```

Должно работать без `query_cache.db`! Все данные теперь в Master DB.

## Обратная совместимость

Pipeline **автоматически** работает с обеими схемами:

### Если Master DB есть
```
1. Загрузка из Master DB ⚡ (все данные)
2. Пропуск preprocessing
3. Пропуск классификации (если данные есть)
4. Сразу кластеризация
```

### Если Master DB нет, но есть query_cache
```
1. Загрузка из query_cache (предобработка)
2. Пропуск preprocessing
3. Классификация (если нет в кэше)
4. Сохранение в Master DB
```

### Если ничего нет
```
1. Загрузка из CSV
2. Полная обработка
3. Сохранение в Master DB
4. (query_cache больше не используется)
```

## Преимущества Master DB

✅ **Единый источник данных** - нет дублирования  
✅ **Больше данных** - SERP, req_id, статусы, кластеры  
✅ **Быстрее** - оптимизированные индексы  
✅ **Надёжнее** - восстановление после сбоев  
✅ **Проще** - одна база вместо трёх  

## Что делать с query_cache.db после миграции

### Вариант 1: Удалить (рекомендуется)

```bash
del output\query_cache.db
```

**Плюсы:**
- Освобождает место (~10-50 МБ)
- Нет путаницы с двумя кэшами
- Принудительно использует только Master DB

**Минусы:**
- Нет fallback при проблемах с Master DB

### Вариант 2: Оставить (legacy fallback)

Оставь `query_cache.db` как запасной вариант.

**Плюсы:**
- Если Master DB повредится, есть резервный кэш
- Плавный переход

**Минусы:**
- Занимает место
- Данные могут устареть

### Вариант 3: Бэкап и удаление

```bash
# Бэкап
copy output\query_cache.db backups\query_cache_$(date +%Y%m%d).db

# Удалить
del output\query_cache.db
```

**Лучший вариант!** Есть бэкап на случай проблем.

## FAQ

### После миграции query_cache всё ещё используется?

**Нет!** Pipeline проверяет Master DB **первой**. query_cache используется только если Master DB недоступна.

### Нужно ли удалять query_cache.db?

**Нет, необязательно.** Она занимает мало места и служит fallback'ом. Но можешь удалить для чистоты.

### Что если я удалю query_cache.db?

Ничего страшного! Pipeline будет работать с Master DB. Если Master DB нет - загрузит из CSV.

### Данные из query_cache потеряются?

**Нет!** Скрипт миграции переносит ВСЁ в Master DB перед удалением.

### Можно ли откатиться назад?

Да! Просто верни `query_cache.db` из бэкапа. Pipeline автоматически начнёт её использовать если Master DB недоступна.

### После миграции preprocessing всё равно выполняется?

**Нет!** Если данные есть в Master DB, preprocessing пропускается полностью:

```python
if getattr(analyzer, 'loaded_from_master_cache', False):
    return  # Пропускаем preprocessing
```

### Как проверить что Master DB используется?

Запусти:
```bash
python main.py видеонаблюдение
```

Если видишь:
```
🚀 Загрузка из Master DB (все данные включая SERP + интент)...
```

Значит **Master DB работает!** ✅

Если видишь:
```
⚡ Загрузка из query_cache (legacy)...
```

Значит Master DB недоступна, используется fallback.

## Проверка после миграции

```bash
# 1. Проверить размер Master DB
ls -lh output/master_queries.db

# 2. Проверить статистику
python -c "from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase; db = MasterQueryDatabase(); print(db.get_all_statistics())"

# 3. Тестовый запуск
python main.py видеонаблюдение

# 4. Проверить что query_cache не используется (смотри логи)
# Должно быть: "Загрузка из Master DB"
# НЕ должно быть: "Загрузка из query_cache"
```

## Итоги

✅ **Master DB - единственный источник данных**  
✅ **query_cache.db - legacy fallback (опциональна)**  
✅ **Миграция простая и безопасная**  
✅ **Можно безопасно удалить query_cache.db после проверки**  

---

**Готово!** После миграции твоя система будет работать только с Master DB. 🎉


