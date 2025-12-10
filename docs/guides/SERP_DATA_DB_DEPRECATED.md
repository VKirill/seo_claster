# serp_data.db больше не используется! 🎉

## Что изменилось?

**serp_data.db полностью заменена на Master DB!**

### Раньше (устаревшая архитектура):

```
┌──────────────────────────────────────┐
│ serp_data.db                         │
│ ├─ xml_response TEXT (полный XML)   │
│ ├─ documents TEXT (все документы)   │
│ └─ lsi_phrases TEXT                  │
└──────────────────────────────────────┘
         │
         │ Дублирование
         ▼
┌──────────────────────────────────────┐
│ master_queries.db                    │
│ ├─ serp_found_docs INTEGER           │
│ ├─ serp_top_urls TEXT (только TOP-20)│
│ └─ serp_lsi_phrases TEXT             │
└──────────────────────────────────────┘
```

**Проблема:** XML занимает много места, дублирование данных.

### Теперь (новая архитектура):

```
┌──────────────────────────────────────────────────┐
│ master_queries.db - ЕДИНСТВЕННЫЙ ИСТОЧНИК        │
├──────────────────────────────────────────────────┤
│ ✅ serp_found_docs, serp_main_pages_count       │
│ ✅ serp_top_urls TEXT (TOP-20 для кластеризации)│
│ ✅ serp_lsi_phrases TEXT (LSI фразы)            │
│ ✅ serp_req_id, serp_status (отслеживание)     │
│ ✅ serp_metrics (все метрики)                   │
│ ❌ НЕТ сырого XML (не нужен!)                   │
└──────────────────────────────────────────────────┘
```

## Почему XML не нужен?

1. **Распаршен:** XML уже обработан в структурированные данные
2. **TOP-20 достаточно:** Для кластеризации нужны только TOP-20 URL
3. **LSI извлечены:** Фразы сохранены отдельно
4. **req_id есть:** Можно восстановить данные через xmlstock API

## Что изменилось в коде?

### `SERPAnalyzer.__init__`

**Было:**
```python
self.cache_manager = SERPCacheManager(
    use_database,
    use_file_cache,
    db_path=db_path,
    query_group=query_group
)
```

**Стало:**
```python
# LEGACY: serp_data.db больше НЕ используется!
self.cache_manager = None
if use_database or use_file_cache:
    print(f"⚠️  serp_data.db устарела! Все данные теперь в Master DB.")
```

### `SERPAnalyzer.analyze_query`

**Было:**
```python
# 1. Пробуем Master DB
# 2. Пробуем serp_data.db ⬅️ УСТАРЕЛО!
cached = self.cache_manager.get_from_cache(query, self.lr, self.stats)
```

**Стало:**
```python
# Проверяем Master DB (единственный источник)
if self.master_db and self.query_group:
    master_cached = self._get_from_master_db(query)
    if master_cached:
        return master_cached
```

### Сохранение данных

**Было:**
```python
# Сохраняем в serp_data.db
self.cache_manager.save_to_caches(query, self.lr, api_result, result)
```

**Стало:**
```python
# Сохраняем в Master DB
self.master_db.update_serp_metrics(
    group_name=self.query_group,
    keyword=query,
    metrics=enriched['metrics'],
    documents=enriched['documents'],
    lsi_phrases=lsi_phrases
)
```

## Миграция

### Нужна ли миграция?

**НЕТ!** Старые данные из `serp_data.db` НЕ мигрируются.

**Почему:**
- Старые XML ответы не нужны (только метрики важны)
- При следующем запуске данные соберутся заново и сохранятся в Master DB
- Если данные уже в Master DB - используются оттуда

### Что делать со старой `serp_data.db`?

#### Вариант 1: Удалить сразу (рекомендуется)

```bash
del output\serp_data.db
```

**Плюсы:**
- Освобождает место (~100-500 МБ)
- Принудительно использует Master DB
- Чистая архитектура

**Минусы:**
- Придётся пересобрать SERP данные (через Batch Async = 5-10 минут)

#### Вариант 2: Оставить временно

Оставь `serp_data.db` на диске, но она **не будет использоваться**.

**Плюсы:**
- Можно восстановить при необходимости
- Страховка на случай проблем

**Минусы:**
- Занимает место
- Создаёт путаницу

#### Вариант 3: Бэкап и удаление

```bash
# Бэкап
copy output\serp_data.db backups\serp_data_archive.db

# Удалить
del output\serp_data.db
```

**Лучший вариант!**

## Проверка

### После удаления `serp_data.db`

```bash
# Запустить скрипт
python main.py видеонаблюдение
```

**Ожидаемое поведение:**

1. Загрузка из Master DB (если данные есть)
```
🚀 Загрузка из Master DB (все данные включая SERP + интент)...
✓ Запросов: 1,000
✓ С SERP: 900
```

2. Сбор новых SERP данных (если нет в Master DB)
```
🚀 BATCH ASYNC MODE: 100 запросов
📦 Проверка кэша...
✓ Закэшировано: 0/100
📤 Нужно загрузить: 100

📤 ЭТАП 1/3: Отправка запросов с delayed=1...
✓ Отправлено: 100/100

📥 ЭТАП 3/3: Получение результатов...
✓ Получено: 100
```

3. Сохранение в Master DB
```
💾 Master DB: обновлено 100 запросов с SERP метриками
```

### Проверка Master DB

```python
from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase

db = MasterQueryDatabase()
stats = db.get_statistics("видеонаблюдение")

print(f"С SERP данными: {stats['with_serp']}/{stats['total_queries']}")
```

## FAQ

### Потеряются ли данные?

**НЕТ!** Старые метрики останутся в Master DB (если были сохранены). Только сырой XML удалится.

### Нужно ли пересобирать SERP?

Только если данных нет в Master DB. При первом запуске после удаления `serp_data.db`:
- Запросы с данными в Master DB → загружаются мгновенно ⚡
- Запросы без данных → собираются заново через Batch Async (быстро!)

### Что если я хочу вернуть `serp_data.db`?

Просто верни файл из бэкапа. Но код больше не будет его использовать (устарело).

### Как восстановить SERP данные после удаления?

```python
# Если есть req_id в Master DB, можно восстановить через xmlstock
python recover_serp_requests.py --group видеонаблюдение --recover
```

Или просто запусти скрипт заново - Batch Async соберёт данные за несколько минут.

### Останется ли `serp_cache/` папка?

Да, папка `output/serp_cache/` тоже устарела! Можешь удалить:

```bash
rmdir /s output\serp_cache
```

## Итоги

✅ **serp_data.db больше НЕ используется**  
✅ **Все SERP данные теперь в Master DB**  
✅ **XML не сохраняется (не нужен)**  
✅ **TOP-20 URL + LSI фразы + метрики в Master DB**  
✅ **Можно безопасно удалять `serp_data.db`**  

---

**Архитектура упрощена! Одна база вместо трёх.** 🎉


