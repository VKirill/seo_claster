# Интеграция с Yandex Direct API

## Описание

Модуль интеграции с Yandex Direct API позволяет обогатить результаты кластеризации реальными данными о:
- Прогнозах трафика по запросам (показы и клики)
- Стоимости клика (CPC) - минимальной, средней, максимальной
- CTR на премиум позициях
- Уровне конкуренции
- Рекомендуемых ставках для выхода в топ

## Получение OAuth токена

1. Зарегистрируйте приложение в [Yandex OAuth](https://oauth.yandex.ru/)
2. Получите права доступа `direct:api`
3. Сгенерируйте токен доступа

## Настройка

Добавьте в `config_local.py`:

```python
# Yandex Direct API
YANDEX_DIRECT_ENABLED = True
YANDEX_DIRECT_TOKEN = "y0_AgAAAAB1HybhAAv66QAAAAEIDEnJAABa13y..."
YANDEX_DIRECT_USE_SANDBOX = False  # True для тестов
YANDEX_DIRECT_GEO_ID = 213  # 213 = Москва, 1 = Москва и область, 225 = Россия
```

## Использование в коде

### Базовое использование

```python
from seo_analyzer.analysis import YandexDirectIntegrator
import pandas as pd

# Инициализация
integrator = YandexDirectIntegrator(
    token="your_token_here",
    use_sandbox=False,
    geo_id=213,  # Москва
    enabled=True
)

# Обогащение DataFrame с запросами
df = pd.DataFrame({
    'query': ['купить холодильник', 'холодильник москва', 'холодильник цена']
})

enriched_df = integrator.enrich_dataframe(df, query_column='query')

# Результат содержит новые колонки:
print(enriched_df[['query', 'direct_shows', 'direct_avg_cpc', 'direct_competition']])
```

### Интеграция с кластерами

```python
# Обогащение кластеров
clusters = [
    {
        'cluster_id': 1,
        'queries': ['холодильник купить', 'купить холодильник недорого'],
        'main_query': 'холодильник купить'
    }
]

enriched_clusters = integrator.enrich_clusters(clusters)

# Кластер теперь содержит:
# - direct_shows: суммарные показы по всем запросам
# - direct_clicks: суммарные клики
# - direct_avg_cpc: средневзвешенный CPC
# - direct_competition: уровень конкуренции
# - direct_recommended_cpc: рекомендуемая ставка
```

### Расчет KEI с данными Direct

```python
from seo_analyzer.metrics import (
    kei_direct_efficiency,
    kei_direct_profitability,
    kei_direct_quality_score
)

# KEI эффективности (частота / CPC * CTR)
df['kei_efficiency'] = kei_direct_efficiency(df)

# Прогнозируемая прибыль
df['kei_profit'] = kei_direct_profitability(
    df, 
    avg_check=5000,  # средний чек 5000 руб
    conversion_rate=2.0  # конверсия 2%
)

# Комплексная оценка качества (0-100)
df['quality_score'] = kei_direct_quality_score(df)
```

## Структура данных

### Добавляемые колонки в DataFrame

| Колонка | Тип | Описание |
|---------|-----|----------|
| `direct_shows` | int | Показы за последние 30 дней |
| `direct_clicks` | int | Клики за последние 30 дней |
| `direct_ctr` | float | CTR в поиске (%) |
| `premium_ctr` | float | CTR на премиум позициях (%) |
| `direct_min_cpc` | float | Минимальный CPC (руб) |
| `direct_avg_cpc` | float | Средний CPC (руб) |
| `direct_max_cpc` | float | Максимальный CPC (руб) |
| `direct_recommended_cpc` | float | Рекомендуемая ставка (руб) |
| `direct_competition` | str | Уровень конкуренции (low/medium/high) |
| `direct_first_place_bid` | float | Ставка за 1 место (руб) |

### Метрики кластера

При обогащении кластера добавляются:
- `direct_shows` - суммарные показы
- `direct_clicks` - суммарные клики  
- `direct_avg_cpc` - средневзвешенный CPC
- `direct_competition` - преобладающий уровень конкуренции
- `direct_recommended_cpc` - медианная рекомендуемая ставка

## Кэширование

Данные автоматически кэшируются в SQLite базу `yandex_direct_cache.db`.

- **Время жизни кэша:** 7 дней
- **Автоочистка:** старые записи удаляются автоматически

```python
from seo_analyzer.core.yandex_direct_cache import YandexDirectCache

cache = YandexDirectCache()

# Получение из кэша
data = cache.get("купить холодильник", geo_id=213)

# Очистка старых записей
deleted = cache.clear_old()
print(f"Удалено устаревших записей: {deleted}")
```

## Лимиты API

- **Максимум фраз в одном запросе:** 10
- **Максимум слов в фразе:** 6
- **Задержка между запросами:** 0.5 сек
- **Sandbox:** неограниченно для тестов
- **Production:** согласно лимитам вашего аккаунта

## Расширенные KEI метрики

### KEI Direct Efficiency
```python
kei_direct_efficiency(df)
```
Формула: `(Frequency / CPC) * (CTR / 100)`

Показывает эффективность запроса с точки зрения ROI.

### KEI Direct Profitability
```python
kei_direct_profitability(df, avg_check=5000, conversion_rate=2.0)
```
Формула: `(Clicks * ConversionRate * AvgCheck) - (Clicks * CPC)`

Прогнозируемая прибыль от запроса.

### KEI Direct Competition Score
```python
kei_direct_competition_score(df)
```
Формула: `(Frequency / Shows) * (100 / CPC)`

Оценка конкуренции (выше = легче продвигаться).

### KEI Direct Traffic Potential
```python
kei_direct_traffic_potential(df, target_position=3)
```

Прогноз трафика при выходе на целевую позицию.

### KEI Direct Budget Required
```python
kei_direct_budget_required(df, target_clicks=100)
```

Требуемый бюджет для получения N кликов.

### KEI Direct Quality Score
```python
kei_direct_quality_score(df)
```

Комплексная оценка качества запроса (0-100 баллов):
- Частотность (0-40)
- Доступность CPC (0-30)
- CTR/релевантность (0-20)
- Конкуренция (0-10)

## Пример n8n воркфлоу

В директории проекта есть примеры n8n скриптов для:
1. Создания прогноза (`CreateNewForecast`)
2. Получения результатов (`GetForecast`)
3. Анализа трафика и бюджетов

См. код в запросе пользователя.

## Troubleshooting

### Ошибка "API returned error"
- Проверьте токен доступа
- Убедитесь что права `direct:api` выданы
- Для production используйте `use_sandbox=False`

### Пустые данные (0 показов)
- Фраза слишком низкочастотная
- Фраза содержит >6 слов (ограничение API)
- Нет данных по региону

### Медленная работа
- Включите кэш: `use_cache=True` (по умолчанию)
- Уменьшите количество запросов в батче
- Используйте sandbox для тестов

## См. также

- [Документация Yandex Direct API](https://yandex.ru/dev/direct/doc/)
- [Метод CreateNewForecast](https://yandex.ru/dev/direct/doc/dg-v4/reference/CreateNewForecast.html)
- [Метод GetForecast](https://yandex.ru/dev/direct/doc/dg-v4/reference/GetForecast.html)






