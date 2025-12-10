# 🔗 Интеграция DeepSeek в Pipeline

## Пошаговая инструкция как добавить DeepSeek в основной процесс

---

## 📍 Где интегрировать

Файл: `pipeline/stages/yandex_direct_enricher.py`

Функция: `enrich_with_yandex_direct_stage()`

Место: После получения данных от Yandex Direct, перед расчётом метрик.

---

## 💻 Код интеграции

### Шаг 1: Добавить импорт

В начало файла `pipeline/stages/yandex_direct_enricher.py`:

```python
from seo_analyzer.analysis.deepseek_conversion_estimator import estimate_conversion_for_dataframe
```

### Шаг 2: Добавить расчёт конверсии

В функции `enrich_with_yandex_direct_stage()`, после строки:

```python
analyzer.df = integrator.enrich(analyzer.df)
```

Добавить:

```python
# === ИНТЕГРАЦИЯ DEEPSEEK ===
# Оценка конверсии через DeepSeek AI (если включено)
deepseek_enabled = config.get('deepseek_enabled', False)
deepseek_api_key = config.get('deepseek_api_key')

if deepseek_enabled and deepseek_api_key:
    print("\n🤖 Оценка конверсии через DeepSeek AI...")
    
    try:
        lead_cost, conversion_rate = estimate_conversion_for_dataframe(
            analyzer.df,
            api_key=deepseek_api_key,
            top_n=config.get('deepseek_top_n', 15),
            frequency_column='frequency_exact'
        )
        
        print(f"   💰 Стоимость лида: {lead_cost:.2f} руб")
        print(f"   📊 Конверсия: {conversion_rate:.2f}%")
        
        # Сохраняем для использования в метриках
        config['avg_check'] = lead_cost
        config['conversion_rate'] = conversion_rate
        
    except Exception as e:
        print(f"   ⚠️  Ошибка DeepSeek: {e}")
        print("   💡 Использую дефолтные значения (2500 руб, 2.0%)")
        config['avg_check'] = 2500
        config['conversion_rate'] = 2.0
else:
    # Fallback значения
    if 'avg_check' not in config:
        config['avg_check'] = 2500
    if 'conversion_rate' not in config:
        config['conversion_rate'] = 2.0
```

---

## 📋 Полный пример интеграции

```python
def enrich_with_yandex_direct_stage(analyzer, config):
    """
    Stage 2: Обогащение данных через Yandex Direct API
    """
    # ... существующий код ...
    
    # Обогащение данных
    analyzer.df = integrator.enrich(analyzer.df)
    
    # === ИНТЕГРАЦИЯ DEEPSEEK ===
    deepseek_enabled = config.get('deepseek_enabled', False)
    deepseek_api_key = config.get('deepseek_api_key')
    
    if deepseek_enabled and deepseek_api_key:
        print("\n🤖 Оценка конверсии через DeepSeek AI...")
        
        try:
            from seo_analyzer.analysis.deepseek_conversion_estimator import (
                estimate_conversion_for_dataframe
            )
            
            lead_cost, conversion_rate = estimate_conversion_for_dataframe(
                analyzer.df,
                api_key=deepseek_api_key,
                top_n=config.get('deepseek_top_n', 15),
                frequency_column='frequency_exact'
            )
            
            print(f"   💰 Стоимость лида: {lead_cost:.2f} руб")
            print(f"   📊 Конверсия: {conversion_rate:.2f}%")
            
            config['avg_check'] = lead_cost
            config['conversion_rate'] = conversion_rate
            
        except Exception as e:
            print(f"   ⚠️  Ошибка DeepSeek: {e}")
            print("   💡 Использую дефолтные значения")
            config['avg_check'] = 2500
            config['conversion_rate'] = 2.0
    else:
        if 'avg_check' not in config:
            config['avg_check'] = 2500
        if 'conversion_rate' not in config:
            config['conversion_rate'] = 2.0
    
    # Продолжение обработки...
    return analyzer
```

---

## ⚙️ Конфигурация в main.py

В функции `load_yandex_direct_config()`:

```python
def load_yandex_direct_config(args):
    """Загрузка конфигурации Yandex Direct"""
    config = {
        'token': getattr(config_local, 'YANDEX_DIRECT_TOKEN', ''),
        'use_sandbox': getattr(config_local, 'YANDEX_DIRECT_USE_SANDBOX', False),
        'geo_id': getattr(config_local, 'YANDEX_DIRECT_GEO_ID', 213),
        
        # === DEEPSEEK CONFIG ===
        'deepseek_enabled': getattr(config_local, 'DEEPSEEK_ENABLED', False),
        'deepseek_api_key': getattr(config_local, 'DEEPSEEK_API_KEY', ''),
        'deepseek_top_n': getattr(config_local, 'DEEPSEEK_TOP_N_QUERIES', 15),
        
        # Fallback значения если DeepSeek отключен
        'avg_check': getattr(config_local, 'AVG_CHECK', 2500),
        'conversion_rate': getattr(config_local, 'CONVERSION_RATE', 2.0),
    }
    return config
```

---

## 🧪 Тестирование

```bash
# 1. Проверка интеграции
python test_deepseek_integration.py

# 2. Запуск основного скрипта с Direct
python main.py semantika/скуд.csv --enable-direct

# В логах должно появиться:
# 🤖 Оценка конверсии через DeepSeek AI...
#    💰 Стоимость лида: 2543.20 руб
#    📊 Конверсия: 2.35%
```

---

## 🔄 Как это работает

1. **Сбор данных**: Yandex Direct собирает CPC, CTR, показы
2. **Анализ DeepSeek**: Отправляет топ-15 ВЧ запросов в DeepSeek
3. **Получение метрик**: DeepSeek возвращает `lead_cost` и `conversion_rate`
4. **Расчёт ROI**: Используются полученные метрики для расчёта:
   - `direct_cost_per_conversion` = CPC / (conversion_rate / 100)
   - `direct_roi_forecast` = ((avg_check / cost_per_conversion) - 1) * 100

---

## 💡 Советы

### Кэширование результатов
Можно кэшировать результаты DeepSeek, чтобы не отправлять запросы при каждом запуске:

```python
# В начале функции
deepseek_cache_file = 'output/.deepseek_cache.json'

if os.path.exists(deepseek_cache_file):
    with open(deepseek_cache_file, 'r') as f:
        cache = json.load(f)
    config['avg_check'] = cache.get('lead_cost', 2500)
    config['conversion_rate'] = cache.get('conversion_rate', 2.0)
else:
    # Запрос к DeepSeek
    # ... код ...
    
    # Сохранение в кэш
    with open(deepseek_cache_file, 'w') as f:
        json.dump({
            'lead_cost': lead_cost,
            'conversion_rate': conversion_rate,
            'timestamp': datetime.now().isoformat()
        }, f)
```

### Разные ниши
Для разных ниш можно использовать разные промпты:

```python
# В config_local.py
DEEPSEEK_PROMPT_FILE = {
    'security': 'prompts/deepseek_security_niche.txt',
    'beauty': 'prompts/deepseek_beauty_niche.txt',
    'default': 'prompts/deepseek_lead_cost_analysis.txt',
}
```

---

## 📊 Результат в Excel

После интеграции в столбце **"Direct: Стоимость конверсии (₽)"** будет:

```
= CPC / (conversion_rate / 100)
```

Где `conversion_rate` получен от DeepSeek на основе анализа ваших запросов!

---

## ❓ FAQ

**Q: Обязательно ли это делать?**  
A: Нет, без DeepSeek будут использоваться дефолтные значения (2500 руб, 2%).

**Q: Сколько стоит один запрос к DeepSeek?**  
A: ~$0.0001 (0.01 руб) за анализ 15 запросов.

**Q: Можно ли использовать другую AI модель?**  
A: Да, измените `API_URL` и `MODEL` в `deepseek_conversion_estimator.py`.

---

## ✅ Готово!

После интеграции у вас будет автоматический расчёт конверсии на основе реального анализа ваших запросов! 🎉


