# ⚠️ Объяснение предупреждений и ошибок

## Некритичные ошибки (можно игнорировать)

### 1. ValueError: Cannot register "REGISTER","rtype":"folder" (joblib)

**Что это:**
```
ValueError: Cannot register "REGISTER","rtype":"folder","base64_name" for automatic cleanup: 
unknown resource type
```

**Причина:**
- Это проблема библиотеки `joblib` с Python 3.14
- Возникает при параллельной обработке в LDA (topic modeling)
- Связана с механизмом очистки временных файлов

**Влияние:**
- ❌ НЕ влияет на работу скрипта
- ❌ НЕ влияет на результаты
- ✅ Все работает корректно, просто warnings в логах

**Решение:**
- Игнорировать - это не ошибка, а warning
- Библиотека `joblib` скоро обновится для Python 3.14
- Если очень мешает, можно подавить:

```python
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='joblib')
```

---

### 2. UserWarning: pkg_resources is deprecated

**Что это:**
```
UserWarning: pkg_resources is deprecated as an API. 
See https://setuptools.pypa.io/en/latest/pkg_resources.html
```

**Причина:**
- Устаревший API в библиотеке `pymorphy2` (используется в `natasha`)
- `pkg_resources` заменяется на `importlib.metadata`
- Разработчики `pymorphy2` обновят библиотеку

**Влияние:**
- ❌ НЕ влияет на работу
- ❌ НЕ влияет на результаты
- ✅ Морфология работает корректно

**Решение:**
- Игнорировать
- Ждать обновления `pymorphy2`/`natasha`

---

### 3. FutureWarning: DataFrameGroupBy.apply

**Что это:**
```
FutureWarning: DataFrameGroupBy.apply operated on the grouping columns. 
This behavior is deprecated
```

**Причина:**
- Изменение в pandas для будущих версий
- В дедупликаторе используется `groupby().apply()`

**Влияние:**
- ❌ НЕ влияет на текущую работу
- ⚠️ Нужно будет исправить при обновлении pandas

**Решение (уже сделано в коде):**
```python
# Было:
result = grouped.apply(select_best).reset_index(drop=True)

# Стало:
result = grouped.apply(select_best, include_groups=False).reset_index(drop=True)
```

---

## Как подавить все warnings (опционально)

Если warnings мешают читать вывод, добавьте в начало `main.py`:

```python
import warnings

# Подавить joblib warnings
warnings.filterwarnings('ignore', category=UserWarning, module='joblib')

# Подавить pkg_resources warnings
warnings.filterwarnings('ignore', message='pkg_resources is deprecated')

# Подавить pandas FutureWarnings
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')
```

**НО:** лучше их оставить, чтобы видеть что происходит!

---

## Реальные ошибки (нужно исправлять)

### ❌ AttributeError

Если видите:
```
AttributeError: 'wrapper_descriptor' object has no attribute '__annotate__'
```

**Решение:** См. `PYTHON_3.14_FIX.md`

### ❌ ModuleNotFoundError

Если видите:
```
ModuleNotFoundError: No module named 'X'
```

**Решение:**
```bash
.venv\Scripts\activate
pip install -r requirements.txt
```

### ❌ UnicodeEncodeError

Если видите:
```
UnicodeEncodeError: 'charmap' codec can't encode character
```

**Решение:** Используйте `run.bat` (автоматически устанавливает UTF-8)

---

## Итого: что делать с warnings

| Warning | Критично? | Действие |
|---------|-----------|----------|
| joblib ValueError | ❌ Нет | Игнорировать |
| pkg_resources deprecated | ❌ Нет | Игнорировать |
| pandas FutureWarning | ❌ Нет | Игнорировать |

**Главное:** если в конце написано `✅ АНАЛИЗ ЗАВЕРШЕН!` и файлы созданы - значит всё работает отлично! 🎯

---

## Проверка результатов

После анализа проверьте:

```bash
# Все файлы на месте?
ls output/

# Есть данные?
python -c "import pandas as pd; df = pd.read_csv('output/seo_analysis_full.csv'); print(f'Запросов: {len(df)}')"

# HTML открывается?
start output/dashboard.html
```

Если всё это работает - warnings можно смело игнорировать! ✅


