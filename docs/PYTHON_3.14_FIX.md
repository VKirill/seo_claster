# Решение проблем совместимости с Python 3.14

## Проблема

При использовании Python 3.14 возникает ошибка:
```
AttributeError: 'wrapper_descriptor' object has no attribute '__annotate__'
```

Это связано с изменениями в модуле `dataclasses` в Python 3.14.1.

## Решение

### Автоматическая установка

После установки зависимостей через `pip install -r requirements.txt`, выполните:

```batch
python fix_networkx_python314.py
```

### Ручная установка

Если автоматический скрипт не работает, выполните следующие шаги:

1. **Установите dev-версию NetworkX:**
   ```batch
   pip uninstall -y networkx
   pip install git+https://github.com/networkx/networkx.git
   ```

2. **Исправьте файл configs.py:**
   
   Откройте файл `.venv\Lib\site-packages\networkx\utils\configs.py`
   
   Найдите строку (около строки 8):
   ```python
   @dataclass(init=False, eq=False, slots=True, kw_only=True, match_args=False)
   ```
   
   Замените на:
   ```python
   @dataclass(init=False, eq=False, slots=False, kw_only=True, match_args=False)
   ```

3. **Очистите кэш Python:**
   ```batch
   Get-ChildItem ".venv/Lib/site-packages/networkx" -Recurse -Filter "__pycache__" | Remove-Item -Recurse -Force
   ```

4. **Используйте скрипт запуска:**
   ```batch
   run.bat
   ```

## Альтернативное решение

Используйте Python 3.13 или более раннюю версию:

```batch
# Удалите текущее виртуальное окружение
Remove-Item -Recurse -Force .venv

# Создайте новое с Python 3.13
py -3.13 -m venv .venv

# Активируйте и установите зависимости
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Проверка

После применения исправлений запустите:

```batch
python test_quick.py
python main.py
```

## Известные предупреждения

При работе могут появляться следующие предупреждения (они не критичны):

1. `pkg_resources is deprecated` - предупреждение от pymorphy2
2. `ValueError: Cannot register ... for automatic cleanup` - предупреждение от joblib

Эти предупреждения не влияют на работу приложения.


