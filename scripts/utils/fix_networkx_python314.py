#!/usr/bin/env python
"""
Скрипт для автоматического исправления NetworkX для Python 3.14
"""
import os
import sys
import shutil
from pathlib import Path

def fix_networkx():
    """Исправляет файл configs.py в NetworkX для совместимости с Python 3.14"""
    
    # Путь к файлу configs.py
    venv_path = Path('.venv')
    if not venv_path.exists():
        print("❌ Виртуальное окружение .venv не найдено!")
        print("   Сначала создайте виртуальное окружение и установите зависимости.")
        return False
    
    configs_file = venv_path / 'Lib' / 'site-packages' / 'networkx' / 'utils' / 'configs.py'
    
    if not configs_file.exists():
        print(f"❌ Файл {configs_file} не найден!")
        print("   Убедитесь, что NetworkX установлен.")
        return False
    
    print(f"📝 Исправление файла: {configs_file}")
    
    # Читаем файл
    content = configs_file.read_text(encoding='utf-8')
    
    # Проверяем, нужно ли исправление
    if 'slots=False' in content:
        print("✅ Файл уже исправлен!")
        return True
    
    # Создаем резервную копию
    backup_file = configs_file.with_suffix('.py.backup')
    shutil.copy2(configs_file, backup_file)
    print(f"💾 Создана резервная копия: {backup_file}")
    
    # Исправляем
    new_content = content.replace('slots=True', 'slots=False')
    
    if new_content == content:
        print("⚠️  Паттерн 'slots=True' не найден в файле!")
        return False
    
    # Записываем исправленный файл
    configs_file.write_text(new_content, encoding='utf-8')
    print("✅ Файл успешно исправлен!")
    
    # Удаляем кэш
    networkx_path = venv_path / 'Lib' / 'site-packages' / 'networkx'
    cache_dirs = list(networkx_path.rglob('__pycache__'))
    
    if cache_dirs:
        print(f"🗑️  Удаление {len(cache_dirs)} директорий кэша...")
        for cache_dir in cache_dirs:
            try:
                shutil.rmtree(cache_dir)
            except Exception as e:
                print(f"   ⚠️  Не удалось удалить {cache_dir}: {e}")
        print("✅ Кэш очищен!")
    
    return True

def main():
    """Главная функция"""
    print("=" * 80)
    print("🔧 ИСПРАВЛЕНИЕ NETWORKX ДЛЯ PYTHON 3.14")
    print("=" * 80)
    print()
    
    # Проверяем версию Python
    if sys.version_info < (3, 14):
        print(f"ℹ️  У вас Python {sys.version_info.major}.{sys.version_info.minor}")
        print("   Исправление требуется только для Python 3.14+")
        print("   Но мы все равно можем применить его для совместимости.")
        print()
    
    if fix_networkx():
        print()
        print("=" * 80)
        print("✅ ИСПРАВЛЕНИЕ ЗАВЕРШЕНО!")
        print("=" * 80)
        print()
        print("Теперь вы можете запустить:")
        print("  python test_quick.py")
        print("  python main.py")
        print()
        return 0
    else:
        print()
        print("=" * 80)
        print("❌ ИСПРАВЛЕНИЕ НЕ УДАЛОСЬ!")
        print("=" * 80)
        print()
        print("Попробуйте выполнить исправление вручную.")
        print("См. инструкции в PYTHON_3.14_FIX.md")
        print()
        return 1

if __name__ == '__main__':
    sys.exit(main())


