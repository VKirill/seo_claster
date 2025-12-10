"""Тестирование автоматической синхронизации CSV с кэшем"""
import shutil
from pathlib import Path

# Пути
csv_path = Path("semantika/видеонаблюдение.csv")
backup_path = Path("semantika/видеонаблюдение_backup.csv")
test_backup_path = Path("semantika/видеонаблюдение_test_backup.csv")

print("🧪 ТЕСТИРОВАНИЕ СИНХРОНИЗАЦИИ CSV С КЭШЕМ")
print("=" * 60)

# Сохраняем текущий очищенный файл
if csv_path.exists():
    shutil.copy2(csv_path, test_backup_path)
    print(f"✓ Создан тестовый backup: {test_backup_path}")

# Копируем backup с дубликатами обратно в основной файл
if backup_path.exists():
    shutil.copy2(backup_path, csv_path)
    print(f"✓ Восстановлен файл с дубликатами: {csv_path}")
    
    import pandas as pd
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    print(f"📊 Строк в CSV теперь: {len(df)}")
    print()
    print("🚀 Теперь запустите: python main.py видеонаблюдение")
    print("   Система должна автоматически обнаружить дубликаты и синхронизировать файл")
else:
    print(f"❌ Backup файл не найден: {backup_path}")

