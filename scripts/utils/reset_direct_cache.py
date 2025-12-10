"""Очистка кэша Yandex Direct для пересбора данных"""

import sqlite3
from pathlib import Path

DB_PATH = "output/serp_data.db"

print("=" * 80)
print("🗑️  ОЧИСТКА КЭША YANDEX DIRECT")
print("=" * 80)
print()

if not Path(DB_PATH).exists():
    print(f"❌ БД не найдена: {DB_PATH}")
    exit(1)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Проверяем наличие таблицы
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='direct_forecasts'")
if not cursor.fetchone():
    print("✅ Таблица direct_forecasts не существует - очистка не требуется")
    conn.close()
    exit(0)

# Подсчёт записей ПЕРЕД
cursor.execute("SELECT COUNT(*) FROM direct_forecasts")
count_before = cursor.fetchone()[0]

print(f"📊 Записей в кэше: {count_before}")
print()

if count_before == 0:
    print("✅ Кэш уже пустой")
    conn.close()
    exit(0)

# Очистка
print("🔄 Удаление всех записей...")
cursor.execute("DELETE FROM direct_forecasts")
conn.commit()

# Подсчёт записей ПОСЛЕ
cursor.execute("SELECT COUNT(*) FROM direct_forecasts")
count_after = cursor.fetchone()[0]

conn.close()

print()
print("=" * 80)
print("✅ КЭШDIRECT ОЧИЩЕН")
print("=" * 80)
print(f"   Удалено записей: {count_before}")
print(f"   Осталось записей: {count_after}")
print()
print("📝 Теперь запусти пересбор данных:")
print("   python main.py semantika/скуд.csv --enable-direct")
print()

