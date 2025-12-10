"""Проверка количества строк в CSV файле"""
import pandas as pd
from pathlib import Path

csv_path = Path("semantika/видеонаблюдение.csv")

if csv_path.exists():
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    print(f"📄 Файл: {csv_path}")
    print(f"📊 Строк в CSV: {len(df)}")
    print(f"📋 Колонки: {list(df.columns)}")
else:
    print(f"❌ Файл не найден: {csv_path}")

