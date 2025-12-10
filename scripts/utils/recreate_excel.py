"""
Пересоздание Excel файла из CSV (если Excel был открыт)
"""
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from seo_analyzer.export.excel_exporter import ExcelExporter
from seo_analyzer.core.config import EXCEL_CONFIG

# Загружаем данные из CSV
csv_file = Path('output/seo_analysis_full.csv')
if not csv_file.exists():
    print(f"❌ Файл не найден: {csv_file}")
    exit(1)

print("📂 Загрузка данных из CSV...")
df = pd.read_csv(csv_file)
print(f"✓ Загружено {len(df)} запросов")

# Пытаемся удалить старый Excel (если не открыт)
excel_file = Path('output/seo_analysis.xlsx')
if excel_file.exists():
    try:
        excel_file.unlink()
        print(f"✓ Удалён старый файл: {excel_file}")
    except PermissionError:
        print(f"❌ ОШИБКА: Файл {excel_file} открыт в другой программе!")
        print(f"   Закройте Excel и запустите этот скрипт снова.")
        exit(1)

# Создаём новый Excel
print("💾 Создание Excel файла...")
exporter = ExcelExporter()

try:
    exporter.export_to_excel(
        df,
        output_path=excel_file,
        include_charts=True,
        group_by_clusters=True
    )
    print(f"✅ Excel файл создан: {excel_file}")
    
    # Проверяем покрытие
    df_check = pd.read_excel(excel_file, sheet_name='All Queries')
    with_serp = (df_check['serp_docs_count'] > 0).sum()
    print(f"✓ Проверка: {with_serp} из {len(df_check)} запросов с SERP данными ({with_serp/len(df_check)*100:.1f}%)")
    
except PermissionError:
    print(f"❌ ОШИБКА: Не удалось создать {excel_file}")
    print(f"   Убедитесь что файл закрыт в Excel!")
except Exception as e:
    print(f"❌ ОШИБКА: {e}")

