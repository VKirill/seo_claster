"""
Пересоздание Excel и JSON с обновленными LSI фразами
Используется когда БД была переобработана но результаты старые
"""
import sys
import pandas as pd
from pathlib import Path
import asyncio

# Проверяем аргументы
if len(sys.argv) < 2:
    print("❌ Укажите группу: python rebuild_exports.py <имя_группы>")
    print("   Например: python rebuild_exports.py скуд")
    sys.exit(1)

group_name = sys.argv[1]
group_dir = Path(f"output/groups/{group_name}")

if not group_dir.exists():
    print(f"❌ Группа '{group_name}' не найдена в output/groups/")
    sys.exit(1)

print("=" * 80)
print(f"🔄 ПЕРЕСОЗДАНИЕ ЭКСПОРТОВ ДЛЯ ГРУППЫ: {group_name}")
print("=" * 80)
print()

# Ищем CSV файл
csv_file = group_dir / "seo_analysis_full.csv"
if not csv_file.exists():
    print(f"❌ Файл {csv_file} не найден!")
    sys.exit(1)

print(f"📂 Загружаем данные из {csv_file.name}...")
df = pd.read_csv(csv_file, encoding='utf-8')
print(f"✓ Загружено {len(df)} запросов")
print()

# Проверяем наличие LSI в DataFrame
if 'lsi_phrases' not in df.columns:
    print("⚠️ В CSV нет колонки 'lsi_phrases'")
    print("   Запустите полный анализ заново: python manage_groups.py")
    sys.exit(1)

# Загружаем LSI из БД для каждого запроса
print("🔄 Загружаем LSI фразы из обновленной БД...")

from seo_analyzer.core.serp.database import SERPDatabase
db = SERPDatabase(db_path=Path('output/serp_data.db'), query_group=group_name)

# Загружаем LSI для каждого запроса
lsi_data = []
keywords_with_lsi = 0

for idx, row in df.iterrows():
    keyword = row['keyword']
    serp_data = db.get_serp_data(keyword, lr=213)
    
    if serp_data and serp_data.get('lsi_phrases'):
        lsi_phrases = serp_data['lsi_phrases']
        lsi_data.append(lsi_phrases)
        keywords_with_lsi += 1
    else:
        lsi_data.append([])
    
    if (idx + 1) % 100 == 0:
        print(f"  Обработано: {idx + 1}/{len(df)}...")

df['lsi_phrases'] = lsi_data

print(f"✓ LSI фразы загружены: {keywords_with_lsi}/{len(df)} запросов имеют LSI")
print()

# Проверяем наличие кластеризации
if 'semantic_cluster_id' not in df.columns:
    print("⚠️ В данных нет кластеризации (semantic_cluster_id)")
    print("   Запустите полный анализ заново: python manage_groups.py")
    sys.exit(1)

# Агрегируем LSI по кластерам
print("🔄 Агрегация LSI по кластерам...")
from seo_analyzer.analysis.cluster_lsi_aggregator import ClusterLSIAggregator

aggregator = ClusterLSIAggregator(top_n_per_cluster=30)

cluster_lsi = aggregator.aggregate_cluster_lsi(
    df,
    cluster_column='semantic_cluster_id'
)

df = aggregator.add_cluster_lsi_to_dataframe(
    df,
    cluster_lsi,
    cluster_column='semantic_cluster_id'
)

clusters_with_lsi = sum(1 for lsi_list in cluster_lsi.values() if lsi_list)
print(f"✓ Кластеров с LSI: {clusters_with_lsi}/{len(cluster_lsi)}")
print()

# Экспортируем cluster_lsi_phrases.csv
print("📝 Экспорт cluster_lsi_phrases.csv...")
lsi_csv_path = group_dir / 'cluster_lsi_phrases.csv'
aggregator.export_cluster_lsi(cluster_lsi, lsi_csv_path)
print(f"✓ Сохранено: {lsi_csv_path}")
print()

# Пересоздаем Excel
print("📊 Создание Excel...")
from seo_analyzer.export.excel.workbook_builder import ExcelWorkbookBuilder

excel_path = group_dir / 'seo_analysis.xlsx'
builder = ExcelWorkbookBuilder(output_path=excel_path)
builder.build(df)
print(f"✓ Сохранено: {excel_path}")
print()

# Пересоздаем JSON
print("📋 Создание JSON...")
from seo_analyzer.export.json_exporter import JSONExporter

json_path = group_dir / 'seo_analysis_hierarchy.json'
exporter = JSONExporter(output_path=json_path)
exporter.export(df)
print(f"✓ Сохранено: {json_path}")
print()

print("=" * 80)
print("✅ ЭКСПОРТЫ ОБНОВЛЕНЫ")
print("=" * 80)
print()
print(f"📂 Файлы с LSI фразами:")
print(f"   - {excel_path}")
print(f"   - {json_path}")
print(f"   - {lsi_csv_path}")
print()
print("🎉 Проверьте файлы - LSI фразы должны быть заполнены!")

