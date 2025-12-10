"""Финальная проверка результатов"""

import pandas as pd

print("=" * 80)
print("ФИНАЛЬНАЯ ПРОВЕРКА РЕЗУЛЬТАТОВ")
print("=" * 80)

# Проверяем CSV
print("\n📄 CSV файл (seo_analysis_full.csv):")
csv_df = pd.read_csv('output/seo_analysis_full.csv', sep=';')
print(f"  Всего запросов: {len(csv_df)}")

if 'serp_urls' in csv_df.columns:
    csv_empty = csv_df['serp_urls'].isna().sum() + (csv_df['serp_urls'] == '').sum()
    csv_filled = len(csv_df) - csv_empty
    print(f"  serp_urls: {csv_filled} заполнено ({csv_filled/len(csv_df)*100:.1f}%), {csv_empty} пусто ({csv_empty/len(csv_df)*100:.1f}%)")
    
    print(f"\n  Примеры (первые 5):")
    for idx in range(min(5, len(csv_df))):
        keyword = csv_df.iloc[idx]['keyword']
        serp_urls = csv_df.iloc[idx]['serp_urls']
        is_empty = (pd.isna(serp_urls) or serp_urls == '')
        status = "✗ ПУСТО" if is_empty else f"✓ {len(str(serp_urls))} символов"
        print(f"    {keyword}: {status}")

# Проверяем Excel
print("\n📊 Excel файл (seo_analysis.xlsx):")
excel_df = pd.read_excel('output/seo_analysis.xlsx', sheet_name='All Queries')
print(f"  Всего запросов: {len(excel_df)}")

if 'serp_urls' in excel_df.columns:
    excel_empty = excel_df['serp_urls'].isna().sum() + (excel_df['serp_urls'] == '').sum()
    excel_filled = len(excel_df) - excel_empty
    print(f"  serp_urls: {excel_filled} заполнено ({excel_filled/len(excel_df)*100:.1f}%), {excel_empty} пусто ({excel_empty/len(excel_df)*100:.1f}%)")
    
    print(f"\n  Примеры (первые 5):")
    for idx in range(min(5, len(excel_df))):
        keyword = excel_df.iloc[idx]['keyword']
        serp_urls = excel_df.iloc[idx]['serp_urls']
        is_empty = (pd.isna(serp_urls) or serp_urls == '')
        status = "✗ ПУСТО" if is_empty else f"✓ {len(str(serp_urls))} символов"
        print(f"    {keyword}: {status}")

print("\n" + "=" * 80)
if csv_filled > len(csv_df) * 0.9 and excel_filled > len(excel_df) * 0.9:
    print("✅ ОТЛИЧНО! Более 90% запросов имеют SERP URLs в обоих файлах!")
elif csv_filled > len(csv_df) * 0.9:
    print("⚠️  CSV файл в порядке (>90%), но Excel может иметь проблемы.")
    print("    Используйте CSV файл для работы!")
else:
    print("❌ Проблема сохраняется. Требуется дополнительное исследование.")


