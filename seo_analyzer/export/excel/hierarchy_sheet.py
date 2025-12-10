"""Создание листа с иерархией проекта в Excel"""

import pandas as pd
from typing import Optional


def create_hierarchy_sheet(
    writer: pd.ExcelWriter,
    formats: dict,
    hierarchy_df: Optional[pd.DataFrame] = None
):
    """
    Создать лист с иерархией проекта
    
    Args:
        writer: ExcelWriter объект
        formats: Словарь с форматами
        hierarchy_df: DataFrame с данными иерархии (опционально)
    """
    sheet_name = 'Иерархия проекта'
    
    if hierarchy_df is None or hierarchy_df.empty:
        # Создаем пустой лист с пояснением
        placeholder_df = pd.DataFrame([{
            'Информация': 'Иерархия не сгенерирована. Для построения иерархии требуется:',
        }, {
            'Информация': '1. SERP данные с URL',
        }, {
            'Информация': '2. API ключ DeepSeek',
        }, {
            'Информация': '3. Запуск анализа иерархии через pipeline'
        }])
        
        placeholder_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        worksheet = writer.sheets[sheet_name]
        worksheet.set_column(0, 0, 80)
        
        # Форматируем заголовок
        worksheet.write(0, 0, 'Информация', formats['header'])
        
        print(f"  ℹ️  Создан лист '{sheet_name}' (пустой)")
        return
    
    # Записываем данные
    hierarchy_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)
    
    worksheet = writer.sheets[sheet_name]
    
    # Записываем заголовки
    for col_num, col_name in enumerate(hierarchy_df.columns):
        worksheet.write(0, col_num, col_name, formats['header'])
    
    # Настройки листа
    worksheet.freeze_panes(1, 0)
    
    # Автофильтр
    if len(hierarchy_df) > 0:
        worksheet.autofilter(0, 0, len(hierarchy_df), len(hierarchy_df.columns) - 1)
    
    # Настройка ширины колонок
    column_widths = {
        'Кластер': 15,
        'Контекст': 40,
        'Уровень': 10,
        'Название': 30,
        'URL slug': 30,
        'Полный путь': 60
    }
    
    for col_num, col_name in enumerate(hierarchy_df.columns):
        width = column_widths.get(col_name, 20)
        worksheet.set_column(col_num, col_num, width)
    
    # Условное форматирование для уровней
    if 'Уровень' in hierarchy_df.columns:
        level_col = hierarchy_df.columns.get_loc('Уровень')
        
        # Уровень 1 - синий фон
        worksheet.conditional_format(1, level_col, len(hierarchy_df), level_col, {
            'type': 'cell',
            'criteria': '==',
            'value': 1,
            'format': writer.book.add_format({'bg_color': '#D6EAF8'})
        })
        
        # Уровень 2 - светло-синий
        worksheet.conditional_format(1, level_col, len(hierarchy_df), level_col, {
            'type': 'cell',
            'criteria': '==',
            'value': 2,
            'format': writer.book.add_format({'bg_color': '#EBF5FB'})
        })
        
        # Уровень 3 - очень светлый
        worksheet.conditional_format(1, level_col, len(hierarchy_df), level_col, {
            'type': 'cell',
            'criteria': '==',
            'value': 3,
            'format': writer.book.add_format({'bg_color': '#F8F9F9'})
        })
    
    print(f"  ✓ Создан лист '{sheet_name}' с {len(hierarchy_df)} записями")


