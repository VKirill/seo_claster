"""Генератор FAQ страницы для Excel"""

import pandas as pd
from .faq_data import get_faq_data


def create_faq_sheet(writer: pd.ExcelWriter, formats: dict):
    """
    Создать FAQ страницу с описанием столбцов
    
    Args:
        writer: ExcelWriter объект
        formats: Словарь с форматами
    """
    sheet_name = 'FAQ'
    
    faq_data = get_faq_data()
    faq_df = pd.DataFrame(faq_data)
    
    # Записываем данные
    faq_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    worksheet = writer.sheets[sheet_name]
    
    # Заморозить первую строку
    worksheet.freeze_panes(1, 0)
    
    # Автофильтр
    worksheet.autofilter(0, 0, len(faq_df), len(faq_df.columns) - 1)
    
    # Настройка ширины колонок
    worksheet.set_column(0, 0, 25)  # Страница
    worksheet.set_column(1, 1, 30)  # Столбец
    worksheet.set_column(2, 2, 80)  # Описание
    
    # Форматирование заголовков
    for col_num in range(len(faq_df.columns)):
        worksheet.write(0, col_num, faq_df.columns[col_num], formats['header'])
    
    print(f"  ✓ Создана страница FAQ с {len(faq_df)} описаниями")
