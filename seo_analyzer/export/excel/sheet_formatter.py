"""Форматирование листов Excel"""

from typing import Dict, List
import pandas as pd


def create_formats(workbook) -> Dict:
    """
    Создать форматы для ячеек Excel
    
    Args:
        workbook: xlsxwriter workbook объект
        
    Returns:
        Словарь с форматами
    """
    formats = {}
    
    # Заголовок
    formats['header'] = workbook.add_format({
        'bold': True,
        'bg_color': '#4472C4',
        'font_color': 'white',
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'text_wrap': True
    })
    
    # Заголовок кластера
    formats['cluster_header'] = workbook.add_format({
        'bold': True,
        'bg_color': '#D9E1F2',
        'border': 1,
        'align': 'left'
    })
    
    # Обычная ячейка
    formats['cell'] = workbook.add_format({
        'border': 1,
        'align': 'left',
        'valign': 'top'
    })
    
    # Число
    formats['number'] = workbook.add_format({
        'border': 1,
        'align': 'right',
        'num_format': '#,##0'
    })
    
    # Процент
    formats['percent'] = workbook.add_format({
        'border': 1,
        'align': 'right',
        'num_format': '0.00%'
    })
    
    # Decimal
    formats['decimal'] = workbook.add_format({
        'border': 1,
        'align': 'right',
        'num_format': '0.00'
    })
    
    # High value (green)
    formats['high'] = workbook.add_format({
        'border': 1,
        'bg_color': '#C6EFCE',
        'font_color': '#006100',
        'align': 'right'
    })
    
    # Low value (red)
    formats['low'] = workbook.add_format({
        'border': 1,
        'bg_color': '#FFC7CE',
        'font_color': '#9C0006',
        'align': 'right'
    })
    
    return formats


def set_column_widths(worksheet, columns: List[str]):
    """
    Установить ширину колонок
    
    Args:
        worksheet: xlsxwriter worksheet объект
        columns: Список названий колонок
    """
    widths = {
        'query': 40,
        'cluster_name': 35,
        'cluster_lsi_phrases': 50,
        'main_intent': 15,
        'frequency': 12,
        'difficulty': 12,
        'priority': 12,
        'kei': 12,
        'serp': 12,
        'default': 15
    }
    
    for col_num, col_name in enumerate(columns):
        col_lower = str(col_name).lower()
        
        # Определяем ширину
        width = widths['default']
        for key, value in widths.items():
            if key in col_lower:
                width = value
                break
        
        worksheet.set_column(col_num, col_num, width)


def add_conditional_formatting(
    worksheet,
    df: pd.DataFrame,
    sheet_name: str
):
    """
    Добавить условное форматирование
    
    Args:
        worksheet: xlsxwriter worksheet объект
        df: DataFrame с данными
        sheet_name: Название листа
    """
    # Находим колонки для форматирования
    for col_num, col_name in enumerate(df.columns):
        col_lower = str(col_name).lower()
        
        # KEI effectiveness - зеленый=высокий, красный=низкий
        if 'effectiveness' in col_lower or 'priority' in col_lower:
            worksheet.conditional_format(1, col_num, len(df), col_num, {
                'type': '3_color_scale',
                'min_color': '#F8696B',
                'mid_color': '#FFEB84',
                'max_color': '#63BE7B'
            })
        
        # Competition - красный=высокий, зеленый=низкий (инвертировано)
        elif 'competition' in col_lower or 'difficulty' in col_lower:
            worksheet.conditional_format(1, col_num, len(df), col_num, {
                'type': '3_color_scale',
                'min_color': '#63BE7B',
                'mid_color': '#FFEB84',
                'max_color': '#F8696B'
            })
        
        # Frequency - зеленый=высокий
        elif 'frequency' in col_lower:
            worksheet.conditional_format(1, col_num, len(df), col_num, {
                'type': '2_color_scale',
                'min_color': '#FFFFFF',
                'max_color': '#63BE7B'
            })


def add_cluster_grouping(
    worksheet,
    df: pd.DataFrame,
    cluster_column: str
):
    """
    Добавить группировку по кластерам
    
    Args:
        worksheet: xlsxwriter worksheet объект
        df: DataFrame с данными
        cluster_column: Название колонки с кластерами
    """
    current_cluster = None
    start_row = 1
    
    for idx, row in df.iterrows():
        cluster_id = row[cluster_column]
        
        if current_cluster != cluster_id:
            if current_cluster is not None and start_row < idx:
                # Группируем предыдущий кластер
                try:
                    worksheet.set_row(start_row, None, None, {'level': 1, 'hidden': False})
                except:
                    pass
            
            current_cluster = cluster_id
            start_row = idx + 1


def apply_number_formats(
    worksheet,
    df: pd.DataFrame,
    formats: dict
):
    """
    Применить форматирование чисел к колонкам
    
    Args:
        worksheet: xlsxwriter worksheet объект
        df: DataFrame с данными
        formats: Словарь с форматами
    """
    for col_num, col_name in enumerate(df.columns):
        col_lower = str(col_name).lower()
        
        # Определяем какой формат применить
        format_to_use = None
        
        # Целые числа (частота, количество)
        if any(word in col_lower for word in ['frequency', 'count', 'docs']):
            format_to_use = formats['number']
        
        # Дробные числа с 2 знаками после запятой
        elif any(word in col_lower for word in [
            'score', 'kei', 'difficulty', 'priority', 'effectiveness',
            'competition', 'ratio', 'coefficient', 'popularity', 'traffic',
            'cost', 'revenue', 'synergy', 'relevance', 'normalized',
            'potential', 'value', 'ctr'
        ]):
            format_to_use = formats['decimal']
        
        # Применяем формат к колонке (со 2-й строки, т.к. 1-я это заголовок)
        # ВАЖНО: Записываем ВСЕ значения, включая 0, чтобы не потерять данные
        if format_to_use:
            for row_num in range(1, len(df) + 1):
                try:
                    cell_value = df.iloc[row_num - 1, col_num]
                    # Записываем все числовые значения, включая 0
                    if isinstance(cell_value, (int, float)):
                        worksheet.write(row_num, col_num, cell_value, format_to_use)
                    elif pd.notna(cell_value):
                        # Если это не число, но не NaN, записываем как есть
                        worksheet.write(row_num, col_num, cell_value)
                except Exception as e:
                    # Игнорируем ошибки записи отдельных ячеек
                    pass
