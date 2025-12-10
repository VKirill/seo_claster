"""
Создание листа с LSI фразами
"""

import sys
import importlib.util
from pathlib import Path
import pandas as pd


def create_lsi_sheet(df: pd.DataFrame, writer: pd.ExcelWriter, formats: dict):
    """
    Создать лист с LSI фразами
    
    Args:
        df: DataFrame с данными
        writer: ExcelWriter объект
        formats: Словарь с форматами
    """
    backup_path = Path(__file__).parent.parent / 'data_writer.py.backup'
    if backup_path.exists():
        spec = importlib.util.spec_from_file_location("data_writer_backup", backup_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.create_lsi_sheet(df, writer, formats)
    else:
        from ..data_writer import create_lsi_sheet as _create_lsi_sheet
        return _create_lsi_sheet(df, writer, formats)

