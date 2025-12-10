"""
Создание листов с интентами
"""

import sys
import importlib.util
from pathlib import Path
import pandas as pd

from .intent_filter_impl import (
    create_intent_filtered_sheet_impl as _create_intent_filtered_sheet_impl,
    create_intent_summary_sheet_impl as _create_intent_summary_sheet_impl
)


def create_intent_summary_sheet(df: pd.DataFrame, writer: pd.ExcelWriter, formats: dict):
    """
    Создать лист с интентами
    
    Args:
        df: DataFrame с данными
        writer: ExcelWriter объект
        formats: Словарь с форматами
    """
    backup_path = Path(__file__).parent.parent / 'data_writer.py.backup'
    if backup_path.exists():
        spec = importlib.util.spec_from_file_location("data_writer_backup", str(backup_path))
        if spec is not None and spec.loader is not None:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.create_intent_summary_sheet(df, writer, formats)
    
    # Fallback: используем базовую реализацию
    return _create_intent_summary_sheet_impl(df, writer, formats)


def create_intent_filtered_sheet(
    df: pd.DataFrame,
    writer: pd.ExcelWriter,
    formats: dict,
    intent_type: str,
    group_by_clusters: bool = True
):
    """
    Создать отфильтрованный лист по интенту
    
    Args:
        df: DataFrame с данными
        writer: ExcelWriter объект
        formats: Словарь с форматами
        intent_type: Тип интента для фильтрации
        group_by_clusters: Группировать по кластерам
    """
    backup_path = Path(__file__).parent.parent / 'data_writer.py.backup'
    if backup_path.exists():
        spec = importlib.util.spec_from_file_location("data_writer_backup", str(backup_path))
        if spec is not None and spec.loader is not None:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module.create_intent_filtered_sheet(df, writer, formats, intent_type, group_by_clusters)
    
    # Fallback: используем базовую реализацию
    return _create_intent_filtered_sheet_impl(df, writer, formats, intent_type, group_by_clusters)

