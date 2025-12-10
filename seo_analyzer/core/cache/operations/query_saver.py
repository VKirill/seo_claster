"""
Сохранение запросов в Master DB
"""

import sys
import importlib.util
import pandas as pd
from pathlib import Path
from typing import Optional


class QuerySaver:
    """Сохранение запросов в Master DB"""
    
    def __init__(self, db_path: Path, query_loader):
        """
        Args:
            db_path: Путь к базе данных
            query_loader: Экземпляр QueryLoader для загрузки существующих данных
        """
        self.db_path = db_path
        self.query_loader = query_loader
        self._old_db_class = None
    
    def _get_old_db_class(self):
        """Получить старый класс MasterQueryDatabase из backup файла"""
        if self._old_db_class is None:
            backup_path = Path(__file__).parent.parent / 'master_query_db.py.backup'
            if backup_path.exists():
                spec = importlib.util.spec_from_file_location("master_query_db_backup", backup_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self._old_db_class = module.MasterQueryDatabase
        return self._old_db_class
    
    def save_queries(
        self,
        group_name: str,
        df: pd.DataFrame,
        csv_path: Path = None,
        csv_hash: str = None
    ):
        """
        Сохраняет/обновляет запросы в master таблице
        
        Args:
            group_name: Название группы
            df: DataFrame со ВСЕМИ обработанными данными
            csv_path: Путь к CSV (опционально)
            csv_hash: Hash CSV (опционально)
        """
        # Используем старую логику из backup файла для сохранения обратной совместимости
        # В будущем можно будет рефакторить дальше
        OldDBClass = self._get_old_db_class()
        if OldDBClass:
            temp_db = OldDBClass(self.db_path)
            temp_db.save_queries(group_name, df, csv_path, csv_hash)
        else:
            # Fallback: используем текущий класс если backup недоступен
            from ..master_query_db import MasterQueryDatabase
            temp_db = MasterQueryDatabase(self.db_path)
            temp_db.save_queries(group_name, df, csv_path, csv_hash)

