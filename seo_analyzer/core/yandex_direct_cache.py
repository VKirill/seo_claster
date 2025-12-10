"""
Кэш для данных Yandex Direct API.

Сохраняет прогнозы в единую БД serp_data.db для избежания повторных запросов.
Данные актуальны 30 дней.
"""

import sqlite3
from typing import Optional, Dict
from datetime import datetime, timedelta

from .yandex_direct_cache_schema import (
    CREATE_TABLE_SQL,
    CREATE_PHRASE_INDEX_SQL,
    CREATE_UPDATED_INDEX_SQL,
    SELECT_FORECAST_SQL,
    INSERT_FORECAST_SQL,
    DELETE_OLD_SQL
)


class YandexDirectCache:
    """Кэш для прогнозов Yandex Direct."""
    
    CACHE_VALIDITY_DAYS = 30
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Инициализация кэша.
        
        Args:
            db_path: Путь к файлу БД (по умолчанию serp_data.db - единая БД)
        """
        if db_path is None:
            db_path = "serp_data.db"  # Единая БД для всех данных
            
        self.db_path = db_path
        self._init_database()
        
    def _init_database(self):
        """Создание таблицы для кэша если её нет."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(CREATE_TABLE_SQL)
        cursor.execute(CREATE_PHRASE_INDEX_SQL)
        cursor.execute(CREATE_UPDATED_INDEX_SQL)
        
        conn.commit()
        conn.close()
        
    def get(self, phrase: str, geo_id: int = 213) -> Optional[Dict]:
        """
        Получение данных из кэша.
        
        Args:
            phrase: Ключевая фраза
            geo_id: ID региона
            
        Returns:
            Dict с данными или None если нет в кэше или устарело
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(SELECT_FORECAST_SQL, (phrase.lower().strip(), geo_id))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
            
        # Проверка актуальности
        updated_at = datetime.fromisoformat(row["updated_at"])
        if datetime.now() - updated_at > timedelta(days=self.CACHE_VALIDITY_DAYS):
            return None
            
        return dict(row)
        
    def set(self, data: Dict, geo_id: int = 213):
        """
        Сохранение данных в кэш.
        
        Args:
            data: Данные прогноза (из YandexDirectParser)
            geo_id: ID региона
        """
        phrase = data["phrase"].lower().strip()
        now = datetime.now().isoformat()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(INSERT_FORECAST_SQL, (
            phrase, geo_id, data["shows"], data["clicks"],
            data["ctr"], data["premium_ctr"],
            data["min_cpc"], data["avg_cpc"], data["max_cpc"], data["recommended_cpc"],
            data["competition_level"], data["first_place_bid"], data["first_place_price"],
            phrase, now, now
        ))
        
        conn.commit()
        conn.close()
        
    def clear_old(self):
        """Удаление устаревших записей."""
        cutoff_date = (datetime.now() - timedelta(days=self.CACHE_VALIDITY_DAYS * 2)).isoformat()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(DELETE_OLD_SQL, (cutoff_date,))
        
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        
        return deleted

