"""
Валидация LSI данных
"""

import json
from typing import Any


class LSIValidator:
    """Валидатор LSI данных"""
    
    @staticmethod
    def has_valid_lsi(lsi_json_str: Any) -> bool:
        """
        Проверяет, есть ли валидные LSI данные в JSON строке
        
        Args:
            lsi_json_str: JSON строка или объект с LSI данными
            
        Returns:
            True если есть валидные LSI данные
        """
        if not lsi_json_str or (isinstance(lsi_json_str, str) and (lsi_json_str.strip() == '' or lsi_json_str.strip() == '[]')):
            return False
        try:
            if isinstance(lsi_json_str, str):
                lsi_data = json.loads(lsi_json_str)
            else:
                lsi_data = lsi_json_str
            
            if not isinstance(lsi_data, list):
                return False
            if len(lsi_data) == 0:
                return False
            
            # Проверяем, что есть хотя бы один объект с полем "phrase"
            for item in lsi_data:
                if isinstance(item, dict) and 'phrase' in item:
                    return True
            return False
        except (json.JSONDecodeError, TypeError):
            return False

