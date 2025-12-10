"""
Извлечение LSI фраз из данных
"""

import json
from typing import List, Dict, Any


class PhraseExtractor:
    """Извлекатель LSI фраз"""
    
    @staticmethod
    def extract_phrases(lsi_data) -> List[Dict[str, Any]]:
        """
        Извлекает и нормализует LSI фразы из данных
        
        Args:
            lsi_data: Список фраз, JSON строка или другой формат
            
        Returns:
            Список нормализованных фраз в формате словарей
        """
        if lsi_data is None:
            return []
        
        # Парсим JSON строку если нужно
        if isinstance(lsi_data, str):
            try:
                lsi_data = json.loads(lsi_data) if lsi_data.strip() else []
            except (json.JSONDecodeError, TypeError):
                return []
        
        # Проверяем что это список и он не пустой
        if not isinstance(lsi_data, list) or len(lsi_data) == 0:
            return []
        
        phrases = []
        for item in lsi_data:
            if isinstance(item, dict):
                # Правильный формат: словарь с ключом 'phrase'
                phrase = item.get('phrase', '')
                if phrase:
                    phrases.append(item)
            elif isinstance(item, str):
                # Если это строка - оборачиваем в словарь
                if item.strip():
                    phrases.append({
                        'phrase': item.strip(),
                        'frequency': 1,
                        'source': 'unknown'
                    })
        
        return phrases

