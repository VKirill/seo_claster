"""
Конвертация LSI фраз для Excel
"""

import json
import numpy as np


def convert_query_lsi_phrases(x):
    """
    Конвертировать LSI фразы запроса в строку для Excel
    
    Args:
        x: LSI фразы (список, строка JSON или None)
        
    Returns:
        Строка с фразами через запятую
    """
    if x is None:
        return ''
    
    if isinstance(x, (list, tuple, np.ndarray)):
        if len(x) == 0:
            return ''
        phrases = []
        for item in x[:20]:  # Топ-20 фраз
            if isinstance(item, dict):
                phrase = item.get('phrase', '')
                if phrase:
                    phrases.append(phrase)
            elif isinstance(item, str):
                if item.strip():
                    phrases.append(item.strip())
        return ', '.join(phrases) if phrases else ''
    elif isinstance(x, str):
        if x.strip() == '' or x.strip() == '[]':
            return ''
        try:
            parsed = json.loads(x)
            if isinstance(parsed, list):
                phrases = []
                for item in parsed[:20]:
                    if isinstance(item, dict):
                        phrase = item.get('phrase', '')
                        if phrase:
                            phrases.append(phrase)
                    elif isinstance(item, str):
                        if item.strip():
                            phrases.append(item.strip())
                return ', '.join(phrases) if phrases else ''
        except (json.JSONDecodeError, TypeError):
            return x
    return ''


def convert_cluster_lsi_phrases(x):
    """
    Конвертировать LSI фразы кластера в строку для Excel
    
    Args:
        x: LSI фразы (список, строка JSON или None)
        
    Returns:
        Строка с фразами через запятую
    """
    if x is None:
        return ''
    
    if isinstance(x, (list, tuple, np.ndarray)):
        if len(x) == 0:
            return ''
        phrases = []
        for item in x[:20]:  # Топ-20 фраз
            if isinstance(item, dict):
                phrase = item.get('phrase', '')
                if phrase:
                    phrases.append(phrase)
            elif isinstance(item, str):
                if item.strip():
                    phrases.append(item.strip())
        return ', '.join(phrases) if phrases else ''
    elif isinstance(x, str):
        if x.strip() == '' or x.strip() == '[]':
            return ''
        # Если это уже готовая строка с фразами - возвращаем как есть
        if ',' in x or len(x) > 50:
            return x
        # Иначе пытаемся распарсить как JSON
        try:
            parsed = json.loads(x)
            if isinstance(parsed, list):
                phrases = []
                for item in parsed[:20]:
                    if isinstance(item, dict):
                        phrase = item.get('phrase', '')
                        if phrase:
                            phrases.append(phrase)
                    elif isinstance(item, str):
                        if item.strip():
                            phrases.append(item.strip())
                return ', '.join(phrases) if phrases else ''
        except (json.JSONDecodeError, TypeError):
            return x
    return ''


