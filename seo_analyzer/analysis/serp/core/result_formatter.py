"""
Форматирование результатов SERP анализа
"""

import json
from typing import Dict, Any, Optional, Tuple

from seo_analyzer.core.serp.serp_data_normalizer import SERPDataNormalizer


class ResultFormatter:
    """Форматирование результатов SERP"""
    
    def __init__(self, lr: int):
        """
        Args:
            lr: Регион поиска
        """
        self.lr = lr
    
    def create_result(
        self,
        query: str,
        api_result: Dict[str, Any],
        source: str = 'api',
        error: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Создать результат из API ответа
        
        Args:
            query: Запрос
            api_result: Результат от API
            source: Источник данных
            error: Текст ошибки (если есть)
            
        Returns:
            Форматированный результат
        """
        return {
            'query': query,
            'lr': self.lr,
            'source': source,
            'cached_at': None,
            'error': error,
            'metrics': api_result['metrics'],
            'documents': api_result['documents'],
            'lsi_phrases': api_result['lsi_phrases']
        }
    
    def format_serp_result(self, query: str, row: Tuple) -> Dict[str, Any]:
        """
        Форматировать результат SERP из БД
        
        Args:
            query: Запрос
            row: Строка из БД (tuple с данными)
            
        Returns:
            Форматированный результат
        """
        # Парсим JSON данные (индексы сдвинулись из-за добавления serp_req_id)
        # Безопасный парсинг с обработкой ошибок
        top_urls = []
        if row[7]:
            try:
                if isinstance(row[7], str):
                    if row[7].strip():
                        top_urls = json.loads(row[7])
                    else:
                        top_urls = []
                else:
                    top_urls = row[7] if row[7] else []
            except (json.JSONDecodeError, TypeError):
                top_urls = []
        
        lsi_phrases = []
        if row[8]:
            try:
                if isinstance(row[8], str):
                    if row[8].strip():
                        lsi_phrases = json.loads(row[8])
                    else:
                        lsi_phrases = []
                else:
                    lsi_phrases = row[8] if row[8] else []
            except (json.JSONDecodeError, TypeError):
                lsi_phrases = []
        
        # Нормализуем формат LSI фраз: преобразуем в список словарей
        normalized_lsi = []
        if lsi_phrases:
            for item in lsi_phrases:
                if isinstance(item, dict):
                    # Уже словарь - проверяем наличие ключа 'phrase'
                    if 'phrase' in item:
                        normalized_lsi.append(item)
                    elif 'phrase' not in item and len(item) > 0:
                        # Словарь без ключа 'phrase' - пропускаем
                        continue
                elif isinstance(item, str):
                    # Строка - преобразуем в словарь
                    normalized_lsi.append({
                        'phrase': item,
                        'frequency': 1,
                        'source': 'unknown'
                    })
        
        # Нормализуем формат URL используя нормализатор
        normalized_urls = SERPDataNormalizer.normalize_serp_urls(top_urls)
        
        # Формируем результат в формате совместимом с остальным кодом
        return {
            'query': query,
            'source': 'master_db',
            'status': row[0],  # serp_status
            'req_id': row[1],  # serp_req_id
            'metrics': {
                'found_docs': row[2],
                'main_pages_count': row[3],
                'titles_with_keyword': row[4],
                'commercial_domains': row[5],
                'info_domains': row[6]
            },
            'documents': normalized_urls,  # TOP-20 URL (нормализованные)
            'lsi_phrases': normalized_lsi,
            'xml_response': None  # В Master DB не храним XML
        }

