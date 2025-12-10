"""
Поиск запросов без LSI фраз
"""

import sqlite3
import json
from typing import List, Tuple, Dict, Any
from datetime import datetime, timedelta

from .lsi_validator import LSIValidator


class LSIQueriesFinder:
    """Поиск запросов без LSI"""
    
    def __init__(self, db_path: str):
        """
        Args:
            db_path: Путь к Master DB
        """
        self.db_path = db_path
        self.validator = LSIValidator()
    
    def find_queries_without_lsi(self, group_name: str = None) -> Tuple[List[Tuple], Dict[str, int]]:
        """
        Найти запросы без LSI фраз
        
        Args:
            group_name: Название группы (если None, ищет во всех группах)
            
        Returns:
            Кортеж (список запросов для обработки, статистика)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if group_name:
            cursor.execute('''
                SELECT keyword, serp_top_urls, serp_req_id, group_name, serp_lsi_phrases, serp_updated_at
                FROM master_queries
                WHERE group_name = ?
                  AND serp_status = 'completed'
            ''', (group_name,))
        else:
            cursor.execute('''
                SELECT keyword, serp_top_urls, serp_req_id, group_name, serp_lsi_phrases, serp_updated_at
                FROM master_queries
                WHERE serp_status = 'completed'
            ''')
        
        all_queries = cursor.fetchall()
        conn.close()
        
        queries_to_process = []
        stats = {
            'with_urls_with_lsi': 0,
            'with_urls_no_lsi': 0,
            'no_urls_with_lsi': 0,
            'no_urls_no_lsi': 0
        }
        
        for row in all_queries:
            if len(row) == 6:
                keyword, top_urls_json, req_id, query_group, lsi_json, serp_updated_at = row
            else:
                # Старый формат без serp_updated_at
                keyword, top_urls_json, req_id, query_group, lsi_json = row[:5]
                serp_updated_at = None
            
            has_urls = self._has_urls(top_urls_json)
            has_lsi = self.validator.has_valid_lsi(lsi_json)
            
            if has_urls and has_lsi:
                stats['with_urls_with_lsi'] += 1
            elif has_urls and not has_lsi:
                stats['with_urls_no_lsi'] += 1
                queries_to_process.append((keyword, top_urls_json, req_id, query_group, serp_updated_at))
            elif not has_urls and has_lsi:
                stats['no_urls_with_lsi'] += 1
            else:
                stats['no_urls_no_lsi'] += 1
                queries_to_process.append((keyword, top_urls_json, req_id, query_group, serp_updated_at))
        
        return queries_to_process, stats
    
    def _has_urls(self, top_urls_json: Any) -> bool:
        """Проверяет наличие URL в данных"""
        if not top_urls_json:
            return False
        try:
            top_urls = json.loads(top_urls_json) if isinstance(top_urls_json, str) else top_urls_json
            if isinstance(top_urls, list) and len(top_urls) > 0:
                first_item = top_urls[0]
                if isinstance(first_item, str):
                    return True
                elif isinstance(first_item, dict):
                    if 'position' in first_item or 'url' in first_item:
                        return True
        except (json.JSONDecodeError, TypeError):
            pass
        return False
    
    def split_queries_by_processing_type(
        self, 
        queries_to_process: List[Tuple]
    ) -> Tuple[List[Tuple], List[Tuple]]:
        """
        Разделить запросы на те, что нужно получить через API и те, из которых можно извлечь LSI локально
        
        Args:
            queries_to_process: Список запросов для обработки (может включать serp_updated_at)
            
        Returns:
            Кортеж (запросы для API, запросы для локального извлечения)
        """
        queries_with_req_id = []
        queries_with_full_data = []
        queries_needing_new_request = []  # Запросы с устаревшим req_id
        
        # Время жизни req_id - 10 минут
        req_id_max_age = timedelta(minutes=10)
        now = datetime.now()
        
        expired_count = 0
        
        for row in queries_to_process:
            # Поддерживаем старый и новый формат
            if len(row) == 5:
                keyword, top_urls_json, req_id, query_group, serp_updated_at = row
            else:
                keyword, top_urls_json, req_id, query_group = row[:4]
                serp_updated_at = None
            if isinstance(top_urls_json, str):
                top_urls = json.loads(top_urls_json) if top_urls_json.strip() else []
            else:
                top_urls = top_urls_json if top_urls_json else []
            
            # Проверяем наличие URL данных
            has_urls = len(top_urls) > 0
            
            # Определяем, нужны ли данные через API
            needs_api = False
            if not has_urls:
                # Нет URL данных - нужен новый запрос (не пытаемся получить по req_id)
                needs_api = True
            elif len(top_urls) > 0:
                if isinstance(top_urls[0], str):
                    # Только строки URL без данных - нужен API
                    needs_api = True
                elif isinstance(top_urls[0], dict):
                    # Проверяем полноту данных
                    has_complete = 'snippet' in top_urls[0] and 'passages' in top_urls[0]
                    if not has_complete:
                        needs_api = True
            
            # Проверяем req_id более явно (может быть None, пустая строка или строка с пробелами)
            has_valid_req_id = req_id and isinstance(req_id, str) and req_id.strip()
            
            # Проверяем, не устарел ли req_id (больше 10 минут)
            req_id_expired = False
            if has_valid_req_id and serp_updated_at:
                try:
                    if isinstance(serp_updated_at, str):
                        updated_time = datetime.fromisoformat(serp_updated_at.replace('Z', '+00:00'))
                    else:
                        updated_time = serp_updated_at
                    
                    age = now - updated_time.replace(tzinfo=None) if updated_time.tzinfo else now - updated_time
                    if age > req_id_max_age:
                        req_id_expired = True
                        expired_count += 1
                except (ValueError, TypeError, AttributeError):
                    # Если не удалось распарсить время, считаем req_id валидным
                    pass
            
            if needs_api:
                # Если нет URL данных - сразу нужен новый запрос (не пытаемся получить по req_id)
                if not has_urls:
                    queries_needing_new_request.append((keyword, top_urls_json, None, query_group))
                # Если есть URL, но неполные данные - можно попробовать получить по req_id
                elif has_valid_req_id and not req_id_expired:
                    # Есть валидный и не устаревший req_id - получаем данные через API
                    queries_with_req_id.append((keyword, top_urls_json, req_id, query_group))
                else:
                    # Нет req_id или он устарел - нужен новый запрос
                    queries_needing_new_request.append((keyword, top_urls_json, None, query_group))
            else:
                # Есть полные данные - можно извлечь LSI локально
                queries_with_full_data.append((keyword, top_urls_json, req_id, query_group))
        
        # Запросы без URL или с устаревшим req_id добавляем в queries_with_full_data с req_id = None
        # чтобы recovery_handler мог их обработать отдельно (сбросить и сделать новый запрос)
        no_url_count = sum(1 for q in queries_needing_new_request if not self._has_urls(q[1]))
        if expired_count > 0 or no_url_count > 0:
            if expired_count > 0:
                print(f"   ⏰ Найдено {expired_count} запросов с устаревшим req_id (>10 минут)")
            if no_url_count > 0:
                print(f"   📭 Найдено {no_url_count} запросов без URL данных - нужен новый запрос (не используем req_id)")
        
        return queries_with_req_id, queries_with_full_data + queries_needing_new_request

