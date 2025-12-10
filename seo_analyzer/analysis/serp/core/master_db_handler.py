"""
Работа с Master DB для SERP анализатора
"""

import sqlite3
import json
from typing import Optional, Dict, Any

from .result_formatter import ResultFormatter


class MasterDBHandler:
    """Обработчик работы с Master DB"""
    
    def __init__(self, master_db, query_group: str, lr: int):
        """
        Args:
            master_db: Экземпляр MasterQueryDatabase
            query_group: Название группы запросов
            lr: Регион поиска
        """
        self.master_db = master_db
        self.query_group = query_group
        self.formatter = ResultFormatter(lr)
    
    def get_from_master_db(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Получить данные из Master DB
        Сначала проверяем текущую группу, затем ищем по всем группам
        (проверяем статус completed и наличие данных)
        
        ВАЖНО: Если статус completed, но данных нет (found_docs = 0 или NULL, нет URL) -
        возвращаем None чтобы загрузить заново
        
        Args:
            query: Запрос
            
        Returns:
            Результат из БД или None
        """
        if not self.master_db:
            return None
        
        try:
            conn = sqlite3.connect(self.master_db.db_path)
            cursor = conn.cursor()
            
            # Сначала проверяем текущую группу (если указана)
            if self.query_group:
                cursor.execute('''
                    SELECT 
                        serp_status,
                        serp_req_id,
                        serp_found_docs,
                        serp_main_pages_count,
                        serp_titles_with_keyword,
                        serp_commercial_domains,
                        serp_info_domains,
                        serp_top_urls,
                        serp_lsi_phrases
                    FROM master_queries
                    WHERE group_name = ? AND keyword = ?
                      AND serp_status = 'completed'
                ''', (self.query_group, query))
                
                row = cursor.fetchone()
                if row:
                    # Проверяем что данные действительно есть
                    found_docs = row[2]  # serp_found_docs
                    top_urls_json = row[7]  # serp_top_urls
                    
                    # Если нет документов - данные неполные, нужно загрузить заново
                    if found_docs is None or found_docs == 0:
                        conn.close()
                        return None  # Данных нет - загрузим заново
                    
                    # Проверяем наличие top_urls_json (может быть NULL или пустой строкой)
                    if top_urls_json:
                        if isinstance(top_urls_json, str):
                            if top_urls_json.strip() == '':
                                top_urls_json = '[]'
                    else:
                        top_urls_json = '[]'
                    
                    # Парсим JSON (даже если это пустой массив)
                    try:
                        top_urls = json.loads(top_urls_json) if isinstance(top_urls_json, str) else top_urls_json
                    except (json.JSONDecodeError, TypeError):
                        top_urls = []
                    
                    conn.close()
                    return self.formatter.format_serp_result(query, row)
            
            # Если не найдено в текущей группе - ищем по всем группам
            cursor.execute('''
                SELECT 
                    serp_status,
                    serp_req_id,
                    serp_found_docs,
                    serp_main_pages_count,
                    serp_titles_with_keyword,
                    serp_commercial_domains,
                    serp_info_domains,
                    serp_top_urls,
                    serp_lsi_phrases
                FROM master_queries
                WHERE keyword = ?
                  AND serp_status = 'completed'
                LIMIT 1
            ''', (query,))
            
            row = cursor.fetchone()
            
            if row:
                # Проверяем что данные действительно есть
                found_docs = row[2]  # serp_found_docs
                top_urls_json = row[7]  # serp_top_urls
                
                # Если нет документов - данные неполные, нужно загрузить заново
                if found_docs is None or found_docs == 0:
                    conn.close()
                    return None  # Данных нет - загрузим заново
                
                # Проверяем наличие top_urls_json (может быть NULL или пустой строкой)
                if top_urls_json:
                    if isinstance(top_urls_json, str):
                        if top_urls_json.strip() == '':
                            top_urls_json = '[]'
                else:
                    top_urls_json = '[]'
                
                # Парсим JSON (даже если это пустой массив)
                try:
                    top_urls = json.loads(top_urls_json) if isinstance(top_urls_json, str) else top_urls_json
                except (json.JSONDecodeError, TypeError):
                    top_urls = []
                
                conn.close()
                # Найдено в другой группе - используем данные
                return self.formatter.format_serp_result(query, row)
            
            conn.close()
            return None
        
        except Exception as e:
            # Ошибка чтения - игнорируем, загрузим из API
            return None
    
    def update_master_status(
        self,
        query: str,
        status: str,
        req_id: str = None,
        error_message: str = None
    ):
        """
        Обновить статус SERP запроса в Master DB
        
        Args:
            query: Запрос
            status: Статус (pending/processing/completed/error)
            req_id: ID запроса в xmlstock
            error_message: Текст ошибки
        """
        if not self.master_db or not self.query_group:
            return
        
        try:
            self.master_db.update_serp_status(
                group_name=self.query_group,
                keyword=query,
                status=status,
                req_id=req_id,
                error_message=error_message
            )
        except Exception as e:
            # Не критично если не обновится
            pass

