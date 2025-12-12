"""
Работа с Master DB для SERP анализатора
"""

import sqlite3
import json
from typing import Optional, Dict, Any

from .result_formatter import ResultFormatter
from seo_analyzer.core.cache.db.initializer import apply_sqlite_optimizations


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
            
            # Применяем оптимизации для ускорения чтения
            apply_sqlite_optimizations(cursor)
            
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
                    
                    # ВАЖНО: Проверяем наличие serp_top_urls - если пустой, собираем заново
                    has_urls = False
                    if top_urls_json:
                        if isinstance(top_urls_json, str):
                            top_urls_str = top_urls_json.strip()
                            if top_urls_str and top_urls_str not in ('', '[]', 'null', 'NULL', 'None'):
                                # Парсим JSON чтобы проверить что там есть данные
                                try:
                                    top_urls = json.loads(top_urls_str)
                                    if isinstance(top_urls, list) and len(top_urls) > 0:
                                        has_urls = True
                                except (json.JSONDecodeError, TypeError):
                                    # Не JSON или ошибка парсинга - считаем что данных нет
                                    has_urls = False
                    else:
                        # NULL или пустое значение
                        has_urls = False
                    
                    # Если нет URL - данные неполные, нужно загрузить заново через API
                    if not has_urls:
                        conn.close()
                        return None  # serp_top_urls пустой - загрузим заново через XMLStock
                    
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
                
                # ВАЖНО: Проверяем наличие serp_top_urls - если пустой, собираем заново
                has_urls = False
                if top_urls_json:
                    if isinstance(top_urls_json, str):
                        top_urls_str = top_urls_json.strip()
                        if top_urls_str and top_urls_str not in ('', '[]', 'null', 'NULL', 'None'):
                            # Парсим JSON чтобы проверить что там есть данные
                            try:
                                top_urls = json.loads(top_urls_str)
                                if isinstance(top_urls, list) and len(top_urls) > 0:
                                    has_urls = True
                            except (json.JSONDecodeError, TypeError):
                                # Не JSON или ошибка парсинга - считаем что данных нет
                                has_urls = False
                else:
                    # NULL или пустое значение
                    has_urls = False
                
                # Если нет URL - данные неполные, нужно загрузить заново через API
                if not has_urls:
                    conn.close()
                    return None  # serp_top_urls пустой - загрузим заново через XMLStock
                
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
        error_message: str = None,
        group_name: str = None
    ):
        """
        Обновить статус SERP запроса в Master DB
        
        Args:
            query: Запрос
            status: Статус (pending/processing/completed/error)
            req_id: ID запроса в xmlstock
            error_message: Текст ошибки
            group_name: Название группы (если None, используется self.query_group)
        """
        # Используем переданный group_name или self.query_group
        target_group = group_name or self.query_group
        
        if not self.master_db or not target_group:
            return
        
        try:
            self.master_db.update_serp_status(
                group_name=target_group,
                keyword=query,
                status=status,
                req_id=req_id,
                error_message=error_message
            )
        except Exception as e:
            # Не критично если не обновится
            pass
    
    def batch_get_from_master_db(self, queries: list, max_batch_size: int = 1000) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Батчевая загрузка данных из Master DB для множества запросов
        Намного быстрее чем проверять каждый запрос отдельно
        
        Если запросов больше max_batch_size, разбивает на несколько батчей
        (SQLite имеет ограничение на количество параметров в IN запросе)
        
        Args:
            queries: Список запросов
            max_batch_size: Максимальный размер батча (по умолчанию 1000)
            
        Returns:
            Словарь {query: result или None}
        """
        result = {}
        if not self.master_db or not queries:
            return {q: None for q in queries}
        
        # Если запросов больше max_batch_size, разбиваем на батчи
        if len(queries) > max_batch_size:
            for i in range(0, len(queries), max_batch_size):
                batch_queries = queries[i:i + max_batch_size]
                batch_results = self._batch_get_from_master_db_single(batch_queries)
                result.update(batch_results)
            return result
        
        return self._batch_get_from_master_db_single(queries)
    
    def _batch_get_from_master_db_single(self, queries: list) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Батчевая загрузка для одного батча запросов (внутренний метод)
        
        Args:
            queries: Список запросов (не более max_batch_size)
            
        Returns:
            Словарь {query: result или None}
        """
        result = {}
        if not self.master_db or not queries:
            return {q: None for q in queries}
        
        try:
            conn = sqlite3.connect(self.master_db.db_path)
            cursor = conn.cursor()
            
            # Применяем оптимизации для ускорения чтения
            apply_sqlite_optimizations(cursor)
            
            # Создаем плейсхолдеры для IN запроса
            placeholders = ','.join(['?'] * len(queries))
            
            # УБРАНО ОГРАНИЧЕНИЕ: Ищем по ВСЕМ группам сразу, а не только по текущей
            # Это позволяет использовать кэш из других групп при объединенной обработке
            cursor.execute(f'''
                SELECT 
                    keyword,
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
                WHERE keyword IN ({placeholders})
                  AND serp_status = 'completed'
                ORDER BY keyword, group_name
            ''', queries)
            
            rows = cursor.fetchall()
            
            # Используем словарь чтобы брать только первую запись для каждого keyword
            seen_keywords = set()
            for row in rows:
                keyword = row[0]
                # Пропускаем если уже обработали этот keyword
                if keyword in seen_keywords:
                    continue
                seen_keywords.add(keyword)
                
                found_docs = row[3]  # serp_found_docs (row[0]=keyword, row[1]=status, row[2]=req_id, row[3]=found_docs)
                top_urls_json = row[8]  # serp_top_urls (row[8]=serp_top_urls)
                
                # Проверяем что данные действительно есть
                has_valid_data = False
                if found_docs is not None and found_docs > 0:
                    # Проверяем наличие serp_top_urls
                    if top_urls_json:
                        if isinstance(top_urls_json, str):
                            top_urls_str = top_urls_json.strip()
                            if top_urls_str and top_urls_str not in ('', '[]', 'null', 'NULL', 'None'):
                                try:
                                    top_urls = json.loads(top_urls_str)
                                    if isinstance(top_urls, list) and len(top_urls) > 0:
                                        has_valid_data = True
                                except (json.JSONDecodeError, TypeError):
                                    pass
                
                if has_valid_data:
                    # Форматируем результат (передаем row[1:] чтобы исключить keyword)
                    # row[1:] содержит: serp_status, serp_req_id, serp_found_docs, ...
                    result[keyword] = self.formatter.format_serp_result(keyword, row[1:])
                else:
                    result[keyword] = None
            
            # УБРАНО: Дублирующий код поиска в других группах - уже ищем по всем группам выше
            
            conn.close()
            
            # Заполняем None для запросов, которые не найдены
            for query in queries:
                if query not in result:
                    result[query] = None
            
            return result
        
        except Exception as e:
            # Ошибка чтения - возвращаем None для всех
            return {q: None for q in queries}

