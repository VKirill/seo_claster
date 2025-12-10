"""
Управление статусами SERP запросов
"""

import sqlite3
import json
from pathlib import Path
from typing import List, Dict, Any


class SERPStatusManager:
    """Менеджер статусов SERP запросов"""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
    
    def update_serp_status(
        self,
        group_name: str,
        keyword: str,
        status: str,
        req_id: str = None,
        error_message: str = None
    ):
        """
        Обновить статус SERP запроса
        
        Args:
            group_name: Название группы
            keyword: Запрос
            status: Статус (pending/processing/completed/error)
            req_id: ID запроса в xmlstock
            error_message: Текст ошибки
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # ВАЖНО: Сначала создаём запись если её нет (защита от потери данных)
        cursor.execute('''
            INSERT OR IGNORE INTO master_queries (group_name, keyword, serp_status, serp_created_at)
            VALUES (?, ?, 'pending', CURRENT_TIMESTAMP)
        ''', (group_name, keyword))
        
        # Теперь UPDATE точно найдёт запись
        cursor.execute('''
            UPDATE master_queries
            SET serp_status = ?,
                serp_req_id = COALESCE(?, serp_req_id),
                serp_error_message = ?,
                serp_updated_at = CURRENT_TIMESTAMP
            WHERE group_name = ? AND keyword = ?
        ''', (status, req_id, error_message, group_name, keyword))
        
        conn.commit()
        conn.close()
    
    def mark_serp_as_pending(self, group_name: str, keywords: List[str]):
        """
        Отметить запросы как ожидающие загрузки SERP
        
        Args:
            group_name: Название группы
            keywords: Список запросов
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # ВАЖНО: Сначала создаём записи если их нет (защита от потери данных)
        cursor.executemany('''
            INSERT OR IGNORE INTO master_queries (group_name, keyword, serp_status, serp_created_at)
            VALUES (?, ?, 'pending', CURRENT_TIMESTAMP)
        ''', [(group_name, kw) for kw in keywords])
        
        # Теперь UPDATE точно найдёт записи
        cursor.executemany('''
            UPDATE master_queries
            SET serp_status = 'pending',
                serp_updated_at = CURRENT_TIMESTAMP
            WHERE group_name = ? AND keyword = ?
        ''', [(group_name, kw) for kw in keywords])
        
        conn.commit()
        conn.close()
        
        print(f"✓ Отмечено {len(keywords)} запросов как pending для SERP загрузки")
    
    def update_serp_metrics(
        self,
        group_name: str,
        keyword: str,
        metrics: Dict[str, Any],
        documents: List[Dict],
        lsi_phrases: List[Dict]
    ):
        """
        Обновить SERP метрики для конкретного запроса и пересчитать зависимые метрики
        
        Args:
            group_name: Название группы
            keyword: Ключевое слово
            metrics: Словарь с метриками из SERPDataEnricher
            documents: Список документов (для извлечения TOP-20)
            lsi_phrases: Список LSI фраз
        """
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # ВАЖНО: Сначала создаём запись если её нет (защита от потери данных)
            cursor.execute('''
                INSERT OR IGNORE INTO master_queries (group_name, keyword, serp_status, serp_created_at)
                VALUES (?, ?, 'processing', CURRENT_TIMESTAMP)
            ''', (group_name, keyword))
            
            # Формируем TOP-20 URLs для кластеризации
            # ВАЖНО: Сохраняем полный формат с snippet и passages для возможности извлечения LSI
            top_urls = []
            for i, doc in enumerate(documents[:20], 1):
                top_urls.append({
                    'position': i,
                    'url': doc.get('url', ''),
                    'domain': doc.get('domain', ''),
                    'title': doc.get('title', ''),
                    'snippet': doc.get('snippet', ''),
                    'passages': doc.get('passages', ''),
                    'is_commercial': doc.get('is_commercial', False),
                    'offer': doc.get('offer', [])  # Добавляем массив offer_info
                })
            
            # LSI фразы как JSON
            lsi_json = json.dumps(lsi_phrases, ensure_ascii=False) if lsi_phrases else None
            top_urls_json = json.dumps(top_urls, ensure_ascii=False) if top_urls else None
            
            # Получаем текущие значения для пересчета intent scores
            cursor.execute('''
                SELECT main_intent, commercial_score, informational_score, navigational_score
                FROM master_queries
                WHERE group_name = ? AND keyword = ?
            ''', (group_name, keyword))
            
            current_row = cursor.fetchone()
            current_intent = current_row[0] if current_row else None
            current_commercial_score = current_row[1] if current_row else 0.0
            current_informational_score = current_row[2] if current_row else 0.0
            current_navigational_score = current_row[3] if current_row else 0.0
            
            # Обновляем SERP метрики
            cursor.execute('''
                UPDATE master_queries
                SET
                    serp_found_docs = ?,
                    serp_main_pages_count = ?,
                    serp_titles_with_keyword = ?,
                    serp_commercial_domains = ?,
                    serp_info_domains = ?,
                    serp_intent = ?,
                    serp_confidence = ?,
                    serp_docs_with_offers = ?,
                    serp_total_docs = ?,
                    serp_offer_ratio = ?,
                    serp_avg_price = ?,
                    serp_min_price = ?,
                    serp_max_price = ?,
                    serp_median_price = ?,
                    serp_currency = ?,
                    serp_offers_count = ?,
                    serp_offers_with_discount = ?,
                    serp_avg_discount_percent = ?,
                    serp_top_urls = ?,
                    serp_lsi_phrases = ?,
                    serp_status = 'completed',
                    serp_updated_at = CURRENT_TIMESTAMP
                WHERE group_name = ? AND keyword = ?
            ''', (
                metrics.get('found_docs'),
                metrics.get('main_pages_count'),
                metrics.get('titles_with_keyword'),
                metrics.get('commercial_domains_count') or metrics.get('commercial_domains', 0),
                metrics.get('informational_domains_count') or metrics.get('info_domains', 0),
                metrics.get('serp_intent'),
                metrics.get('serp_confidence'),
                metrics.get('docs_with_offers'),
                metrics.get('total_docs_analyzed'),
                metrics.get('offer_ratio'),
                metrics.get('avg_price'),
                metrics.get('min_price'),
                metrics.get('max_price'),
                metrics.get('median_price'),
                metrics.get('currency', 'RUR'),
                metrics.get('offers_count'),
                metrics.get('offers_with_discount'),
                metrics.get('avg_discount_percent'),
                top_urls_json,
                lsi_json,
                group_name,
                keyword
            ))
            
            # Пересчитываем intent scores на основе новых SERP данных
            commercial_domains = metrics.get('commercial_domains_count') or metrics.get('commercial_domains', 0)
            info_domains = metrics.get('informational_domains_count') or metrics.get('info_domains', 0)
            self._recalculate_intent_scores_by_serp(
                cursor, group_name, keyword,
                commercial_domains,
                info_domains,
                metrics.get('serp_intent'),
                current_intent,
                current_commercial_score,
                current_informational_score,
                current_navigational_score
            )
            
            conn.commit()
        finally:
            conn.close()
    
    def _recalculate_intent_scores_by_serp(
        self,
        cursor,
        group_name: str,
        keyword: str,
        commercial_domains: int,
        info_domains: int,
        serp_intent: str = None,
        current_intent: str = None,
        current_commercial_score: float = 0.0,
        current_informational_score: float = 0.0,
        current_navigational_score: float = 0.0
    ):
        """
        Пересчитать intent scores на основе SERP данных
        
        Args:
            cursor: Курсор базы данных
            group_name: Название группы
            keyword: Ключевое слово
            commercial_domains: Количество коммерческих доменов
            info_domains: Количество информационных доменов
            serp_intent: Интент из SERP offer_info (приоритетный)
            current_intent: Текущий интент
            current_commercial_score: Текущий commercial_score
            current_informational_score: Текущий informational_score
            current_navigational_score: Текущий navigational_score
        """
        # Вычисляем соотношение доменов
        total_domains = commercial_domains + info_domains
        
        if total_domains == 0:
            # Нет SERP данных - не меняем scores
            return
        
        commercial_ratio = commercial_domains / total_domains
        info_ratio = info_domains / total_domains
        
        # Инициализируем переменные
        new_intent = current_intent
        new_commercial_score = current_commercial_score
        new_informational_score = current_informational_score
        new_navigational_score = current_navigational_score
        
        # Приоритет 1: SERP intent из offer_info (если есть)
        if serp_intent and serp_intent in ['commercial', 'informational']:
            new_intent = serp_intent
            if serp_intent == 'commercial':
                new_commercial_score = max(current_commercial_score, commercial_ratio * 10.0)
                new_informational_score = current_informational_score * 0.5  # Снижаем если был коммерческий
            else:  # informational
                new_informational_score = max(current_informational_score, info_ratio * 10.0)
                new_commercial_score = current_commercial_score * 0.5  # Снижаем если был информационный
        
        # Приоритет 2: Корректировка на основе соотношения доменов
        elif current_intent:
            new_intent = current_intent
            commercial_threshold = 0.6
            
            # Корректировка informational → commercial
            if current_intent == 'informational' and commercial_ratio >= commercial_threshold:
                new_intent = 'commercial'
                new_commercial_score = commercial_ratio * 10.0
                new_informational_score = current_informational_score * 0.5
            
            # Корректировка commercial → informational
            elif current_intent == 'commercial' and info_ratio >= commercial_threshold:
                new_intent = 'informational'
                new_informational_score = info_ratio * 10.0
                new_commercial_score = current_commercial_score * 0.5
            
            # Обновление scores для commercial_geo
            elif current_intent == 'commercial_geo':
                new_commercial_score = max(current_commercial_score, commercial_ratio * 10.0)
                new_informational_score = current_informational_score
            
            # Обновление scores для informational_geo
            elif current_intent == 'informational_geo':
                new_informational_score = max(current_informational_score, info_ratio * 10.0)
                new_commercial_score = current_commercial_score
            
            # Для других интентов обновляем scores пропорционально
            else:
                new_commercial_score = max(current_commercial_score, commercial_ratio * 10.0)
                new_informational_score = max(current_informational_score, info_ratio * 10.0)
            
            new_navigational_score = current_navigational_score
        
        # Если нет текущего интента - создаем на основе SERP данных
        else:
            if commercial_ratio >= 0.6:
                new_intent = 'commercial'
                new_commercial_score = commercial_ratio * 10.0
                new_informational_score = info_ratio * 10.0
            elif info_ratio >= 0.6:
                new_intent = 'informational'
                new_informational_score = info_ratio * 10.0
                new_commercial_score = commercial_ratio * 10.0
            else:
                # Сохраняем текущий интент или используем commercial по умолчанию
                new_intent = current_intent or 'commercial'
                new_commercial_score = commercial_ratio * 10.0
                new_informational_score = info_ratio * 10.0
            
            new_navigational_score = current_navigational_score
        
        # Обновляем intent scores в БД
        cursor.execute('''
            UPDATE master_queries
            SET
                main_intent = COALESCE(?, main_intent),
                commercial_score = ?,
                informational_score = ?,
                navigational_score = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE group_name = ? AND keyword = ?
        ''', (
            new_intent,
            new_commercial_score,
            new_informational_score,
            new_navigational_score,
            group_name,
            keyword
        ))

