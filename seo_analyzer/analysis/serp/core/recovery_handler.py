"""
Восстановление незавершённых запросов и LSI фраз
Фасад для модулей восстановления
"""

from typing import Optional

from .master_db_handler import MasterDBHandler
from .recovery.pending_queries_finder import PendingQueriesFinder
from .recovery.pending_queries_recoverer import PendingQueriesRecoverer
from .recovery.lsi_queries_finder import LSIQueriesFinder
from .recovery.lsi_api_fetcher import LSIApiFetcher
from .recovery.lsi_local_extractor import LSILocalExtractor


class RecoveryHandler:
    """Обработчик восстановления незавершённых запросов"""
    
    def __init__(
        self,
        api_key: str,
        lr: int,
        master_db_handler: MasterDBHandler,
        query_group: str
    ):
        """
        Args:
            api_key: API ключ
            lr: Регион поиска
            master_db_handler: Обработчик Master DB
            query_group: Название группы запросов
        """
        self.api_key = api_key
        self.lr = lr
        self.master_db_handler = master_db_handler
        self.query_group = query_group
    
    async def recover_pending_requests(self) -> int:
        """
        Автоматическое восстановление незавершённых запросов из всех групп
        
        Returns:
            Количество восстановленных запросов
        """
        if not self.master_db_handler.master_db:
            return 0
        
        print(f"\n{'='*80}")
        print(f"🔄 Автоматическое восстановление незавершённых запросов")
        print(f"{'='*80}")
        
        # Находим незавершённые запросы
        finder = PendingQueriesFinder()
        all_pending = finder.find_pending_queries(self.master_db_handler.master_db.db_path)
        
        if not all_pending:
            print("✓ Нет незавершённых запросов для восстановления")
            return 0
        
        print(f"📦 Найдено групп с незавершёнными запросами: {len(set(q['group'] for q in all_pending))}")
        print(f"📋 Найдено незавершённых запросов: {len(all_pending)}")
        
        # Восстанавливаем запросы
        recoverer = PendingQueriesRecoverer(
            api_key=self.api_key,
            lr=self.lr,
            master_db_handler=self.master_db_handler
        )
        
        recovered_count = await recoverer.recover(all_pending)
        
        print(f"\n✓ Восстановлено запросов по req_id: {recovered_count}")
        print(f"{'='*80}\n")
        
        return recovered_count
    
    async def recover_missing_lsi_from_urls(self, group_name: str = None) -> int:
        """
        Дособрать LSI фразы для запросов, у которых есть URL, но нет LSI
        
        Args:
            group_name: Название группы (если None, используется self.query_group)
            
        Returns:
            Количество обновленных запросов
        """
        if not self.master_db_handler.master_db:
            print("⚠️  Master DB не настроен")
            return 0
        
        group = group_name or self.query_group
        
        # Находим запросы без LSI
        finder = LSIQueriesFinder(self.master_db_handler.master_db.db_path)
        queries_to_process, stats = finder.find_queries_without_lsi(group)
        
        # Диагностика
        total = sum(stats.values())
        if total > 0:
            print(f"   📊 Статистика по запросам:")
            print(f"      С URL и LSI: {stats['with_urls_with_lsi']} ({stats['with_urls_with_lsi']/total*100:.1f}%)")
            print(f"      С URL без LSI: {stats['with_urls_no_lsi']} ({stats['with_urls_no_lsi']/total*100:.1f}%)")
            print(f"      Без URL с LSI: {stats['no_urls_with_lsi']} ({stats['no_urls_with_lsi']/total*100:.1f}%)")
            print(f"      Без URL без LSI: {stats['no_urls_no_lsi']} ({stats['no_urls_no_lsi']/total*100:.1f}%)")
        
        if not queries_to_process:
            print("✓ Нет запросов для обработки")
            return 0
        
        print(f"   Найдено {len(queries_to_process)} запросов без LSI")
        
        # Разделяем запросы на те, что нужны через API и те, что можно обработать локально
        queries_with_req_id, queries_with_full_data = finder.split_queries_by_processing_type(queries_to_process)
        
        # Отладочная информация
        if queries_to_process:
            sample_req_id = queries_to_process[0][2] if len(queries_to_process[0]) > 2 else None
            print(f"   🔍 Отладка: пример req_id из первого запроса: '{sample_req_id}' (тип: {type(sample_req_id)})")
            print(f"   🔍 Отладка: запросов для API: {len(queries_with_req_id)}, для локальной обработки: {len(queries_with_full_data)}")
        
        updated_count = 0
        
        # Обрабатываем запросы через API (по существующим req_id)
        # Только для запросов, у которых ЕСТЬ URL данные, но нет LSI
        if queries_with_req_id:
            print(f"   📤 Получение данных через API для {len(queries_with_req_id)} запросов (по req_id)...")
            api_fetcher = LSIApiFetcher(
                api_key=self.api_key,
                lr=self.lr,
                db_path=self.master_db_handler.master_db.db_path
            )
            updated_count += await api_fetcher.fetch_lsi_for_queries(queries_with_req_id)
        else:
            print(f"   ⚠️  Нет запросов с валидным req_id для получения через API")
        
        # Обрабатываем запросы, которым нужен новый запрос (нет URL или устаревший req_id)
        queries_needing_new_request = [
            q for q in queries_with_full_data 
            if q[2] is None or (isinstance(q[2], str) and not q[2].strip())
        ]
        
        if queries_needing_new_request:
            print(f"   🔄 Найдено {len(queries_needing_new_request)} запросов без URL данных или с устаревшим req_id")
            print(f"   💡 Сбрасываем req_id - эти запросы будут обработаны при следующем запуске SERP анализа")
            # Сбрасываем req_id в базе данных для новых запросов
            import sqlite3
            conn = sqlite3.connect(self.master_db_handler.master_db.db_path)
            cursor = conn.cursor()
            reset_count = 0
            for keyword, _, req_id, query_group in queries_needing_new_request:
                cursor.execute('''
                    UPDATE master_queries
                    SET serp_req_id = NULL, serp_status = 'pending'
                    WHERE group_name = ? AND keyword = ?
                ''', (query_group or group, keyword))
                reset_count += cursor.rowcount
            conn.commit()
            conn.close()
            if reset_count > 0:
                print(f"   ✓ Сброшено {reset_count} req_id для повторного запроса")
        
        # Обрабатываем запросы локально
        if queries_with_full_data:
            print(f"   📝 Извлечение LSI из имеющихся данных для {len(queries_with_full_data)} запросов...")
            local_extractor = LSILocalExtractor(self.master_db_handler.master_db.db_path)
            updated_count += local_extractor.extract_lsi_for_queries(queries_with_full_data)
        
        print(f"✓ Дособор LSI завершен: обновлено {updated_count} запросов")
        return updated_count
