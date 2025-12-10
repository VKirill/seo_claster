"""
Обновление интента запросов в Master DB
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List


class IntentUpdater:
    """Обновление интента запросов в Master DB"""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к базе данных
        """
        self.db_path = db_path
    
    def update_intent(
        self,
        group_name: str,
        keyword: str,
        main_intent: str,
        commercial_score: float = None,
        informational_score: float = None
    ) -> bool:
        """
        Обновляет интент для конкретного запроса
        
        Args:
            group_name: Название группы
            keyword: Ключевое слово
            main_intent: Новый основной интент
            commercial_score: Коммерческий скор (опционально)
            informational_score: Информационный скор (опционально)
            
        Returns:
            True если обновление успешно, False иначе
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # ВАЖНО: Сначала создаём запись если её нет (защита от потери данных)
            cursor.execute('''
                INSERT OR IGNORE INTO master_queries (group_name, keyword, main_intent)
                VALUES (?, ?, ?)
            ''', (group_name, keyword, main_intent))
            
            # Обновляем интент
            update_parts = ['main_intent = ?']
            params = [main_intent]
            
            if commercial_score is not None:
                update_parts.append('commercial_score = ?')
                params.append(commercial_score)
            
            if informational_score is not None:
                update_parts.append('informational_score = ?')
                params.append(informational_score)
            
            # Добавляем WHERE условия
            params.extend([group_name, keyword])
            
            query = f'''
                UPDATE master_queries
                SET {', '.join(update_parts)}
                WHERE group_name = ? AND keyword = ?
            '''
            
            cursor.execute(query, params)
            conn.commit()
            
            updated = cursor.rowcount > 0
            conn.close()
            
            return updated
            
        except Exception as e:
            print(f"⚠️  Ошибка обновления интента для '{keyword}': {e}")
            return False
    
    def update_intents_batch(
        self,
        group_name: str,
        updates: List[Dict]
    ) -> int:
        """
        Пакетное обновление интентов
        
        Args:
            group_name: Название группы
            updates: Список словарей с обновлениями:
                [
                    {
                        'keyword': str,
                        'main_intent': str,
                        'commercial_score': float (опционально),
                        'informational_score': float (опционально)
                    },
                    ...
                ]
        
        Returns:
            Количество обновленных записей
        """
        if not updates:
            return 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            updated_count = 0
            
            for update in updates:
                keyword = update.get('keyword')
                main_intent = update.get('main_intent')
                
                if not keyword or not main_intent:
                    continue
                
                # ВАЖНО: Сначала создаём запись если её нет (защита от потери данных)
                cursor.execute('''
                    INSERT OR IGNORE INTO master_queries (group_name, keyword, main_intent)
                    VALUES (?, ?, ?)
                ''', (group_name, keyword, main_intent))
                
                # Собираем параметры для обновления
                update_parts = ['main_intent = ?']
                params = [main_intent]
                
                if 'commercial_score' in update:
                    update_parts.append('commercial_score = ?')
                    params.append(update['commercial_score'])
                
                if 'informational_score' in update:
                    update_parts.append('informational_score = ?')
                    params.append(update['informational_score'])
                
                # Добавляем WHERE условия
                params.extend([group_name, keyword])
                
                query = f'''
                    UPDATE master_queries
                    SET {', '.join(update_parts)}
                    WHERE group_name = ? AND keyword = ?
                '''
                
                cursor.execute(query, params)
                updated_count += cursor.rowcount
            
            conn.commit()
            conn.close()
            
            return updated_count
            
        except Exception as e:
            print(f"⚠️  Ошибка пакетного обновления интентов: {e}")
            return 0
    
    def update_intents_from_dataframe(
        self,
        group_name: str,
        df: pd.DataFrame
    ) -> int:
        """
        Обновляет интенты из DataFrame
        
        Args:
            group_name: Название группы
            df: DataFrame с колонками keyword, main_intent, commercial_score, informational_score
            
        Returns:
            Количество обновленных записей
        """
        if 'keyword' not in df.columns or 'main_intent' not in df.columns:
            return 0
        
        updates = []
        
        for _, row in df.iterrows():
            update = {
                'keyword': row['keyword'],
                'main_intent': row['main_intent']
            }
            
            if 'commercial_score' in df.columns and pd.notna(row.get('commercial_score')):
                update['commercial_score'] = float(row['commercial_score'])
            
            if 'informational_score' in df.columns and pd.notna(row.get('informational_score')):
                update['informational_score'] = float(row['informational_score'])
            
            updates.append(update)
        
        return self.update_intents_batch(group_name, updates)


__all__ = ['IntentUpdater']


