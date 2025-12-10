"""Основной класс дедупликатора"""

from typing import Dict, List, Set, Tuple
import pandas as pd
from pathlib import Path
from collections import defaultdict

from .text_processor import get_signature


class AdvancedDeduplicator:
    """
    Дедупликатор для поиска неявных дублей с настраиваемыми правилами
    
    Правила поиска дублей:
    1. Рассматриваем только уникальные слова (не их количество)
    2. Не учитываем спецсимволы (только буквы, цифры и пробелы)
    3. Используем список исключений (стоп-слова)
    
    Правила удаления дублей:
    - Если все фразы имеют значение: оставить все фразы в группе кроме одной (случайный выбор)
    - Если несколько фраз имеют макс. значение: оставить все фразы в группе кроме одной (случайный выбор)
    """
    
    def __init__(self, stopwords: Set[str] = None):
        """
        Инициализация дедупликатора
        
        Args:
            stopwords: Множество стоп-слов для исключения
        """
        self.stopwords = stopwords or set()
        self.duplicates_count = 0
        self.unique_count = 0
        self.duplicate_groups = []
    
    def find_implicit_duplicates(
        self,
        df: pd.DataFrame,
        keyword_column: str = 'keyword'
    ) -> Dict[str, List[Dict]]:
        """
        Находит неявные дубли в DataFrame
        
        Args:
            df: DataFrame с запросами
            keyword_column: Название колонки с запросами
            
        Returns:
            Словарь {сигнатура: [список запросов с метаданными]}
        """
        # Группируем по сигнатурам
        signature_groups = defaultdict(list)
        
        for idx, row in df.iterrows():
            query = row[keyword_column]
            signature = get_signature(query, self.stopwords)
            
            # Пропускаем пустые сигнатуры (только стоп-слова)
            if not signature:
                continue
            
            signature_groups[signature].append({
                'index': idx,
                'keyword': query,
                'frequency_world': row.get('frequency_world', 0),
                'frequency_exact': row.get('frequency_exact', 0),
            })
        
        # Оставляем только группы с дублями (2+ запроса)
        duplicates = {
            sig: queries 
            for sig, queries in signature_groups.items() 
            if len(queries) > 1
        }
        
        return duplicates
    
    def remove_duplicates(
        self,
        df: pd.DataFrame,
        keyword_column: str = 'keyword',
        freq_column: str = 'frequency_world',
        keep_strategy: str = 'max_freq_random'
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Удаляет дубли из DataFrame
        
        Args:
            df: DataFrame с запросами
            keyword_column: Колонка с запросами
            freq_column: Колонка с частотностью
            keep_strategy: Стратегия выбора:
                - 'max_freq_random': Если несколько фраз имеют макс. значение, 
                                    оставить все фразы в группе кроме одной (случайный выбор)
                - 'all_same_random': Если все фразы имеют одинаковое значение,
                                    оставить все фразы в группе кроме одной (случайный выбор)
            
        Returns:
            (DataFrame без дублей, DataFrame с удаленными дублями)
        """
        # Находим дубли
        duplicate_groups = self.find_implicit_duplicates(df, keyword_column)
        
        if not duplicate_groups:
            self.unique_count = len(df)
            return df, pd.DataFrame()
        
        # Индексы для удаления
        indices_to_remove = set()
        removed_records = []
        
        for signature, queries in duplicate_groups.items():
            # Получаем частотности
            frequencies = [q[freq_column] for q in queries]
            max_freq = max(frequencies)
            
            # Находим все запросы с максимальной частотностью
            max_freq_queries = [q for q in queries if q[freq_column] == max_freq]
            
            # Проверяем: все ли фразы имеют одинаковое значение
            all_same = len(set(frequencies)) == 1
            
            if all_same:
                # Все фразы имеют одинаковое значение
                # Оставляем все кроме одной (удаляем одну случайную)
                queries_to_remove = [queries[0]]  # Берем первую как "случайную"
            else:
                # Несколько фраз имеют макс. значение
                if len(max_freq_queries) > 1:
                    # Оставляем все фразы с макс. частотностью кроме одной
                    queries_to_remove = [max_freq_queries[0]]  # Удаляем первую
                else:
                    # Только одна фраза с макс. частотностью - оставляем её
                    # Удаляем все остальные
                    queries_to_remove = [q for q in queries if q[freq_column] < max_freq]
            
            # Добавляем индексы на удаление
            for q in queries_to_remove:
                indices_to_remove.add(q['index'])
                removed_records.append({
                    'keyword': q['keyword'],
                    'frequency_world': q['frequency_world'],
                    'frequency_exact': q['frequency_exact'],
                    'signature': signature,
                    'reason': 'implicit_duplicate'
                })
        
        # Создаем DataFrame с удаленными записями
        removed_df = pd.DataFrame(removed_records) if removed_records else pd.DataFrame()
        
        # Удаляем дубли из основного DataFrame
        result_df = df.drop(indices_to_remove).reset_index(drop=True)
        
        self.duplicates_count = len(indices_to_remove)
        self.unique_count = len(result_df)
        self.duplicate_groups = duplicate_groups
        
        return result_df, removed_df
    
    def get_deduplication_stats(self) -> Dict[str, int]:
        """Возвращает статистику дедупликации"""
        return {
            'total_duplicates_removed': self.duplicates_count,
            'unique_queries': self.unique_count,
            'duplicate_groups': len(self.duplicate_groups),
        }
    
    def export_duplicate_groups(
        self,
        output_path: Path,
        duplicate_groups: Dict[str, List[Dict]] = None
    ):
        """
        Экспортирует группы дублей в CSV для ручной проверки
        
        Args:
            output_path: Путь для сохранения
            duplicate_groups: Группы дублей (если None, используются найденные)
        """
        if duplicate_groups is None:
            duplicate_groups = self.duplicate_groups
        
        if not duplicate_groups:
            print("  ℹ️  Нет дублей для экспорта")
            return
        
        # Формируем таблицу для экспорта
        rows = []
        
        for signature, queries in duplicate_groups.items():
            # Сортируем по частотности
            queries_sorted = sorted(queries, key=lambda x: x['frequency_world'], reverse=True)
            
            for i, q in enumerate(queries_sorted):
                rows.append({
                    'signature': signature,
                    'group_size': len(queries),
                    'rank_in_group': i + 1,
                    'keyword': q['keyword'],
                    'frequency_world': q['frequency_world'],
                    'frequency_exact': q['frequency_exact'],
                    'is_max_freq': q['frequency_world'] == max(qu['frequency_world'] for qu in queries)
                })
        
        # Сохраняем
        df = pd.DataFrame(rows)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"✓ Экспортировано {len(duplicate_groups)} групп дублей ({len(rows)} запросов)")

