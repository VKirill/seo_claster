"""
Модель запросов с ленивой загрузкой из БД
"""

from typing import Optional, Dict, Any
from PyQt6.QtCore import QAbstractTableModel, QModelIndex, Qt, pyqtSignal
import pandas as pd

from seo_analyzer.core.cache.master_query_db import MasterQueryDatabase


class QueryModel(QAbstractTableModel):
    """Модель таблицы запросов с ленивой загрузкой"""
    
    # Колонки таблицы
    COLUMNS = [
        'keyword', 'frequency_world', 'frequency_exact',
        'main_intent', 'kei', 'difficulty', 'competition_score',
        'semantic_cluster_id', 'serp_status'
    ]
    
    COLUMN_NAMES = {
        'keyword': 'Запрос',
        'frequency_world': 'Частота',
        'frequency_exact': 'Частота точн.',
        'main_intent': 'Интент',
        'kei': 'KEI',
        'difficulty': 'Сложность',
        'competition_score': 'Конкуренция',
        'semantic_cluster_id': 'Кластер',
        'serp_status': 'SERP'
    }
    
    data_loaded = pyqtSignal(int)  # Сигнал при загрузке данных (количество строк)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.master_db = MasterQueryDatabase()
        self.df: Optional[pd.DataFrame] = None
        self.current_group: Optional[str] = None
        self._filters: Dict[str, Any] = {}
    
    def load_group(self, group_name: str):
        """
        Загрузить запросы группы
        
        Args:
            group_name: Название группы
        """
        try:
            self.beginResetModel()
            self.current_group = group_name
            
            # Загружаем данные из БД
            self.df = self.master_db.load_queries(group_name, include_serp_urls=False)
            
            if self.df is None or self.df.empty:
                self.df = pd.DataFrame()
            else:
                # Применяем фильтры если есть
                self._apply_filters()
            
            self.endResetModel()
            
            row_count = len(self.df) if self.df is not None else 0
            self.data_loaded.emit(row_count)
            
        except Exception as e:
            print(f"Ошибка загрузки группы {group_name}: {e}")
            self.df = pd.DataFrame()
            self.endResetModel()
    
    def _apply_filters(self):
        """Применить активные фильтры"""
        if self.df is None or self.df.empty:
            return
        
        df = self.df.copy()
        
        # Текстовый поиск
        if 'search_text' in self._filters and self._filters['search_text']:
            if 'keyword' in df.columns:
                search = self._filters['search_text'].lower()
                mask = df['keyword'].str.lower().str.contains(search, na=False)
                df = df[mask]
        
        # Фильтр по интенту
        if 'intent' in self._filters and self._filters['intent']:
            if 'main_intent' in df.columns:
                df = df[df['main_intent'] == self._filters['intent']]
        
        # Диапазон частоты
        if 'frequency_min' in self._filters and 'frequency_world' in df.columns:
            df = df[df['frequency_world'] >= self._filters['frequency_min']]
        if 'frequency_max' in self._filters and 'frequency_world' in df.columns:
            df = df[df['frequency_world'] <= self._filters['frequency_max']]
        
        self.df = df
    
    def set_filters(self, filters: Dict[str, Any]):
        """Установить фильтры"""
        self._filters = filters
        if self.current_group:
            self.load_group(self.current_group)
    
    def rowCount(self, parent=QModelIndex()) -> int:
        """Количество строк"""
        if self.df is None:
            return 0
        return len(self.df)
    
    def columnCount(self, parent=QModelIndex()) -> int:
        """Количество колонок"""
        return len(self.COLUMNS)
    
    def _get_available_columns(self) -> list:
        """Получить список доступных колонок в DataFrame"""
        if self.df is None or self.df.empty:
            return []
        return [col for col in self.COLUMNS if col in self.df.columns]
    
    def headerData(self, section: int, orientation: Qt.Orientation, role=Qt.ItemDataRole.DisplayRole):
        """Заголовки колонок"""
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            if section < len(self.COLUMNS):
                col_name = self.COLUMNS[section]
                # Показываем заголовок даже если колонка отсутствует в данных
                return self.COLUMN_NAMES.get(col_name, col_name)
        return None
    
    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        """Данные ячейки"""
        if not index.isValid() or self.df is None or self.df.empty:
            return None
        
        row = index.row()
        col = index.column()
        
        if row >= len(self.df) or col >= len(self.COLUMNS):
            return None
        
        column_name = self.COLUMNS[col]
        
        # Проверяем наличие колонки в DataFrame
        if column_name not in self.df.columns:
            if role == Qt.ItemDataRole.DisplayRole:
                return ""  # Возвращаем пустую строку для отсутствующих колонок
            return None
        
        if role == Qt.ItemDataRole.DisplayRole:
            value = self.df.iloc[row][column_name]
            
            # Форматирование значений
            if pd.isna(value):
                return ""
            elif isinstance(value, float):
                if column_name in ['kei', 'difficulty', 'competition_score']:
                    return f"{value:.2f}"
                elif column_name in ['frequency_world', 'frequency_exact']:
                    return f"{int(value):,}"
            elif isinstance(value, (int, float)) and column_name == 'semantic_cluster_id':
                return str(int(value)) if not pd.isna(value) else ""
            
            return str(value)
        
        elif role == Qt.ItemDataRole.ToolTipRole:
            value = self.df.iloc[row][column_name]
            return str(value) if not pd.isna(value) else ""
        
        return None
    
    def get_query_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Получить данные запроса по строке"""
        if self.df is None or row >= len(self.df):
            return None
        return self.df.iloc[row].to_dict()

