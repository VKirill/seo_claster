"""
Модель кластеров для дерева
"""

from typing import List, Dict, Any, Optional
from PyQt6.QtCore import QAbstractItemModel, QModelIndex, Qt
import pandas as pd


class ClusterModel(QAbstractItemModel):
    """Модель дерева кластеров"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.df: Optional[pd.DataFrame] = None
        self.cluster_data: Dict[int, Dict[str, Any]] = {}
    
    def load_data(self, df: pd.DataFrame):
        """
        Загрузить данные для построения дерева кластеров
        
        Args:
            df: DataFrame с колонкой semantic_cluster_id
        """
        self.beginResetModel()
        self.df = df
        self.cluster_data = {}
        
        if df is not None and 'semantic_cluster_id' in df.columns:
            # Группируем по кластерам
            for cluster_id, group in df.groupby('semantic_cluster_id'):
                if pd.isna(cluster_id):
                    continue
                
                cluster_id = int(cluster_id)
                self.cluster_data[cluster_id] = {
                    'id': cluster_id,
                    'size': len(group),
                    'queries': group['keyword'].tolist(),
                    'avg_frequency': group['frequency_world'].mean() if 'frequency_world' in group.columns else 0,
                    'intents': group['main_intent'].value_counts().to_dict() if 'main_intent' in group.columns else {}
                }
        
        self.endResetModel()
    
    def rowCount(self, parent=QModelIndex()) -> int:
        """Количество строк (кластеров)"""
        if not parent.isValid():
            return len(self.cluster_data)
        return 0  # Плоский список кластеров (без вложенности пока)
    
    def columnCount(self, parent=QModelIndex()) -> int:
        """Количество колонок"""
        return 1
    
    def index(self, row: int, column: int, parent=QModelIndex()) -> QModelIndex:
        """Создать индекс"""
        if not self.hasIndex(row, column, parent):
            return QModelIndex()
        
        if not parent.isValid():
            cluster_ids = sorted(self.cluster_data.keys())
            if row < len(cluster_ids):
                return self.createIndex(row, column, cluster_ids[row])
        
        return QModelIndex()
    
    def parent(self, index: QModelIndex) -> QModelIndex:
        """Родительский индекс"""
        return QModelIndex()
    
    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        """Данные для отображения"""
        if not index.isValid():
            return None
        
        cluster_id = index.internalPointer()
        if cluster_id not in self.cluster_data:
            return None
        
        cluster = self.cluster_data[cluster_id]
        
        if role == Qt.ItemDataRole.DisplayRole:
            return f"Кластер {cluster_id} ({cluster['size']} запросов)"
        
        elif role == Qt.ItemDataRole.ToolTipRole:
            intents_str = ", ".join([f"{k}: {v}" for k, v in cluster['intents'].items()])
            return f"Размер: {cluster['size']}\nСредняя частота: {cluster['avg_frequency']:.0f}\nИнтенты: {intents_str}"
        
        return None
    
    def get_cluster_queries(self, cluster_id: int) -> List[str]:
        """Получить список запросов кластера"""
        if cluster_id in self.cluster_data:
            return self.cluster_data[cluster_id]['queries']
        return []





