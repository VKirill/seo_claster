"""
Дерево кластеров
"""

from PyQt6.QtWidgets import QWidget, QVBoxLayout, QTreeView, QLabel
from PyQt6.QtCore import Qt, pyqtSignal

from ..models.cluster_model import ClusterModel


class ClusterTreeView(QWidget):
    """Дерево кластеров"""
    
    cluster_selected = pyqtSignal(int)  # Сигнал выбора кластера
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.model = ClusterModel()
        self._init_ui()
        self._connect_signals()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QVBoxLayout(self)
        
        # Заголовок
        title = QLabel("Кластеры")
        title.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(title)
        
        # Дерево
        self.tree_view = QTreeView()
        self.tree_view.setModel(self.model)
        self.tree_view.setHeaderHidden(True)
        layout.addWidget(self.tree_view)
        
        # Статистика кластера
        self.stats_label = QLabel("Выберите кластер")
        self.stats_label.setWordWrap(True)
        layout.addWidget(self.stats_label)
    
    def _connect_signals(self):
        """Подключение сигналов"""
        self.tree_view.selectionModel().selectionChanged.connect(self._on_selection_changed)
    
    def _on_selection_changed(self):
        """Обработка выбора кластера"""
        indexes = self.tree_view.selectionModel().selectedIndexes()
        if indexes:
            index = indexes[0]
            cluster_id = index.internalPointer()
            if cluster_id:
                self.cluster_selected.emit(cluster_id)
                self._update_stats(cluster_id)
    
    def _update_stats(self, cluster_id: int):
        """Обновить статистику кластера"""
        queries = self.model.get_cluster_queries(cluster_id)
        if queries:
            preview = ", ".join(queries[:5])
            if len(queries) > 5:
                preview += f"... (+{len(queries) - 5})"
            self.stats_label.setText(f"Кластер {cluster_id}\nЗапросов: {len(queries)}\n\n{preview}")
        else:
            self.stats_label.setText(f"Кластер {cluster_id}\nНет данных")
    
    def load_data(self, df):
        """Загрузить данные для дерева"""
        self.model.load_data(df)


