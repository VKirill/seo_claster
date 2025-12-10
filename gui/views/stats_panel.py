"""
Панель статистики
"""

from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel
from PyQt6.QtCore import Qt


class StatsPanel(QWidget):
    """Панель статистики проекта"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QVBoxLayout(self)
        
        title = QLabel("Статистика")
        title.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(title)
        
        self.stats_label = QLabel("Нет данных")
        self.stats_label.setWordWrap(True)
        layout.addWidget(self.stats_label)
        
        layout.addStretch()
    
    def update_stats(self, stats: dict):
        """
        Обновить статистику
        
        Args:
            stats: Словарь со статистикой
        """
        if not stats:
            self.stats_label.setText("Нет данных")
            return
        
        lines = []
        if 'total_queries' in stats:
            lines.append(f"Всего запросов: {stats['total_queries']}")
        if 'clusters_count' in stats:
            lines.append(f"Кластеров: {stats['clusters_count']}")
        if 'avg_frequency' in stats:
            lines.append(f"Средняя частота: {stats['avg_frequency']:.0f}")
        
        self.stats_label.setText("\n".join(lines))


