"""
Виджет прогресса операций
"""

from PyQt6.QtWidgets import QWidget, QVBoxLayout, QProgressBar, QLabel
from PyQt6.QtCore import Qt


class ProgressWidget(QWidget):
    """Виджет отображения прогресса"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QVBoxLayout(self)
        
        self.status_label = QLabel("Готово")
        layout.addWidget(self.status_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
    
    def set_progress(self, message: str, value: int = 0, maximum: int = 100):
        """
        Установить прогресс
        
        Args:
            message: Текст сообщения
            value: Текущее значение
            maximum: Максимальное значение
        """
        self.status_label.setText(message)
        self.progress_bar.setMaximum(maximum)
        self.progress_bar.setValue(value)
        self.progress_bar.setVisible(maximum > 0)
    
    def hide_progress(self):
        """Скрыть прогресс-бар"""
        self.progress_bar.setVisible(False)
        self.status_label.setText("Готово")






