"""
Виджет фильтров (вынесен для переиспользования)
"""

from PyQt6.QtWidgets import QWidget, QHBoxLayout, QLineEdit, QComboBox, QLabel, QPushButton
from PyQt6.QtCore import pyqtSignal


class FilterWidget(QWidget):
    """Виджет фильтров"""
    
    filters_changed = pyqtSignal(dict)  # Сигнал изменения фильтров
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QHBoxLayout(self)
        
        # Поиск
        layout.addWidget(QLabel("Поиск:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Введите запрос...")
        self.search_input.textChanged.connect(self._emit_filters)
        layout.addWidget(self.search_input)
        
        # Интент
        layout.addWidget(QLabel("Интент:"))
        self.intent_combo = QComboBox()
        self.intent_combo.addItems(["Все", "commercial", "informational", "navigational"])
        self.intent_combo.currentTextChanged.connect(self._emit_filters)
        layout.addWidget(self.intent_combo)
        
        # Кнопка сброса
        self.btn_reset = QPushButton("Сбросить")
        self.btn_reset.clicked.connect(self._reset)
        layout.addWidget(self.btn_reset)
    
    def _emit_filters(self):
        """Отправить сигнал с фильтрами"""
        filters = {}
        
        search_text = self.search_input.text().strip()
        if search_text:
            filters['search_text'] = search_text
        
        intent = self.intent_combo.currentText()
        if intent != "Все":
            filters['intent'] = intent
        
        self.filters_changed.emit(filters)
    
    def _reset(self):
        """Сбросить фильтры"""
        self.search_input.clear()
        self.intent_combo.setCurrentIndex(0)
        self._emit_filters()





