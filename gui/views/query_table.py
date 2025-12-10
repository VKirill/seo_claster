"""
Таблица запросов с фильтрами
"""

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableView,
    QLineEdit, QComboBox, QLabel, QPushButton
)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QKeySequence

from ..models.query_model import QueryModel


class QueryTableView(QWidget):
    """Таблица запросов с фильтрами"""
    
    query_selected = pyqtSignal(int)  # Сигнал выбора запроса (строка)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.model = QueryModel()
        self._init_ui()
        self._connect_signals()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QVBoxLayout(self)
        
        # Панель фильтров
        filters_layout = QHBoxLayout()
        
        # Поиск
        filters_layout.addWidget(QLabel("Поиск:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Введите запрос...")
        filters_layout.addWidget(self.search_input)
        
        # Интент
        filters_layout.addWidget(QLabel("Интент:"))
        self.intent_combo = QComboBox()
        self.intent_combo.addItems(["Все", "commercial", "informational", "navigational"])
        filters_layout.addWidget(self.intent_combo)
        
        # Частота мин
        filters_layout.addWidget(QLabel("Частота от:"))
        self.freq_min_input = QLineEdit()
        self.freq_min_input.setPlaceholderText("0")
        filters_layout.addWidget(self.freq_min_input)
        
        # Частота макс
        filters_layout.addWidget(QLabel("до:"))
        self.freq_max_input = QLineEdit()
        self.freq_max_input.setPlaceholderText("∞")
        filters_layout.addWidget(self.freq_max_input)
        
        # Кнопка сброса
        self.btn_reset = QPushButton("Сбросить")
        filters_layout.addWidget(self.btn_reset)
        
        layout.addLayout(filters_layout)
        
        # Таблица
        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        self.table_view.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.table_view.setSortingEnabled(True)
        self.table_view.setAlternatingRowColors(True)
        layout.addWidget(self.table_view)
        
        # Статус
        self.status_label = QLabel("Загрузите проект")
        layout.addWidget(self.status_label)
    
    def _connect_signals(self):
        """Подключение сигналов"""
        self.search_input.textChanged.connect(self._apply_filters)
        self.intent_combo.currentTextChanged.connect(self._apply_filters)
        self.freq_min_input.textChanged.connect(self._apply_filters)
        self.freq_max_input.textChanged.connect(self._apply_filters)
        self.btn_reset.clicked.connect(self._reset_filters)
        self.table_view.selectionModel().selectionChanged.connect(self._on_selection_changed)
        self.model.data_loaded.connect(self._on_data_loaded)
    
    def _apply_filters(self):
        """Применить фильтры"""
        filters = {}
        
        # Текстовый поиск
        search_text = self.search_input.text().strip()
        if search_text:
            filters['search_text'] = search_text
        
        # Интент
        intent = self.intent_combo.currentText()
        if intent != "Все":
            filters['intent'] = intent
        
        # Частота
        try:
            freq_min = int(self.freq_min_input.text()) if self.freq_min_input.text() else None
            if freq_min is not None:
                filters['frequency_min'] = freq_min
        except ValueError:
            pass
        
        try:
            freq_max = int(self.freq_max_input.text()) if self.freq_max_input.text() else None
            if freq_max is not None:
                filters['frequency_max'] = freq_max
        except ValueError:
            pass
        
        self.model.set_filters(filters)
    
    def _reset_filters(self):
        """Сбросить фильтры"""
        self.search_input.clear()
        self.intent_combo.setCurrentIndex(0)
        self.freq_min_input.clear()
        self.freq_max_input.clear()
        self._apply_filters()
    
    def _on_selection_changed(self):
        """Обработка выбора строки"""
        indexes = self.table_view.selectionModel().selectedIndexes()
        if indexes:
            row = indexes[0].row()
            self.query_selected.emit(row)
    
    def _on_data_loaded(self, count: int):
        """Обновить статус после загрузки"""
        self.status_label.setText(f"Загружено запросов: {count}")
    
    def load_group(self, group_name: str):
        """Загрузить группу"""
        self.model.load_group(group_name)


