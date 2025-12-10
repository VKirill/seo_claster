"""
Диалог экспорта результатов
"""

from pathlib import Path
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel,
    QComboBox, QPushButton, QCheckBox, QFileDialog
)
from PyQt6.QtCore import pyqtSignal


class ExportDialog(QDialog):
    """Диалог экспорта"""
    
    export_requested = pyqtSignal(str, dict)  # Сигнал экспорта (формат, опции)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Экспорт результатов")
        self.setMinimumWidth(400)
        self._init_ui()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QVBoxLayout(self)
        
        # Формат
        format_layout = QHBoxLayout()
        format_layout.addWidget(QLabel("Формат:"))
        self.format_combo = QComboBox()
        self.format_combo.addItems(["Excel", "CSV", "JSON", "HTML"])
        format_layout.addWidget(self.format_combo)
        layout.addLayout(format_layout)
        
        # Опции
        self.chk_charts = QCheckBox("Включить графики")
        self.chk_charts.setChecked(False)
        layout.addWidget(self.chk_charts)
        
        self.chk_pivot = QCheckBox("Включить сводные таблицы")
        self.chk_pivot.setChecked(False)
        layout.addWidget(self.chk_pivot)
        
        # Кнопки
        buttons_layout = QHBoxLayout()
        self.btn_export = QPushButton("Экспортировать")
        self.btn_cancel = QPushButton("Отмена")
        buttons_layout.addWidget(self.btn_export)
        buttons_layout.addWidget(self.btn_cancel)
        layout.addLayout(buttons_layout)
        
        # Подключение сигналов
        self.btn_export.clicked.connect(self._on_export)
        self.btn_cancel.clicked.connect(self.reject)
    
    def _on_export(self):
        """Экспорт"""
        format_name = self.format_combo.currentText().lower()
        options = {
            'include_charts': self.chk_charts.isChecked(),
            'include_pivot': self.chk_pivot.isChecked()
        }
        self.export_requested.emit(format_name, options)
        self.accept()


