"""
Диалог импорта CSV
"""

from pathlib import Path
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QPushButton, QFileDialog
)


class ImportDialog(QDialog):
    """Диалог импорта CSV файла"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Импорт проекта")
        self.setMinimumWidth(500)
        self.file_path = None
        self.project_name = None
        self._init_ui()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QVBoxLayout(self)
        
        # Файл
        file_layout = QHBoxLayout()
        file_layout.addWidget(QLabel("CSV файл:"))
        self.file_input = QLineEdit()
        self.file_input.setReadOnly(True)
        file_layout.addWidget(self.file_input)
        
        self.btn_browse = QPushButton("Обзор...")
        self.btn_browse.clicked.connect(self._on_browse)
        file_layout.addWidget(self.btn_browse)
        layout.addLayout(file_layout)
        
        # Название проекта
        name_layout = QHBoxLayout()
        name_layout.addWidget(QLabel("Название проекта:"))
        self.name_input = QLineEdit()
        name_layout.addWidget(self.name_input)
        layout.addLayout(name_layout)
        
        # Кнопки
        buttons_layout = QHBoxLayout()
        self.btn_import = QPushButton("Импортировать")
        self.btn_cancel = QPushButton("Отмена")
        buttons_layout.addWidget(self.btn_import)
        buttons_layout.addWidget(self.btn_cancel)
        layout.addLayout(buttons_layout)
        
        # Подключение сигналов
        self.btn_import.clicked.connect(self._on_import)
        self.btn_cancel.clicked.connect(self.reject)
    
    def _on_browse(self):
        """Выбор файла"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Выберите CSV файл",
            "",
            "CSV Files (*.csv);;All Files (*)"
        )
        if file_path:
            self.file_input.setText(file_path)
            # Автозаполнение названия из имени файла
            if not self.name_input.text():
                self.name_input.setText(Path(file_path).stem)
    
    def _on_import(self):
        """Импорт"""
        file_path = self.file_input.text()
        project_name = self.name_input.text().strip()
        
        if not file_path or not Path(file_path).exists():
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.warning(self, "Ошибка", "Выберите существующий файл")
            return
        
        if not project_name:
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.warning(self, "Ошибка", "Введите название проекта")
            return
        
        self.file_path = Path(file_path)
        self.project_name = project_name
        self.accept()





