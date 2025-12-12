"""
Левая панель управления проектами
"""

from pathlib import Path
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QTreeView, QLabel, QFileDialog, QMessageBox
)
from PyQt6.QtCore import Qt, pyqtSignal

from ..models.project_model import ProjectModel


class ProjectPanel(QWidget):
    """Панель управления проектами"""
    
    project_selected = pyqtSignal(str)  # Сигнал выбора проекта
    project_imported = pyqtSignal(str, Path)  # Сигнал импорта проекта
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.model = ProjectModel()
        self._init_ui()
        self._connect_signals()
    
    def _init_ui(self):
        """Инициализация UI"""
        layout = QVBoxLayout(self)
        
        # Заголовок
        title = QLabel("Проекты")
        title.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(title)
        
        # Кнопки управления
        buttons_layout = QHBoxLayout()
        
        self.btn_create = QPushButton("Создать")
        self.btn_import = QPushButton("Импорт CSV")
        self.btn_delete = QPushButton("Удалить")
        self.btn_refresh = QPushButton("Обновить")
        
        buttons_layout.addWidget(self.btn_create)
        buttons_layout.addWidget(self.btn_import)
        buttons_layout.addWidget(self.btn_delete)
        buttons_layout.addWidget(self.btn_refresh)
        
        layout.addLayout(buttons_layout)
        
        # Дерево проектов
        self.tree_view = QTreeView()
        self.tree_view.setModel(self.model)
        self.tree_view.setHeaderHidden(True)
        self.tree_view.setRootIsDecorated(False)
        layout.addWidget(self.tree_view)
        
        # Статистика
        self.stats_label = QLabel("Выберите проект")
        self.stats_label.setWordWrap(True)
        layout.addWidget(self.stats_label)
    
    def _connect_signals(self):
        """Подключение сигналов"""
        self.tree_view.selectionModel().selectionChanged.connect(self._on_selection_changed)
        self.btn_create.clicked.connect(self._on_create_clicked)
        self.btn_import.clicked.connect(self._on_import_clicked)
        self.btn_delete.clicked.connect(self._on_delete_clicked)
        self.btn_refresh.clicked.connect(self._on_refresh_clicked)
    
    def _on_selection_changed(self):
        """Обработка выбора проекта"""
        indexes = self.tree_view.selectionModel().selectedIndexes()
        if indexes:
            index = indexes[0]
            group = self.model.groups[index.row()]
            self.project_selected.emit(group.name)
            self._update_stats(group)
    
    def _update_stats(self, group):
        """Обновить статистику группы"""
        try:
            import pandas as pd
            if group.input_file.exists():
                df = pd.read_csv(group.input_file, delimiter=';', encoding='utf-8-sig')
                count = len(df)
                self.stats_label.setText(f"Проект: {group.name}\nЗапросов: {count}")
            else:
                self.stats_label.setText(f"Проект: {group.name}\nФайл не найден")
        except Exception as e:
            self.stats_label.setText(f"Ошибка: {e}")
    
    def _on_create_clicked(self):
        """Создать новый проект"""
        # Пока используем импорт как создание
        self._on_import_clicked()
    
    def _on_import_clicked(self):
        """Импортировать CSV файл"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Выберите CSV файл",
            "",
            "CSV Files (*.csv);;All Files (*)"
        )
        
        if file_path:
            # Запрашиваем название проекта
            from PyQt6.QtWidgets import QInputDialog
            name, ok = QInputDialog.getText(
                self,
                "Название проекта",
                "Введите название проекта:",
                text=Path(file_path).stem
            )
            
            if ok and name:
                if self.model.add_group(name, Path(file_path)):
                    QMessageBox.information(self, "Успех", f"Проект '{name}' добавлен")
                    self.project_imported.emit(name, Path(file_path))
                else:
                    QMessageBox.warning(self, "Ошибка", "Не удалось добавить проект")
    
    def _on_delete_clicked(self):
        """Удалить выбранный проект"""
        indexes = self.tree_view.selectionModel().selectedIndexes()
        if not indexes:
            QMessageBox.warning(self, "Внимание", "Выберите проект для удаления")
            return
        
        index = indexes[0]
        group = self.model.groups[index.row()]
        
        reply = QMessageBox.question(
            self,
            "Подтверждение",
            f"Удалить проект '{group.name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            if self.model.remove_group(group.name):
                QMessageBox.information(self, "Успех", "Проект удален")
            else:
                QMessageBox.warning(self, "Ошибка", "Не удалось удалить проект")
    
    def _on_refresh_clicked(self):
        """Обновить список проектов"""
        self.model.refresh()






