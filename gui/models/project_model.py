"""
Модель проектов/групп запросов
"""

from pathlib import Path
from typing import List, Optional, Dict, Any
from PyQt6.QtCore import QAbstractItemModel, QModelIndex, Qt, pyqtSignal

from seo_analyzer.core.query_groups import QueryGroupManager
from seo_analyzer.core.query_groups.group_config import QueryGroup


class ProjectModel(QAbstractItemModel):
    """Модель для управления проектами/группами"""
    
    project_changed = pyqtSignal(str)  # Сигнал при изменении активного проекта
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.group_manager = QueryGroupManager()
        self.groups: List[QueryGroup] = []
        self.current_group: Optional[str] = None
        self._refresh_groups()
    
    def _refresh_groups(self):
        """Обновить список групп"""
        self.groups = self.group_manager.discover_groups()
    
    def rowCount(self, parent=QModelIndex()) -> int:
        """Количество строк (групп)"""
        return len(self.groups)
    
    def columnCount(self, parent=QModelIndex()) -> int:
        """Количество колонок"""
        return 1
    
    def index(self, row: int, column: int, parent=QModelIndex()) -> QModelIndex:
        """Создать индекс для строки"""
        if not self.hasIndex(row, column, parent):
            return QModelIndex()
        return self.createIndex(row, column, self.groups[row] if row < len(self.groups) else None)
    
    def parent(self, index: QModelIndex) -> QModelIndex:
        """Родительский индекс (все группы на верхнем уровне)"""
        return QModelIndex()
    
    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        """Данные для отображения"""
        if not index.isValid():
            return None
        
        group = self.groups[index.row()]
        
        if role == Qt.ItemDataRole.DisplayRole:
            return group.name
        elif role == Qt.ItemDataRole.ToolTipRole:
            return f"Файл: {group.input_file}\nЗапросов: {self._get_queries_count(group)}"
        
        return None
    
    def _get_queries_count(self, group: QueryGroup) -> int:
        """Получить количество запросов в группе"""
        try:
            if group.input_file.exists():
                import pandas as pd
                df = pd.read_csv(group.input_file, delimiter=';', encoding='utf-8-sig', nrows=1)
                # Полный подсчет
                df = pd.read_csv(group.input_file, delimiter=';', encoding='utf-8-sig')
                return len(df)
        except Exception:
            pass
        return 0
    
    def get_group(self, name: str) -> Optional[QueryGroup]:
        """Получить группу по имени"""
        return self.group_manager.get_group(name)
    
    def set_current_group(self, name: str):
        """Установить текущую активную группу"""
        if name != self.current_group:
            self.current_group = name
            self.project_changed.emit(name)
    
    def refresh(self):
        """Обновить список групп"""
        self.beginResetModel()
        self._refresh_groups()
        self.endResetModel()
    
    def add_group(self, name: str, csv_file: Path) -> bool:
        """
        Добавить новую группу
        
        Args:
            name: Название группы
            csv_file: Путь к CSV файлу
            
        Returns:
            True если успешно добавлено
        """
        try:
            # Копируем файл в semantika/
            semantika_dir = Path("semantika")
            semantika_dir.mkdir(exist_ok=True)
            target_file = semantika_dir / f"{name}.csv"
            
            import shutil
            shutil.copy2(csv_file, target_file)
            
            # Обновляем модель
            self.refresh()
            return True
        except Exception as e:
            print(f"Ошибка добавления группы: {e}")
            return False
    
    def remove_group(self, name: str) -> bool:
        """
        Удалить группу
        
        Args:
            name: Название группы
            
        Returns:
            True если успешно удалено
        """
        try:
            group = self.get_group(name)
            if not group:
                return False
            
            # Создаем backup
            backup_file = group.input_file.parent / f"{group.input_file.stem}_backup.csv"
            if group.input_file.exists():
                import shutil
                shutil.copy2(group.input_file, backup_file)
            
            # Удаляем файл
            if group.input_file.exists():
                group.input_file.unlink()
            
            # Обновляем модель
            self.refresh()
            return True
        except Exception as e:
            print(f"Ошибка удаления группы: {e}")
            return False





