"""
Query Group Manager
Менеджер для работы с несколькими группами запросов
"""

from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd

from .group_config import QueryGroup
from .group_database import GroupDatabaseManager


class QueryGroupManager:
    """Менеджер групп запросов"""
    
    # Базовая директория для semantika файлов
    SEMANTIKA_DIR = Path("semantika")
    
    def __init__(self):
        """Инициализация менеджера групп"""
        self.groups: List[QueryGroup] = []
        self.current_group: Optional[QueryGroup] = None
    
    def discover_groups(self) -> List[QueryGroup]:
        """
        Автоматическое обнаружение групп из semantika/
        
        Returns:
            Список обнаруженных групп
        """
        if not self.SEMANTIKA_DIR.exists():
            return []
        
        groups = []
        
        # Ищем все CSV файлы в semantika/
        for csv_file in self.SEMANTIKA_DIR.glob("*.csv"):
            # Игнорируем служебные файлы
            if csv_file.stem.endswith('_backup'):
                continue
            if csv_file.stem.startswith('~'):  # Временные файлы Excel
                continue
            if csv_file.stem.startswith('.'):  # Скрытые файлы
                continue
            
            # Название группы = имя файла без расширения
            group_name = csv_file.stem
            
            group = QueryGroup(
                name=group_name,
                input_file=csv_file
            )
            
            groups.append(group)
        
        self.groups = groups
        return groups
    
    def add_group(
        self,
        name: str,
        input_file: Path
    ) -> QueryGroup:
        """
        Добавить группу вручную
        
        Args:
            name: Название группы (slug)
            input_file: Путь к файлу с запросами
            
        Returns:
            Созданная группа
        """
        group = QueryGroup(name=name, input_file=input_file)
        self.groups.append(group)
        return group
    
    def get_group(self, name: str) -> Optional[QueryGroup]:
        """Получить группу по имени"""
        for group in self.groups:
            if group.name == name:
                return group
        return None
    
    def set_current_group(self, name: str):
        """Установить текущую активную группу"""
        group = self.get_group(name)
        if group:
            self.current_group = group
            group.ensure_directories()
        else:
            raise ValueError(f"Группа '{name}' не найдена")
    
    def load_queries(self, group: QueryGroup) -> pd.DataFrame:
        """
        Загрузить запросы из файла группы
        
        Args:
            group: Группа запросов
            
        Returns:
            DataFrame с запросами
        """
        if not group.input_file.exists():
            raise FileNotFoundError(
                f"Файл группы не найден: {group.input_file}"
            )
        
        # Загружаем CSV с автоопределением разделителя
        # Пробуем разные комбинации encoding и delimiter
        df = None
        for encoding in ['utf-8-sig', 'utf-8', 'cp1251', 'windows-1251']:
            for delimiter in [',', ';', '\t']:
                try:
                    test_df = pd.read_csv(group.input_file, delimiter=delimiter, encoding=encoding)
                    # Проверяем что загрузилось корректно (больше 1 колонки)
                    if len(test_df.columns) > 1:
                        df = test_df
                        break
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
            if df is not None:
                break
        
        if df is None:
            raise ValueError(f"Не удалось определить формат файла: {group.input_file}")
        
        # Добавляем метаданные группы
        df['query_group'] = group.name
        
        return df
    
    def get_group_database(self, group: QueryGroup) -> GroupDatabaseManager:
        """
        Получить менеджер БД для группы
        
        Args:
            group: Группа запросов
            
        Returns:
            GroupDatabaseManager для этой группы
            
        Note:
            GroupDatabaseManager теперь только создаёт таблицу domain_stats в master_queries.db
        """
        return GroupDatabaseManager(query_group=group.name)
    
    def list_groups(self) -> List[Dict[str, Any]]:
        """
        Получить список всех групп с информацией
        
        Returns:
            Список словарей с информацией о группах
        """
        result = []
        
        for group in self.groups:
            info = {
                'name': group.name,
                'input_file': str(group.input_file),
                'output_dir': str(group.output_dir),
                'db_exists': group.db_path.exists(),
                'file_exists': group.input_file.exists(),
            }
            
            # Количество запросов
            if group.input_file.exists():
                try:
                    df = pd.read_csv(
                        group.input_file, 
                        delimiter=';', 
                        encoding='utf-8-sig'
                    )
                    info['queries_count'] = len(df)
                except Exception:
                    info['queries_count'] = None
            else:
                info['queries_count'] = None
            
            result.append(info)
        
        return result
    
    def ensure_all_directories(self):
        """Создать все необходимые директории для всех групп"""
        for group in self.groups:
            group.ensure_directories()

