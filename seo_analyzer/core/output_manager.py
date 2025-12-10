"""
Output Manager
Управление output директориями для групп и общего режима
"""

from pathlib import Path
from typing import Optional


class OutputManager:
    """Менеджер output директорий"""
    
    DEFAULT_OUTPUT_DIR = Path("output")
    
    def __init__(self, current_group=None):
        """
        Инициализация менеджера
        
        Args:
            current_group: QueryGroup или None для общего режима
        """
        self.current_group = current_group
        
        if current_group:
            self.output_dir = current_group.output_dir
        else:
            self.output_dir = self.DEFAULT_OUTPUT_DIR
        
        # Создаем директорию
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def get_path(self, filename: str) -> Path:
        """
        Получить путь к файлу в output директории
        
        Args:
            filename: Имя файла
            
        Returns:
            Полный путь к файлу
        """
        return self.output_dir / filename
    
    def get_cache_dir(self) -> Path:
        """Получить директорию для кэша"""
        if self.current_group:
            cache_dir = self.current_group.cache_dir
        else:
            cache_dir = self.output_dir / "serp_cache"
        
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir
    
    def get_db_path(self) -> Path:
        """Получить путь к БД"""
        # Всегда используем master_queries.db (единая БД для всех данных)
        return self.output_dir / "master_queries.db"
    
    def ensure_directories(self):
        """Создать все необходимые директории"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.get_cache_dir()
        
        if self.current_group:
            self.current_group.ensure_directories()

