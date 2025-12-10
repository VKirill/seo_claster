"""
Query Group Configuration
Конфигурация отдельной группы запросов
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class QueryGroup:
    """Конфигурация группы запросов"""
    
    name: str  # Название группы (slug, например: "skud", "crm")
    input_file: Path  # Путь к файлу с запросами (semantika/skud.csv)
    
    # Автоматически генерируемые пути
    output_dir: Optional[Path] = None  # output/groups/skud/
    
    # БД теперь общая для всех групп: output/serp_data.db
    # Данные разделяются через поле query_group в таблицах
    
    def __post_init__(self):
        """Автоматическая генерация путей"""
        if self.output_dir is None:
            self.output_dir = Path(f"output/groups/{self.name}")
    
    def ensure_directories(self):
        """Создать все необходимые директории"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def get_output_path(self, filename: str) -> Path:
        """Получить путь к файлу в output директории группы"""
        return self.output_dir / filename

