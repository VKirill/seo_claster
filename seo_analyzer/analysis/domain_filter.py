"""Фильтрация доменов по стоп-листу"""

from typing import Set
from urllib.parse import urlparse
from pathlib import Path


class DomainFilter:
    """Фильтр доменов по стоп-листу"""
    
    def __init__(self, stop_domains_file: Path):
        """
        Инициализация
        
        Args:
            stop_domains_file: Путь к файлу со стоп-доменами
        """
        self.stop_domains = self._load_stop_domains(stop_domains_file)
    
    def _load_stop_domains(self, file_path: Path) -> Set[str]:
        """
        Загрузить стоп-домены из файла
        
        Args:
            file_path: Путь к файлу
            
        Returns:
            Множество доменов
        """
        stop_domains = set()
        
        if not file_path.exists():
            print(f"⚠️  Файл стоп-доменов не найден: {file_path}")
            return stop_domains
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    domain = line.strip().lower()
                    if domain and not domain.startswith('#'):
                        stop_domains.add(domain)
            
            print(f"✓ Загружено {len(stop_domains)} стоп-доменов")
        except Exception as e:
            print(f"⚠️  Ошибка загрузки стоп-доменов: {e}")
        
        return stop_domains
    
    def is_allowed(self, url: str) -> bool:
        """
        Проверить разрешен ли URL
        
        Args:
            url: URL для проверки
            
        Returns:
            True если разрешен, False если в стоп-листе
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Убираем www. если есть
            if domain.startswith('www.'):
                domain_without_www = domain[4:]
            else:
                domain_without_www = domain
            
            # Проверяем оба варианта (с www и без)
            if domain in self.stop_domains:
                return False
            
            if domain_without_www in self.stop_domains:
                return False
            
            # Проверяем поддомены
            for stop_domain in self.stop_domains:
                if domain.endswith(f'.{stop_domain}') or domain == stop_domain:
                    return False
            
            return True
            
        except Exception as e:
            print(f"⚠️  Ошибка парсинга URL {url}: {e}")
            return False
    
    def filter_urls(self, urls: list) -> list:
        """
        Отфильтровать список URL
        
        Args:
            urls: Список URL
            
        Returns:
            Отфильтрованный список
        """
        return [url for url in urls if self.is_allowed(url)]


