"""
Менеджер прокси для ротации IP-адресов
"""

import random
from typing import List, Optional, Dict, Any
from pathlib import Path


class ProxyManager:
    """Управление прокси с ротацией"""
    
    def __init__(self, proxies: Optional[List[str]] = None, proxy_file: Optional[str] = None, silent: bool = False):
        """
        Args:
            proxies: Список прокси в формате ['http://user:pass@ip:port', ...]
            proxy_file: Путь к файлу с прокси (по одному на строку)
            silent: Не выводить сообщения о загрузке прокси
        """
        self.proxies = []
        self.current_index = 0
        
        # Загружаем прокси из списка
        if proxies:
            self.proxies = proxies.copy()
        
        # Загружаем прокси из файла
        if proxy_file:
            proxy_path = Path(proxy_file)
            if proxy_path.exists():
                with open(proxy_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        # Парсим формат: IP:PORT:LOGIN:PASSWORD или IP PORT LOGIN PASSWORD
                        if ':' in line:
                            # Формат IP:PORT:LOGIN:PASSWORD
                            parts = line.split(':')
                            if len(parts) == 4:
                                ip, port, login, password = parts
                                proxy_url = f"http://{login}:{password}@{ip}:{port}"
                                self.proxies.append(proxy_url)
                            elif line.startswith('http://') or line.startswith('https://'):
                                # Уже в формате URL
                                self.proxies.append(line)
                        elif ' ' in line:
                            # Формат IP PORT LOGIN PASSWORD (через пробелы)
                            parts = line.split()
                            if len(parts) == 4:
                                ip, port, login, password = parts
                                proxy_url = f"http://{login}:{password}@{ip}:{port}"
                                self.proxies.append(proxy_url)
                        else:
                            # Уже в формате URL или непонятный формат - добавляем как есть
                            if line.startswith('http://') or line.startswith('https://'):
                                self.proxies.append(line)
        
        # Удаляем дубликаты
        self.proxies = list(dict.fromkeys(self.proxies))
        
        if not silent:
            if self.proxies:
                print(f"✓ Загружено {len(self.proxies)} прокси для ротации")
            else:
                print("⚠️  Прокси не загружены - запросы будут идти напрямую")
    
    def get_proxy(self, strategy: str = 'round_robin') -> Optional[Dict[str, str]]:
        """
        Получить прокси для запроса
        
        Args:
            strategy: Стратегия выбора ('round_robin', 'random')
            
        Returns:
            Словарь для requests: {'http': 'http://...', 'https': 'http://...'}
            или None если прокси нет
        """
        if not self.proxies:
            return None
        
        # Если прокси только один - всегда возвращаем его (не ротируем)
        if len(self.proxies) == 1:
            proxy_url = self.proxies[0]
        # Выбираем прокси по стратегии
        elif strategy == 'random':
            proxy_url = random.choice(self.proxies)
        else:  # round_robin
            proxy_url = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
        
        # Формируем словарь для requests
        return {
            'http': proxy_url,
            'https': proxy_url
        }
    
    def get_proxy_count(self) -> int:
        """Получить количество доступных прокси"""
        return len(self.proxies)
    
    def add_proxy(self, proxy: str):
        """Добавить прокси"""
        if proxy and proxy not in self.proxies:
            self.proxies.append(proxy)
    
    def remove_proxy(self, proxy: str):
        """Удалить прокси"""
        if proxy in self.proxies:
            self.proxies.remove(proxy)
            # Корректируем индекс если нужно
            if self.current_index >= len(self.proxies) and self.proxies:
                self.current_index = 0
