"""
SERP Cache Module
Файловый кэш для XML ответов от xmlstock (резервный вариант)
"""

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any


class SERPCache:
    """Файловый кэш для SERP результатов"""
    
    def __init__(self, cache_dir: Path = None, ttl_days: int = 30):
        if cache_dir is None:
            cache_dir = Path("output/serp_cache")
        
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_days = ttl_days
    
    def _get_cache_key(self, query: str, lr: int) -> str:
        """Генерация ключа кэша"""
        combined = f"{query.lower().strip()}_{lr}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """Путь к файлу кэша"""
        # Создаем подпапки для лучшей организации (первые 2 символа хэша)
        subdir = cache_key[:2]
        cache_subdir = self.cache_dir / subdir
        cache_subdir.mkdir(exist_ok=True)
        return cache_subdir / f"{cache_key}.json"
    
    def get(self, query: str, lr: int = 213) -> Optional[Dict[str, Any]]:
        """
        Получить данные из кэша
        
        Returns:
            Dict с данными или None если не найдено/устарело
        """
        cache_key = self._get_cache_key(query, lr)
        cache_path = self._get_cache_path(cache_key)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Проверяем TTL
            cached_at = datetime.fromisoformat(data['cached_at'])
            age = datetime.now() - cached_at
            
            if age.days > self.ttl_days:
                # Кэш устарел
                cache_path.unlink()
                return None
            
            return data
        
        except (json.JSONDecodeError, KeyError, ValueError):
            # Поврежденный файл кэша
            if cache_path.exists():
                cache_path.unlink()
            return None
    
    def set(self, query: str, lr: int, data: Dict[str, Any]):
        """Сохранить данные в кэш"""
        cache_key = self._get_cache_key(query, lr)
        cache_path = self._get_cache_path(cache_key)
        
        cache_data = {
            'query': query,
            'lr': lr,
            'cached_at': datetime.now().isoformat(),
            'data': data
        }
        
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
    
    def clear_expired(self) -> int:
        """Удалить устаревшие файлы кэша"""
        deleted = 0
        cutoff = datetime.now() - timedelta(days=self.ttl_days)
        
        for cache_file in self.cache_dir.rglob("*.json"):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                cached_at = datetime.fromisoformat(data['cached_at'])
                
                if cached_at < cutoff:
                    cache_file.unlink()
                    deleted += 1
            
            except (json.JSONDecodeError, KeyError, ValueError, FileNotFoundError):
                # Удаляем поврежденные файлы
                if cache_file.exists():
                    cache_file.unlink()
                    deleted += 1
        
        return deleted
    
    def clear_all(self) -> int:
        """Очистить весь кэш"""
        deleted = 0
        for cache_file in self.cache_dir.rglob("*.json"):
            cache_file.unlink()
            deleted += 1
        return deleted
    
    def get_statistics(self) -> Dict[str, Any]:
        """Статистика кэша"""
        cache_files = list(self.cache_dir.rglob("*.json"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        return {
            'total_files': len(cache_files),
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'cache_dir': str(self.cache_dir),
            'ttl_days': self.ttl_days
        }
