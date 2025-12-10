"""
Enhanced Domain Classifier
Классификатор с поддержкой глобальной БД для мультигрупповой работы
"""

from pathlib import Path
from typing import Set, Tuple, Optional, Dict, Any
import re
import sqlite3

from .query_groups import GroupDatabaseManager


class EnhancedDomainClassifier:
    """
    Улучшенный классификатор доменов с глобальной БД
    
    Источники классификации (приоритет):
    1. Глобальная БД (общая статистика по всем группам)
    2. БД текущей группы (domain_stats)
    3. TXT файлы (ручная классификация)
    """
    
    def __init__(
        self,
        domains_dir: Path = None,
        group_db_path: Optional[Path] = None,
        use_global_db: bool = True,
        min_confidence: float = 0.5
    ):
        """
        Инициализация классификатора
        
        Args:
            domains_dir: Путь к директории с файлами доменов
            group_db_path: Путь к БД группы
            use_global_db: Использовать глобальную БД
            min_confidence: Минимальный порог уверенности
        """
        if domains_dir is None:
            domains_dir = Path("keywords_settings")
        
        self.domains_dir = Path(domains_dir)
        self.group_db_path = group_db_path
        self.use_global_db = use_global_db
        self.min_confidence = min_confidence
        
        # Кэш классификаций
        self.global_cache: Dict[str, Dict[str, Any]] = {}
        self.group_cache: Dict[str, Dict[str, Any]] = {}
        
        # Загружаем данные
        if use_global_db:
            self._load_global_classifications()
        
        if group_db_path and group_db_path.exists():
            self._load_group_classifications()
        
        # Загружаем txt файлы
        self.commercial_domains = self._load_domains('commercial_domains.txt')
        self.informational_domains = self._load_domains('informational_domains.txt')
        
        print(f"✓ Классификатор доменов инициализирован:")
        print(f"  - Глобальная БД: {len(self.global_cache)} доменов")
        print(f"  - БД группы: {len(self.group_cache)} доменов")
        print(f"  - TXT коммерческие: {len(self.commercial_domains)}")
        print(f"  - TXT информационные: {len(self.informational_domains)}")
    
    def _load_global_classifications(self):
        """Загрузить классификации из глобальной БД"""
        global_db_path = GroupDatabaseManager.GLOBAL_DB_PATH
        
        if not global_db_path.exists():
            return
        
        try:
            with sqlite3.connect(global_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        domain,
                        is_commercial,
                        commercial_ratio,
                        confidence_score,
                        total_queries,
                        groups_count
                    FROM domain_aggregated_stats
                    WHERE confidence_score >= ?
                """, (self.min_confidence,))
                
                for row in cursor.fetchall():
                    self.global_cache[row['domain'].lower()] = dict(row)
        
        except Exception as e:
            print(f"⚠️  Ошибка загрузки глобальной БД: {e}")
    
    def _load_group_classifications(self):
        """Загрузить классификации из БД группы"""
        if not self.group_db_path or not self.group_db_path.exists():
            return
        
        try:
            with sqlite3.connect(self.group_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Проверяем существование таблицы
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='domain_stats'
                """)
                
                if not cursor.fetchone():
                    return
                
                # Загружаем классификацию
                cursor.execute("""
                    SELECT 
                        domain, 
                        classification, 
                        confidence
                    FROM domain_stats
                    WHERE classification IN ('commercial', 'informational')
                """)
                
                for row in cursor.fetchall():
                    self.group_cache[row['domain'].lower()] = dict(row)
        
        except Exception as e:
            print(f"⚠️  Ошибка загрузки БД группы: {e}")
    
    def _load_domains(self, filename: str) -> Set[str]:
        """Загрузить домены из TXT файла"""
        file_path = self.domains_dir / filename
        domains = set()
        
        if not file_path.exists():
            return domains
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    domains.add(line.lower())
        
        return domains
    
    def classify_domain(self, domain: str) -> str:
        """
        Классифицировать домен
        
        Приоритет:
        1. Глобальная БД (наиболее надежно)
        2. БД группы
        3. TXT файлы
        
        Returns:
            'commercial' | 'informational' | 'unknown'
        """
        domain_clean = self._normalize_domain(domain)
        
        # 1. Проверяем глобальную БД
        if self.use_global_db and domain_clean in self.global_cache:
            data = self.global_cache[domain_clean]
            return 'commercial' if data['is_commercial'] else 'informational'
        
        # 2. Проверяем БД группы
        if domain_clean in self.group_cache:
            return self.group_cache[domain_clean]['classification']
        
        # 3. Проверяем TXT файлы
        if domain_clean in self.commercial_domains:
            return 'commercial'
        elif domain_clean in self.informational_domains:
            return 'informational'
        
        return 'unknown'
    
    def get_classification_info(self, domain: str) -> Dict[str, Any]:
        """
        Получить подробную информацию о классификации
        
        Returns:
            Dict с данными классификации или None
        """
        domain_clean = self._normalize_domain(domain)
        
        # Глобальная БД
        if domain_clean in self.global_cache:
            data = self.global_cache[domain_clean]
            return {
                'domain': domain_clean,
                'classification': 'commercial' if data['is_commercial'] else 'informational',
                'source': 'global_db',
                'confidence': data['confidence_score'],
                'total_queries': data['total_queries'],
                'groups_count': data['groups_count'],
                'commercial_ratio': data['commercial_ratio']
            }
        
        # БД группы
        if domain_clean in self.group_cache:
            data = self.group_cache[domain_clean]
            return {
                'domain': domain_clean,
                'classification': data['classification'],
                'source': 'group_db',
                'confidence': data['confidence']
            }
        
        # TXT файлы
        if domain_clean in self.commercial_domains:
            return {
                'domain': domain_clean,
                'classification': 'commercial',
                'source': 'txt_file',
                'confidence': 1.0
            }
        elif domain_clean in self.informational_domains:
            return {
                'domain': domain_clean,
                'classification': 'informational',
                'source': 'txt_file',
                'confidence': 1.0
            }
        
        return {
            'domain': domain_clean,
            'classification': 'unknown',
            'source': None,
            'confidence': 0.0
        }
    
    def _normalize_domain(self, domain: str) -> str:
        """Нормализовать домен"""
        domain = domain.lower().strip()
        domain = re.sub(r'^https?://', '', domain)
        domain = domain.split('/')[0]
        domain = re.sub(r'^www\.', '', domain)
        return domain

