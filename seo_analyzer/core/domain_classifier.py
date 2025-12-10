"""
Классификация доменов для определения интента запроса.

Загружает классификацию доменов из:
1. БД (domain_stats) - автоматическая классификация по offer_info
2. txt файлов (commercial_domains.txt, informational_domains.txt) - ручная классификация

Приоритет: БД > txt файлы
"""
from pathlib import Path
from typing import Set, Tuple, Optional
import re
import sqlite3

from seo_analyzer.core.number_formatter import round_float


class DomainClassifier:
    """Классификатор доменов для определения коммерческого/информационного интента"""
    
    def __init__(self, domains_dir: Path = None, db_path: Optional[Path] = None, use_db: bool = True):
        """
        Инициализация классификатора.
        
        Args:
            domains_dir: Путь к директории с файлами доменов (по умолчанию keywords_settings/)
            db_path: Путь к БД с domain_stats (по умолчанию output/master_queries.db)
            use_db: Использовать БД для классификации (приоритет над txt файлами)
        """
        if domains_dir is None:
            domains_dir = Path(__file__).parent.parent.parent / 'keywords_settings'
        
        if db_path is None:
            db_path = Path(__file__).parent.parent.parent / 'output' / 'master_queries.db'
        
        self.domains_dir = Path(domains_dir)
        self.db_path = Path(db_path)
        self.use_db = use_db
        
        # Загружаем домены из БД (если есть)
        self.db_domains = {}
        if use_db and self.db_path.exists():
            self.db_domains = self._load_domains_from_db()
            if self.db_domains:
                print(f"✓ Загружено из БД: {len(self.db_domains)} доменов")
        
        # Загружаем домены из txt файлов
        self.commercial_domains = self._load_domains('commercial_domains.txt')
        self.informational_domains = self._load_domains('informational_domains.txt')
        
        print(f"✓ Загружено из txt коммерческих: {len(self.commercial_domains)}")
        print(f"✓ Загружено из txt информационных: {len(self.informational_domains)}")
    
    def _load_domains_from_db(self) -> dict:
        """
        Загружает классификацию доменов из БД (domain_stats).
        
        Returns:
            Словарь {domain: {'classification': str, 'confidence': float}}
        """
        domains = {}
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            # Проверяем существование таблицы
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='domain_stats'
            """)
            
            if not cursor.fetchone():
                conn.close()
                return domains
            
            # Загружаем домены
            cursor.execute("""
                SELECT domain, classification, confidence, offer_info_count, total_documents
                FROM domain_stats
                WHERE classification IN ('commercial', 'informational')
            """)
            
            for domain, classification, confidence, offers, total in cursor.fetchall():
                domains[domain.lower()] = {
                    'classification': classification,
                    'confidence': confidence,
                    'offers': offers,
                    'total': total
                }
            
            conn.close()
            
        except Exception as e:
            print(f"⚠️ Ошибка загрузки доменов из БД: {e}")
        
        return domains
    
    def _load_domains(self, filename: str) -> Set[str]:
        """
        Загружает домены из txt файла.
        
        Args:
            filename: Имя файла
            
        Returns:
            Множество доменов
        """
        domains = set()
        file_path = self.domains_dir / filename
        
        if not file_path.exists():
            print(f"⚠️ Файл {filename} не найден: {file_path}")
            return domains
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    
                    # Пропускаем комментарии и пустые строки
                    if not line or line.startswith('#'):
                        continue
                    
                    # Убираем комментарии в конце строки
                    domain = line.split('#')[0].strip()
                    
                    # Убираем счетчики (например "domain.com (123)")
                    domain = re.sub(r'\s*\(\d+\)', '', domain).strip()
                    
                    if domain:
                        domains.add(domain.lower())
            
            return domains
            
        except Exception as e:
            print(f"⚠️ Ошибка при загрузке {filename}: {e}")
            return domains
    
    def extract_domain(self, url: str) -> str:
        """
        Извлекает домен из URL.
        
        Args:
            url: Полный URL
            
        Returns:
            Домен (без www)
        """
        try:
            # Убираем протокол
            url = re.sub(r'^https?://', '', url)
            
            # Берем до первого /
            domain = url.split('/')[0]
            
            # Убираем www
            domain = re.sub(r'^www\.', '', domain)
            
            return domain.lower()
        except:
            return ""
    
    def classify_domain(self, domain: str) -> str:
        """
        Классифицирует домен.
        
        Приоритет:
        1. БД (domain_stats) - автоматическая классификация по offer_info
        2. txt файлы - ручная классификация
        
        Args:
            domain: Домен для классификации (может быть с www.)
            
        Returns:
            'commercial' | 'informational' | 'unknown'
        """
        domain_clean = domain.lower()
        
        # Убираем www. если есть
        domain_clean = re.sub(r'^www\.', '', domain_clean)
        
        # Приоритет 1: Проверяем БД
        if self.use_db and domain_clean in self.db_domains:
            return self.db_domains[domain_clean]['classification']
        
        # Приоритет 2: Проверяем txt файлы
        if domain_clean in self.commercial_domains:
            return 'commercial'
        elif domain_clean in self.informational_domains:
            return 'informational'
        else:
            return 'unknown'
    
    def analyze_serp_urls(self, urls: list) -> Tuple[float, float]:
        """
        Анализирует список URL из SERP.
        
        Args:
            urls: Список URL из выдачи
            
        Returns:
            (commercial_ratio, informational_ratio) - доли коммерческих и информационных доменов
        """
        if not urls:
            return 0.0, 0.0
        
        commercial_count = 0
        informational_count = 0
        total_count = 0
        
        for url in urls[:10]:  # Анализируем топ-10
            if not url:
                continue
            
            domain = self.extract_domain(url)
            if not domain:
                continue
            
            total_count += 1
            classification = self.classify_domain(domain)
            
            if classification == 'commercial':
                commercial_count += 1
            elif classification == 'informational':
                informational_count += 1
        
        if total_count == 0:
            return 0.0, 0.0
        
        commercial_ratio = round_float(commercial_count / total_count)
        informational_ratio = round_float(informational_count / total_count)
        
        return commercial_ratio, informational_ratio
    
    def suggest_intent_by_serp(
        self,
        urls: list,
        threshold: float = 0.5
    ) -> Tuple[str, float]:
        """
        Предлагает интент на основе анализа SERP.
        
        Args:
            urls: Список URL из выдачи
            threshold: Порог для определения интента (по умолчанию 0.5 = 50%)
            
        Returns:
            (intent, confidence) - интент и уверенность (0-1)
        """
        commercial_ratio, informational_ratio = self.analyze_serp_urls(urls)
        
        if commercial_ratio >= threshold:
            return 'commercial', commercial_ratio
        elif informational_ratio >= threshold:
            return 'informational', informational_ratio
        else:
            return 'unknown', max(commercial_ratio, informational_ratio)
    
    def get_domain_stats(self) -> dict:
        """Возвращает статистику загруженных доменов"""
        db_commercial = sum(1 for d in self.db_domains.values() if d['classification'] == 'commercial')
        db_informational = sum(1 for d in self.db_domains.values() if d['classification'] == 'informational')
        
        return {
            'db_commercial': db_commercial,
            'db_informational': db_informational,
            'txt_commercial': len(self.commercial_domains),
            'txt_informational': len(self.informational_domains),
            'total_count': len(self.db_domains) + len(self.commercial_domains) + len(self.informational_domains)
        }

