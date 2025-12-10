"""
Structure Clusterer
Кластеризация запросов по структурным паттернам и модификаторам
"""

from typing import Dict, List
from collections import Counter, defaultdict
import pandas as pd

from .pattern_detector import PatternDetector


class StructureClusterer:
    """Кластеризатор по структурным паттернам запросов"""
    
    def __init__(self):
        """Инициализация кластеризатора"""
        self.pattern_detector = PatternDetector()
    
    def detect_pattern(self, query: str) -> tuple:
        """Определяет структурный паттерн запроса"""
        return self.pattern_detector.detect_pattern(query)
    
    def analyze_structure(self, query: str) -> Dict:
        """Детальный анализ структуры запроса"""
        return self.pattern_detector.analyze_structure(query)
    
    def cluster_by_structure(self, queries: List[str]) -> Dict[str, List[str]]:
        """Группирует запросы по структурным паттернам"""
        clusters = defaultdict(list)
        
        for query in queries:
            _, pattern_name = self.detect_pattern(query)
            clusters[pattern_name].append(query)
        
        return dict(clusters)
    
    def get_pattern_distribution(self, queries: List[str]) -> Dict[str, int]:
        """Возвращает распределение запросов по паттернам"""
        pattern_counter = Counter()
        
        for query in queries:
            _, pattern_name = self.detect_pattern(query)
            pattern_counter[pattern_name] += 1
        
        return dict(pattern_counter)
    
    def extract_structural_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Извлекает структурные признаки для DataFrame"""
        print("🔄 Анализ структуры запросов...")
        
        structural_info = df['keyword'].apply(self.analyze_structure)
        
        for key in ['query_pattern', 'has_action', 'has_question', 'has_price', 
                    'has_comparison', 'has_modifier']:
            df[key] = structural_info.apply(lambda x: x.get(key))
        
        print("✓ Структурный анализ завершен")
        return df
    
    def get_pattern_summary(self, queries: List[str]) -> Dict[str, Dict]:
        """Возвращает детальную сводку по паттернам"""
        clusters = self.cluster_by_structure(queries)
        
        summary = {}
        for pattern_name, pattern_queries in clusters.items():
            summary[pattern_name] = {
                'pattern': pattern_name,
                'count': len(pattern_queries),
                'percentage': len(pattern_queries) / len(queries) * 100,
                'examples': pattern_queries[:5],
            }
        
        return summary


class ModifierClusterer:
    """Кластеризатор по модификаторам"""
    
    def __init__(self):
        """Инициализация"""
        self.modifiers = {
            'price': {
                'words': ['дешево', 'дорого', 'недорого', 'цена', 'стоимость', 'премиум', 'бюджетн'],
                'type': 'price',
            },
            'quality': {
                'words': ['лучший', 'качественный', 'надежный', 'проверенный', 'профессиональн'],
                'type': 'quality',
            },
            'time': {
                'words': ['срочно', 'быстро', 'экспресс', 'немедленно', 'сегодня', 'завтра'],
                'type': 'time',
            },
            'location': {
                'words': ['рядом', 'около', 'близко', 'метро', 'район'],
                'type': 'location',
            },
            'format': {
                'words': ['опт', 'оптом', 'розница', 'б/у', 'новый', 'подержанн'],
                'type': 'format',
            },
        }
    
    def detect_modifiers(self, query: str) -> List[str]:
        """Определяет модификаторы в запросе"""
        query_lower = query.lower()
        detected = []
        
        for modifier_type, modifier_info in self.modifiers.items():
            for word in modifier_info['words']:
                if word in query_lower:
                    detected.append(modifier_type)
                    break
        
        return detected
    
    def cluster_by_modifiers(self, queries: List[str]) -> Dict[str, List[str]]:
        """Группирует запросы по модификаторам"""
        clusters = defaultdict(list)
        
        for query in queries:
            modifiers = self.detect_modifiers(query)
            
            if not modifiers:
                clusters['no_modifier'].append(query)
            else:
                for modifier in modifiers:
                    clusters[modifier].append(query)
        
        return dict(clusters)
    
    def get_modifier_distribution(self, queries: List[str]) -> Dict[str, int]:
        """Возвращает распределение по модификаторам"""
        counter = Counter()
        
        for query in queries:
            modifiers = self.detect_modifiers(query)
            
            if not modifiers:
                counter['no_modifier'] += 1
            else:
                for modifier in modifiers:
                    counter[modifier] += 1
        
        return dict(counter)



