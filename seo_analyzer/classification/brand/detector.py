"""
Brand Detector
Детектор брендов в запросах с использованием множественных эвристик
"""

import re
from typing import Dict, List, Set, Tuple
from collections import Counter
from tqdm import tqdm

from .analyzer import BrandAnalyzer
from .ner_enhancer import NERBrandEnhancer


class BrandDetector:
    """
    Детектор брендов с использованием множественных эвристик + NER.
    
    Комбинирует:
    - Эвристический анализ (капс, паттерны, морфология)
    - NER (Named Entity Recognition) через Natasha для извлечения ORG
    """
    
    def __init__(self, geo_dicts: Dict[str, Set[str]], commercial_words: Set[str] = None, info_words: Set[str] = None, use_ner: bool = True):
        """
        Инициализация детектора
        
        Args:
            geo_dicts: Географические словари для исключения
            commercial_words: Коммерческие слова из commercial.txt (не бренды)
            info_words: Информационные слова из info.txt (не бренды)
            use_ner: Использовать ли NER для дополнительной валидации (по умолчанию True)
        """
        self.geo_dicts = geo_dicts
        self.use_ner = use_ner
        
        # Собираем все географические термины для исключения
        geo_terms = set()
        for geo_dict in geo_dicts.values():
            geo_terms.update(w.lower() for w in geo_dict)
        
        # Служебные слова (не бренды) - теперь из файлов + базовые предлоги
        common_words = {
            'в', 'на', 'с', 'и', 'для', 'от', 'к', 'по', 'из', 'под', 'над',
            'система', 'устройство', 'оборудование', 'товар', 'услуга'
        }
        
        # Добавляем слова из commercial.txt и info.txt
        if commercial_words:
            common_words.update(w.lower() for w in commercial_words)
        if info_words:
            common_words.update(w.lower() for w in info_words)
        
        # Инициализируем анализатор
        self.analyzer = BrandAnalyzer(geo_terms, common_words)
        
        # Инициализируем NER enhancer
        if self.use_ner:
            try:
                self.ner_enhancer = NERBrandEnhancer()
            except Exception as e:
                # Если NER не удалось инициализировать, отключаем его
                print(f"⚠️  NER enhancer недоступен: {e}")
                self.use_ner = False
                self.ner_enhancer = None
        else:
            self.ner_enhancer = None
        
        # Счетчик встречаемости для статистического анализа
        self.brand_counter = Counter()
    
    def detect_brands_in_query(self, query: str) -> Dict[str, any]:
        """
        Определяет бренды в запросе.
        
        Алгоритм:
        1. Эвристический анализ слов (капс, паттерны)
        2. NER проверка через Natasha (извлечение ORG)
        3. Повышение confidence если NER подтвердил бренд
        """
        words = query.split()
        detected_brands = []
        max_confidence = 0.0
        detection_reasons = []
        
        # Шаг 1: Эвристический анализ
        for word in words:
            is_brand, confidence, reason = self.analyzer.is_likely_brand(word)
            
            if is_brand:
                detected_brands.append({
                    'brand': word,
                    'confidence': confidence,
                    'reason': reason,
                    'ner_validated': False,  # Будет обновлено ниже
                })
                
                if confidence > max_confidence:
                    max_confidence = confidence
                
                detection_reasons.append(reason)
                self.brand_counter[word.lower()] += 1
        
        # Шаг 2: NER валидация и повышение confidence
        if self.use_ner and self.ner_enhancer and detected_brands:
            try:
                # Извлекаем организации через NER
                ner_organizations = self.ner_enhancer.extract_organizations(query)
                
                # Проверяем каждый найденный бренд
                for brand_dict in detected_brands:
                    brand_lower = brand_dict['brand'].lower()
                    
                    # Проверяем, подтверждается ли бренд через NER
                    ner_confirmed = any(
                        brand_lower in org or org in brand_lower
                        for org in ner_organizations
                    )
                    
                    if ner_confirmed:
                        # Повышаем confidence на 0.2 (но не выше 1.0)
                        old_confidence = brand_dict['confidence']
                        brand_dict['confidence'] = min(1.0, old_confidence + 0.2)
                        brand_dict['reason'] += ' + NER:ORG'
                        brand_dict['ner_validated'] = True
                        
                        # Обновляем max_confidence если нужно
                        if brand_dict['confidence'] > max_confidence:
                            max_confidence = brand_dict['confidence']
                
                # Добавляем бренды, найденные ТОЛЬКО через NER (пропущенные эвристикой)
                existing_brands_lower = {b['brand'].lower() for b in detected_brands}
                for org in ner_organizations:
                    # Проверяем, не был ли уже найден эвристикой
                    if org not in existing_brands_lower:
                        # Добавляем с базовым confidence 0.6
                        detected_brands.append({
                            'brand': org,
                            'confidence': 0.6,
                            'reason': 'NER:ORG',
                            'ner_validated': True,
                        })
                        detection_reasons.append('NER:ORG')
                        self.brand_counter[org] += 1
                        
                        if 0.6 > max_confidence:
                            max_confidence = 0.6
            
            except Exception as e:
                # Если NER не сработал, продолжаем без него
                pass
        
        return {
            'has_brand': len(detected_brands) > 0,
            'brands': detected_brands,
            'brand_count': len(detected_brands),
            'max_confidence': max_confidence,
            'detected_brand': detected_brands[0]['brand'] if detected_brands else None,
            'brand_confidence': max_confidence,
            'is_brand_query': max_confidence >= 0.7,
            'detection_reasons': detection_reasons,
            'ner_used': self.use_ner and self.ner_enhancer is not None,
        }
    
    def extract_main_brand(self, query: str) -> Tuple[str, float]:
        """Извлекает основной бренд из запроса"""
        brand_info = self.detect_brands_in_query(query)
        
        if brand_info['brands']:
            best_brand = max(brand_info['brands'], key=lambda x: x['confidence'])
            return best_brand['brand'], best_brand['confidence']
        
        return None, 0.0
    
    def get_top_brands(self, top_n: int = 50) -> List[Tuple[str, int]]:
        """Возвращает топ найденных брендов"""
        return self.brand_counter.most_common(top_n)
    
    def classify_brand_query_type(self, query: str, commercial_words: Set[str] = None, info_words: Set[str] = None) -> str:
        """
        Классифицирует тип брендового запроса
        
        Args:
            query: Запрос
            commercial_words: Коммерческие слова из commercial.txt
            info_words: Информационные слова из info.txt
        """
        query_lower = query.lower()
        brand_info = self.detect_brands_in_query(query)
        
        if not brand_info['has_brand']:
            return "non_brand"
        
        # Брендовые + коммерческие (используем слова из файла)
        if commercial_words and any(word.lower() in query_lower for word in commercial_words):
            return "brand_commercial"
        
        # Брендовые + информационные (используем слова из файла)
        if info_words and any(word.lower() in query_lower for word in info_words):
            return "brand_info"
        
        # Брендовые + навигационные (базовые паттерны)
        if any(word in query_lower for word in ['сайт', 'официальный', 'контакты']):
            return "brand_navigational"
        
        # Модельные запросы
        if re.search(r'\d+', query):
            return "brand_model"
        
        # Чисто брендовой запрос
        return "brand_pure"
    
    def detect_batch(self, queries: List[str]) -> List[Dict[str, any]]:
        """Пакетное определение брендов с прогресс-баром"""
        results = []
        
        show_progress = len(queries) >= 100
        iterator = tqdm(queries, desc="Определение брендов", disable=not show_progress)
        
        for query in iterator:
            brand_info = self.detect_brands_in_query(query)
            brand_type = self.classify_brand_query_type(query)
            
            result = {
                'query': query,
                **brand_info,
                'brand_query_type': brand_type,
            }
            
            results.append(result)
        
        return results
    
    def get_statistics(self) -> Dict[str, any]:
        """Возвращает статистику детектора"""
        return {
            'total_brands_detected': len(self.brand_counter),
            'total_mentions': sum(self.brand_counter.values()),
            'top_brands': self.get_top_brands(20),
        }

