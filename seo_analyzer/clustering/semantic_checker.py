"""
УНИВЕРСАЛЬНЫЙ семантический анализатор для разделения кластеров.

Проверяет ТОЛЬКО географическую привязку через Natasha NER.
Это единственный универсальный критерий, работающий для ЛЮБОЙ ниши.

Примеры:
- "купить квартиру москва" vs "купить квартиру спб" → разные кластеры
- "доставка пиццы казань" vs "доставка пиццы краснодар" → разные кластеры
- "скуд санкт-петербург" vs "скуд москва" → разные кластеры

УДАЛЕНО (хардкод):
- Типы продуктов (было захардкожено для СКУД)
- Маркеры интентов (субъективно и не универсально)

Философия: Инструмент должен работать для ЛЮБОЙ ниши без настройки!
"""
import re
from typing import Optional, Set, Dict, Tuple
from functools import lru_cache

from .geo_validator import GeoValidator


class SemanticClusterChecker:
    """
    УНИВЕРСАЛЬНЫЙ чекер совместимости запросов для кластеризации.
    
    Работает ТОЛЬКО с географией (Natasha NER).
    Никакого хардкода типов продуктов или интентов!
    
    Подходит для любой ниши:
    - E-commerce, B2B, услуги, недвижимость, рестораны, и т.д.
    """
    
    def __init__(self, geo_dicts: Dict[str, Set[str]] = None):
        """
        Args:
            geo_dicts: Словари с географическими названиями для валидации Natasha результатов
        """
        self.geo_dicts = geo_dicts or {}
        
        # Валидатор географических названий
        self.geo_validator = GeoValidator(geo_dicts)
        
        # Инициализируем Natasha для NER (LOC)
        self._init_natasha_ner()
        
        # УДАЛЕНО: Хардкод типов продуктов (было только для СКУД)
        # Для универсального инструмента проверяем только географию через Natasha
    
    def _init_natasha_ner(self):
        """Инициализация Natasha NER для определения локаций"""
        try:
            from natasha import (
                Segmenter,
                NewsEmbedding,
                NewsNERTagger,
                Doc,
                LOC
            )
            
            self.segmenter = Segmenter()
            self.emb = NewsEmbedding()
            self.ner_tagger = NewsNERTagger(self.emb)
            self.LOC = LOC
            self.natasha_enabled = True
        except Exception as e:
            # Fallback на старую систему с regex
            self.natasha_enabled = False
            self._compile_geo_patterns()  # Используем старый метод
    
    
    def _compile_geo_patterns(self):
        """Компилирует паттерны для поиска городов (FALLBACK, если Natasha недоступна)"""
        self.geo_patterns = {}
        self.city_to_base = {}  # Маппинг любой формы → базовая форма
        
        # Russian cities
        russian_cities = self.geo_dicts.get('Russian', set())
        if russian_cities:
            all_forms = []
            
            for city in russian_cities:
                base_form = city.lower()
                forms = self._generate_city_forms(city)
                
                # Добавляем в маппинг все формы
                for form in forms:
                    self.city_to_base[form] = base_form
                    all_forms.append(form)
            
            # Сортируем по длине (длинные первыми, чтобы избежать коллизий)
            sorted_forms = sorted(set(all_forms), key=len, reverse=True)
            escaped = [re.escape(form) for form in sorted_forms]
            pattern = r'\b(' + '|'.join(escaped) + r')\b'
            self.geo_patterns['russian'] = re.compile(pattern, re.IGNORECASE)
        
        # Moscow variants (города МО)
        moscow_words = self.geo_dicts.get('Moscow', set())
        if moscow_words:
            all_forms = []
            
            for city in moscow_words:
                base_form = city.lower()
                forms = self._generate_city_forms(city)
                
                for form in forms:
                    self.city_to_base[form] = base_form
                    all_forms.append(form)
            
            sorted_forms = sorted(set(all_forms), key=len, reverse=True)
            escaped = [re.escape(form) for form in sorted_forms]
            pattern = r'\b(' + '|'.join(escaped) + r')\b'
            self.geo_patterns['moscow'] = re.compile(pattern, re.IGNORECASE)
    
    # УДАЛЕНО: Хардкод типов продуктов и интентов
    # Это было специфично для СКУД, но инструмент должен быть универсальным
    def _capitalize_known_cities(self, query: str) -> str:
        """
        Капитализирует известные города в запросе для корректной работы Natasha NER.
        
        Natasha NER требует заглавную букву для распознавания Named Entities.
        Этот метод находит известные города (из словаря) и капитализирует их.
        
        Args:
            query: Исходный запрос (может быть в lowercase)
            
        Returns:
            Запрос с капитализированными городами
        """
        words = query.split()
        result_words = []
        
        for word in words:
            word_lower = word.lower()
            
            # Проверяем, является ли слово известным городом
            if self.geo_validator.is_valid_city(word_lower):
                # Получаем базовую форму и капитализируем её
                base_form = self.geo_validator.normalize_city(word_lower)
                result_words.append(base_form.capitalize())
            else:
                # Оставляем как есть
                result_words.append(word)
        
        return ' '.join(result_words)
    
    # Оставляем только географию (Natasha NER) - это универсально для любой ниши
    @lru_cache(maxsize=10000)
    def extract_geo(self, query: str) -> Optional[str]:
        """
        Извлекает географическую привязку из запроса через Natasha NER.
        
        Natasha автоматически распознает города во ВСЕХ падежах:
        - "москва" → "москва"
        - "в москве" → "москва"  
        - "для москвы" → "москва"
        - "о москве" → "москва"
        - "санкт-петербург" → "санкт-петербург"
        - "в петербурге" → "санкт-петербург"
        - "скуд в школе москва" → "москва" (отфильтрует "школе")
        
        Returns:
            Название города в базовой форме (нижний регистр) или None
        """
        if self.natasha_enabled:
            # Используем Natasha NER (быстро и точно!)
            try:
                from natasha import Doc
                
                # Капитализируем известные города для Natasha
                query_capitalized = self._capitalize_known_cities(query)
                
                doc = Doc(query_capitalized)
                doc.segment(self.segmenter)
                doc.tag_ner(self.ner_tagger)
                
                # Извлекаем все LOC (локации) и фильтруем по словарю городов
                for span in doc.spans:
                    if span.type == self.LOC:
                        location_lower = span.text.lower()
                        
                        # Проверяем, является ли это валидным городом
                        if self.geo_validator.is_valid_city(location_lower):
                            # Нормализуем к базовой форме
                            base_city = self.geo_validator.normalize_city(location_lower)
                            return base_city
                
                return None
            except Exception:
                # Если ошибка - fallback на regex
                pass
        
        # Fallback: старый метод через regex (если Natasha недоступна)
        query_lower = query.lower()
        
        # Приоритет 1: Russian cities (включая Москву, СПб и т.д.)
        if hasattr(self, 'geo_patterns') and 'russian' in self.geo_patterns:
            match = self.geo_patterns['russian'].search(query_lower)
            if match:
                found_form = match.group(1).lower()
                # Возвращаем базовую форму города через маппинг
                return self.city_to_base.get(found_form, found_form)
        
        # Приоритет 2: Moscow region cities (города МО)
        if hasattr(self, 'geo_patterns') and 'moscow' in self.geo_patterns:
            match = self.geo_patterns['moscow'].search(query_lower)
            if match:
                found_form = match.group(1).lower()
                return self.city_to_base.get(found_form, found_form)
        
        return None
    
    # УДАЛЕНО: extract_product_type() и extract_intent_markers()
    # Это был хардкод для конкретной ниши (СКУД)
    # Универсальный инструмент не должен знать про конкретные продукты
    def are_queries_compatible(
        self, 
        query1: str, 
        query2: str,
        check_geo: bool = True
    ) -> Tuple[bool, str]:
        """
        Проверяет совместимость двух запросов для одного кластера.
        
        УНИВЕРСАЛЬНАЯ ПРОВЕРКА - только география (через Natasha NER).
        Никакого хардкода типов продуктов или интентов.
        
        Args:
            query1: Первый запрос
            query2: Второй запрос
            check_geo: Проверять географию (default: True)
            
        Returns:
            (compatible, reason) - совместимы ли запросы и причина несовместимости
        """
        # Единственная проверка: Географическая привязка (универсально для всех ниш)
        if check_geo:
            geo1 = self.extract_geo(query1)
            geo2 = self.extract_geo(query2)
            
            # Если оба запроса имеют гео, но разные города - разделяем
            if geo1 and geo2 and geo1 != geo2:
                return False, f"Разные города: {geo1} vs {geo2}"
            
            # Если один с гео, другой без - тоже разделяем
            if (geo1 and not geo2) or (geo2 and not geo1):
                return False, f"Один с гео ({geo1 or geo2}), другой без"
        
        return True, "Совместимы"
    
    def should_separate_from_cluster(
        self,
        query: str,
        cluster_queries: list,
        check_geo: bool = True
    ) -> Tuple[bool, str]:
        """
        Проверяет нужно ли отделить запрос от кластера.
        
        УНИВЕРСАЛЬНАЯ ПРОВЕРКА - только география.
        
        Args:
            query: Проверяемый запрос
            cluster_queries: Запросы в кластере
            check_geo: Проверять географию
            
        Returns:
            (should_separate, reason) - нужно ли отделить и почему
        """
        if not cluster_queries:
            return False, "Пустой кластер"
        
        # Проверяем совместимость с каждым запросом в кластере
        incompatible_count = 0
        reasons = []
        
        for cluster_query in cluster_queries:
            compatible, reason = self.are_queries_compatible(
                query, cluster_query,
                check_geo=check_geo
            )
            
            if not compatible:
                incompatible_count += 1
                reasons.append(reason)
        
        # Если несовместим с большинством (>50%) - отделяем
        if incompatible_count > len(cluster_queries) * 0.5:
            return True, f"Несовместим с {incompatible_count}/{len(cluster_queries)}: {reasons[0]}"
        
        return False, "Совместим"

