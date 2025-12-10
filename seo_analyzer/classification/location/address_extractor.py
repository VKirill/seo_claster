"""
Address Extractor
Извлечение адресов (улицы, дома) из запросов через Natasha AddrExtractor + regex паттерны
"""

from typing import Dict, Optional, Any
from functools import lru_cache
import re


class AddressExtractor:
    """
    Извлекает адреса из запросов используя Natasha AddrExtractor.
    
    Распознает:
    - Улицы (ул. Мира, проспект Победы)
    - Номера домов (52, 10а, 93б)
    - Корпуса (к.1, корп. 2)
    
    Examples:
        >>> extractor = AddressExtractor()
        >>> extractor.extract("домофон мира 52")
        {'has_address': True, 'street': 'мира', 'house': '52'}
    """
    
    def __init__(self):
        """Инициализация с ленивой загрузкой Natasha"""
        self._addr_extractor = None
        self._morph_vocab = None
        self._natasha_available = True
        self._init_error = None
        
        # Инициализируем при первом использовании
        self._initialize_natasha()
        
        # Регулярные выражения для простых адресов (улица + дом)
        self._compile_address_patterns()
    
    def _initialize_natasha(self):
        """Ленивая инициализация Natasha компонентов"""
        try:
            from natasha import MorphVocab, AddrExtractor
            
            self._morph_vocab = MorphVocab()
            self._addr_extractor = AddrExtractor(self._morph_vocab)
            self._natasha_available = True
            
        except ImportError as e:
            self._natasha_available = False
            self._init_error = f"Natasha not available: {e}"
        except Exception as e:
            self._natasha_available = False
            self._init_error = f"Failed to initialize Natasha: {e}"
    
    def _compile_address_patterns(self):
        """Компилирует regex паттерны для поиска простых адресов"""
        # Паттерн: (улица|ул.|проспект|пр.) НАЗВАНИЕ НОМЕР_ДОМА
        self._street_patterns = [
            # С явным указанием типа: "ул. мира 52", "проспект победы 93б"
            re.compile(
                r'\b(?:улица|ул\.?|проспект|пр\.?|переулок|пер\.?|бульвар|б-р|б\.)\s+'
                r'([а-яё\s\-]+?)\s+'
                r'(\d+[а-яa-z]?)\b',
                re.IGNORECASE
            ),
            # Без указания типа: "мира 52", "ленина 10а"
            re.compile(
                r'\b([а-яё]+)\s+(\d+[а-яa-z]?)\b',
                re.IGNORECASE
            ),
        ]
    
    @lru_cache(maxsize=10000)
    def extract(self, query: str) -> Dict[str, Any]:
        """
        Извлекает адрес из запроса.
        
        Args:
            query: Поисковый запрос
            
        Returns:
            Словарь с информацией об адресе:
            {
                'has_address': bool,
                'street': str | None,
                'house': str | None,
                'corpus': str | None,
                'apartment': str | None,
                'full_address': str | None
            }
        """
        result = {
            'has_address': False,
            'street': None,
            'house': None,
            'corpus': None,
            'apartment': None,
            'full_address': None
        }
        
        # ШАГ 1: Пробуем через Natasha AddrExtractor (для полных адресов)
        if self._natasha_available:
            try:
                match = self._addr_extractor.find(query)
                
                if match and match.fact:
                    address_parts = {}
                    
                    # Извлекаем компоненты адреса
                    for part in match.fact.parts:
                        part_type = part.type.lower()
                        
                        # Маппинг типов Natasha к нашим полям
                        if part_type in ['улица', 'street']:
                            address_parts['street'] = part.value
                        elif part_type in ['дом', 'house']:
                            address_parts['house'] = part.value
                        elif part_type in ['корпус', 'corpus', 'корп']:
                            address_parts['corpus'] = part.value
                        elif part_type in ['квартира', 'apartment', 'кв']:
                            address_parts['apartment'] = part.value
                    
                    # Если нашли хотя бы улицу или дом
                    if address_parts.get('street') or address_parts.get('house'):
                        result['has_address'] = True
                        result.update(address_parts)
                        
                        # Формируем полный адрес
                        parts = []
                        if address_parts.get('street'):
                            parts.append(address_parts['street'])
                        if address_parts.get('house'):
                            parts.append(address_parts['house'])
                        if address_parts.get('corpus'):
                            parts.append(f"к.{address_parts['corpus']}")
                        
                        result['full_address'] = ' '.join(parts) if parts else None
                        return result  # Нашли через Natasha, возвращаем
            
            except Exception:
                # Тихо игнорируем ошибки парсинга
                pass
        
        # ШАГ 2: Fallback на regex паттерны (для простых адресов типа "мира 52")
        for pattern in self._street_patterns:
            match = pattern.search(query)
            if match:
                street = match.group(1).strip().lower()
                house = match.group(2).strip()
                
                # Фильтруем false positives (частые слова, не являющиеся улицами)
                common_words = {'дома', 'дом', 'дому', 'доме', 'года', 'год', 'году', 'лет', 'цена', 'цены'}
                if street in common_words:
                    continue
                
                # Если длина улицы < 3 символов - скорее всего это не улица
                if len(street) < 3:
                    continue
                
                result['has_address'] = True
                result['street'] = street
                result['house'] = house
                result['full_address'] = f"{street} {house}"
                return result
        
        return result
    
    def has_address(self, query: str) -> bool:
        """
        Быстрая проверка наличия адреса в запросе.
        
        Args:
            query: Поисковый запрос
            
        Returns:
            True если найден адрес
        """
        result = self.extract(query)
        return result['has_address']
    
    def get_street_and_house(self, query: str) -> Optional[tuple[str, str]]:
        """
        Извлекает улицу и дом из запроса.
        
        Args:
            query: Поисковый запрос
            
        Returns:
            Кортеж (улица, дом) или None
        """
        result = self.extract(query)
        
        if result['has_address'] and (result['street'] or result['house']):
            return (result['street'], result['house'])
        
        return None

