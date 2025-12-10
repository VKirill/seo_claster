"""
Валидатор и нормализатор географических названий.

Строит индекс городов со всеми падежными формами и маппингами к базовым формам.
Используется для фильтрации результатов Natasha NER.
"""
from typing import Set, Dict
from functools import lru_cache

from ..core.lemmatizer import get_morph_analyzer


@lru_cache(maxsize=1000)
def generate_city_forms(city: str) -> Set[str]:
    """
    Генерирует все падежные формы города через pymorphy3.
    
    Args:
        city: Название города в именительном падеже
        
    Returns:
        Множество всех падежных форм (включая оригинал)
        
    Example:
        >>> generate_city_forms('москва')
        {'москва', 'москвы', 'москве', 'москву', 'москвой', 'москвою'}
    """
    morph = get_morph_analyzer()
    forms = {city.lower()}  # Добавляем исходную форму
    
    try:
        # Парсим слово
        parsed = morph.parse(city)[0]
        
        # Генерируем все падежи
        cases = ['nomn', 'gent', 'datv', 'accs', 'ablt', 'loct']
        for case in cases:
            inflected = parsed.inflect({case})
            if inflected:
                forms.add(inflected.word.lower())
    except Exception:
        # Если не удалось склонить, возвращаем только исходную форму
        pass
    
    return forms


class GeoValidator:
    """
    Валидатор географических названий.
    
    Строит индекс валидных городов из словарей с падежными формами
    и маппингами к базовым формам.
    """
    
    def __init__(self, geo_dicts: Dict[str, Set[str]] = None):
        """
        Args:
            geo_dicts: Словари с географическими названиями 
                      (Russian, Moscow, Kazakhstan и т.д.)
        """
        self.geo_dicts = geo_dicts or {}
        self.valid_cities = set()  # Все формы всех городов (lowercase)
        self.city_form_to_base = {}  # Любая форма → базовая форма (lowercase)
        self.city_display_name = {}  # Базовая форма (lowercase) → красивое название (Title Case)
        self.city_aliases = {}  # Алиасы городов
        
        self._build_city_index()
    
    def _build_city_index(self):
        """
        Строит индекс валидных городов со всеми падежными формами.
        """
        # Собираем города из geo_dicts (Russian.txt, Moscow.txt и т.д.)
        russian_cities = self.geo_dicts.get('Russian', set())
        moscow_cities = self.geo_dicts.get('Moscow', set())
        
        # Объединяем все города
        all_cities = russian_cities | moscow_cities
        
        # Для каждого города генерируем все падежные формы
        for city in all_cities:
            city_lower = city.lower()
            
            # Добавляем базовую форму
            self.valid_cities.add(city_lower)
            self.city_form_to_base[city_lower] = city_lower
            
            # Сохраняем красивое название (как в словаре, с заглавными)
            self.city_display_name[city_lower] = city
            
            # Генерируем все падежные формы
            forms = generate_city_forms(city)
            for form in forms:
                self.valid_cities.add(form)
                self.city_form_to_base[form] = city_lower
        
        # Добавляем алиасы (спб, питер, мск и т.д.)
        self._add_city_aliases()
        
        # Добавляем красивые названия для полных форм алиасов
        self._add_display_names_for_aliases()
    
    def _add_city_aliases(self):
        """
        Добавляет сокращения и алиасы городов из city_aliases.xml.
        
        XML содержит маппинг сокращений к полным названиям:
        - спб → Санкт-Петербург
        - мск → Москва
        - питер → Санкт-Петербург
        
        Алиасы НЕ склоняются (это просто сокращения).
        НО: полные названия городов УЖЕ склонены через generate_city_forms() 
        в методе _build_city_index(), поэтому алиас маппится на уже склонённый город.
        """
        # Загружаем алиасы из XML файла
        from ..core.city_alias_loader import load_city_aliases
        
        xml_aliases = load_city_aliases()
        
        # Преобразуем алиасы в маппинг для кластеризации
        # XML: {'спб': 'Санкт-Петербург', 'москве': 'Москва'} 
        #   → {'спб': 'санкт-петербург', 'москве': 'москва'}
        self.city_aliases = {}
        for alias_lower, full_name in xml_aliases.items():
            base_city_lower = full_name.lower()
            self.city_aliases[alias_lower] = base_city_lower
        
        # Добавляем каждый алиас в индекс валидных городов
        for alias, base_city in self.city_aliases.items():
            # Алиас маппится на базовую форму города (которая уже есть в системе)
            # Например: 'спб' → 'санкт-петербург', 'мск' → 'москва'
            if base_city in self.city_form_to_base.values() or base_city in self.valid_cities:
                self.valid_cities.add(alias)
                self.city_form_to_base[alias] = base_city
                # НЕ генерируем падежи для алиасов - это сокращения, их не склоняют!
    
    def is_valid_city(self, location: str) -> bool:
        """
        Проверяет, является ли локация валидным городом.
        
        Args:
            location: Название локации (любой регистр)
            
        Returns:
            True если это известный город
        """
        return location.lower() in self.valid_cities
    
    def normalize_city(self, location: str) -> str:
        """
        Нормализует форму города к базовой форме.
        
        Args:
            location: Название города в любой форме (москве, спб, питере)
            
        Returns:
            Базовая форма города (москва, санкт-петербург)
        """
        location_lower = location.lower()
        return self.city_form_to_base.get(location_lower, location_lower)
    
    def _add_display_names_for_aliases(self):
        """
        Добавляет красивые названия для полных форм алиасов.
        
        Например:
        - санкт-петербург → Санкт-Петербург (из словаря)
        - москва → Москва (из словаря)
        """
        # Маппинг базовых форм к красивым названиям для алиасов
        alias_display_names = {
            'санкт-петербург': 'Санкт-Петербург',
            'москва': 'Москва',
        }
        
        # Обновляем display_name для базовых форм, если они не из словаря
        for base_form, display_name in alias_display_names.items():
            if base_form not in self.city_display_name:
                self.city_display_name[base_form] = display_name
    
    def get_display_name(self, location: str) -> str:
        """
        Получает красивое название города для отображения в экспорте.
        
        Преобразует:
        - "москва" → "Москва"
        - "спб" → "Санкт-Петербург"
        - "питере" → "Санкт-Петербург"
        - "мск" → "Москва"
        - "екатеринбурге" → "Екатеринбург"
        
        Args:
            location: Название города в любой форме
            
        Returns:
            Красивое название с заглавной буквы, полная форма для алиасов
        """
        # Нормализуем к базовой форме
        base_form = self.normalize_city(location)
        
        # Получаем красивое название
        display_name = self.city_display_name.get(base_form)
        
        if display_name:
            return display_name
        
        # Если нет в маппинге, капитализируем базовую форму
        return base_form.title()

