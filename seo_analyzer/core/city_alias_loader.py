"""
Загрузка маппинга алиасов городов из XML файла
"""

from pathlib import Path
from typing import Dict
import xml.etree.ElementTree as ET
from functools import lru_cache


@lru_cache(maxsize=1)
def load_city_aliases() -> Dict[str, str]:
    """
    Загружает маппинг алиасов городов из XML файла.
    
    Returns:
        Словарь {короткое_название: Полное Название}
        Например: {'спб': 'Санкт-Петербург', 'москва': 'Москва'}
    """
    xml_path = Path(__file__).parent.parent.parent / 'keyword_settings' / 'city_aliases.xml'
    
    if not xml_path.exists():
        print(f"⚠️ Файл city_aliases.xml не найден: {xml_path}")
        return {}
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        aliases = {}
        for alias_elem in root.findall('alias'):
            short = alias_elem.get('short', '').strip().lower()
            full = alias_elem.get('full', '').strip()
            
            if short and full:
                aliases[short] = full
        
        return aliases
    
    except Exception as e:
        print(f"⚠️ Ошибка загрузки city_aliases.xml: {e}")
        return {}


def normalize_city_name(city_name: str) -> str:
    """
    Преобразует короткое название города в полное.
    
    Args:
        city_name: Название города (может быть сокращением, падежной формой)
        
    Returns:
        Полное официальное название с заглавной буквы
        
    Examples:
        >>> normalize_city_name("спб")
        "Санкт-Петербург"
        >>> normalize_city_name("мск")
        "Москва"
        >>> normalize_city_name("екатеринбурге")
        "Екатеринбург"
    """
    if not city_name:
        return city_name
    
    aliases = load_city_aliases()
    city_lower = city_name.lower()
    
    # Проверяем маппинг алиасов
    if city_lower in aliases:
        return aliases[city_lower]
    
    # Если не найдено в алиасах, капитализируем
    return city_name.title()

