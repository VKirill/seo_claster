"""
XML Text Extractor Module
Извлечение чистого текста из XML элементов с вложенными тегами
"""

import re
import xml.etree.ElementTree as ET
from typing import Optional


class XMLTextExtractor:
    """Извлечение текста из XML с правильной обработкой вложенных тегов"""
    
    def extract_text(self, element: Optional[ET.Element]) -> str:
        """
        Извлечь весь текст из XML элемента, включая текст из вложенных тегов.
        
        Args:
            element: XML элемент
            
        Returns:
            Очищенная текстовая строка
            
        Example:
            >>> xml = '<title><hlword>СКУД</hlword> – система <hlword>контроля</hlword></title>'
            >>> elem = ET.fromstring(xml)
            >>> extractor = XMLTextExtractor()
            >>> extractor.extract_text(elem)
            'СКУД – система контроля'
        """
        if element is None:
            return ''
        
        # Собираем весь текст из элемента и его потомков
        text_parts = list(element.itertext())
        full_text = ''.join(text_parts)
        
        return self._clean_text(full_text)
    
    def _clean_text(self, text: str) -> str:
        """
        Очистить текст от лишних пробелов и нормализовать.
        
        Args:
            text: Исходный текст
            
        Returns:
            Очищенный текст
        """
        if not text:
            return ''
        
        # Декодируем HTML entities
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        text = text.replace('&nbsp;', ' ')
        text = text.replace('\xa0', ' ')
        
        # Убираем множественные пробелы
        text = re.sub(r'\s+', ' ', text)
        
        # Убираем пробелы в начале и конце
        return text.strip()
    
    def extract_text_from_multiple(
        self, 
        elements: list[ET.Element]
    ) -> list[str]:
        """
        Извлечь текст из списка XML элементов.
        
        Args:
            elements: Список XML элементов
            
        Returns:
            Список текстовых строк
        """
        return [
            self.extract_text(elem) 
            for elem in elements 
            if elem is not None
        ]

