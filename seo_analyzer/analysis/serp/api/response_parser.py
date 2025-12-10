"""
Парсинг ответов от API
"""

import re
from typing import Dict, Any, Optional


class ResponseParser:
    """Парсер ответов от API"""
    
    @staticmethod
    def parse_html_error(error_text: str) -> str:
        """
        Извлечь информацию об ошибке из HTML
        
        Args:
            error_text: HTML текст с ошибкой
            
        Returns:
            Сообщение об ошибке
        """
        title_match = re.search(r'<title>([^<]+)</title>', error_text, re.IGNORECASE)
        title = title_match.group(1) if title_match else "Server Error"
        h1_match = re.search(r'<h1[^>]*>([^<]+)</h1>', error_text, re.IGNORECASE)
        h1 = h1_match.group(1) if h1_match else ""
        return f"{title}" + (f": {h1}" if h1 else "")
    
    @staticmethod
    def is_html_response(xml_text: str) -> bool:
        """Проверить, является ли ответ HTML вместо XML"""
        return xml_text.strip().lower().startswith('<html')
    
    @staticmethod
    def extract_req_id(xml_text: str) -> Optional[str]:
        """
        Извлечь req_id из XML ответа
        
        Args:
            xml_text: XML текст ответа
            
        Returns:
            req_id или None если не найден
        """
        req_id_match = re.search(r'<req_id>([^<]+)</req_id>', xml_text)
        return req_id_match.group(1) if req_id_match else None
    
    @staticmethod
    def has_xml_error(xml_text: str) -> bool:
        """Проверить наличие ошибки в XML"""
        return '<error' in xml_text
    
    @staticmethod
    def is_not_ready_error(xml_text: str) -> bool:
        """Проверить, является ли ошибка "еще не готово" (202)"""
        return 'code="202"' in xml_text or 'не обработан' in xml_text
    
    @staticmethod
    def is_queue_error(xml_text: str) -> bool:
        """Проверить, является ли ошибка "в очереди" (210)"""
        return 'code="210"' in xml_text or 'поставлен в очередь' in xml_text

