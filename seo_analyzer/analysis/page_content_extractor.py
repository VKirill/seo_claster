"""Извлечение текстового контента из HTML страниц"""

import re
from typing import Optional, Dict
from bs4 import BeautifulSoup


class PageContentExtractor:
    """Извлекает и анализирует текстовый контент страниц"""
    
    def __init__(self):
        """Инициализация"""
        pass
    
    def extract_text_from_html(self, html: str) -> Dict:
        """
        Извлечь текст из HTML
        
        Args:
            html: HTML код страницы
            
        Returns:
            Словарь с данными о контенте
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Удаляем script, style, nav, footer, header
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            tag.decompose()
        
        # Получаем текст из body
        body = soup.find('body')
        if not body:
            body = soup
        
        text = body.get_text(separator=' ', strip=True)
        
        # Очищаем текст
        text = self._clean_text(text)
        
        # Анализируем
        result = {
            'text': text,
            'text_length': len(text),
            'words_count': len(text.split()),
            'title': self._extract_title(soup),
            'h1': self._extract_h1(soup),
            'meta_description': self._extract_meta_description(soup)
        }
        
        return result
    
    def _clean_text(self, text: str) -> str:
        """Очистить текст от лишних символов"""
        # Убираем множественные пробелы
        text = re.sub(r'\s+', ' ', text)
        
        # Убираем пробелы в начале и конце
        text = text.strip()
        
        return text
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Извлечь title"""
        title_tag = soup.find('title')
        return title_tag.get_text(strip=True) if title_tag else ''
    
    def _extract_h1(self, soup: BeautifulSoup) -> str:
        """Извлечь H1"""
        h1_tag = soup.find('h1')
        return h1_tag.get_text(strip=True) if h1_tag else ''
    
    def _extract_meta_description(self, soup: BeautifulSoup) -> str:
        """Извлечь meta description"""
        meta = soup.find('meta', attrs={'name': 'description'})
        if not meta:
            meta = soup.find('meta', attrs={'property': 'og:description'})
        
        return meta.get('content', '') if meta else ''






