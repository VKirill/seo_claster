"""Генератор падежных форм для запросов"""

from typing import Dict, List, Optional
import pymorphy3


class FormsGenerator:
    """Генератор словоформ в разных падежах"""
    
    # Названия падежей
    CASES = {
        'nomn': 'nominative',      # Именительный
        'gent': 'genitive',        # Родительный
        'datv': 'dative',          # Дательный
        'accs': 'accusative',      # Винительный
        'ablt': 'instrumental',    # Творительный
        'loct': 'prepositional',   # Предложный
    }
    
    def __init__(self):
        """Инициализация генератора"""
        self.morph = pymorphy3.MorphAnalyzer()
    
    def inflect_word(self, word: str, case: str) -> str:
        """
        Склоняет одно слово в заданный падеж
        
        Args:
            word: Слово для склонения
            case: Код падежа (nomn, gent, datv, accs, ablt, loct)
            
        Returns:
            Слово в нужном падеже
        """
        parsed = self.morph.parse(word)[0]
        
        # Пытаемся склонить
        inflected = parsed.inflect({case})
        
        if inflected:
            return inflected.word
        else:
            # Если не получилось, возвращаем оригинал
            return word
    
    def inflect_phrase(self, phrase: str, case: str) -> str:
        """
        Склоняет фразу (несколько слов) в заданный падеж
        
        Args:
            phrase: Фраза
            case: Код падежа
            
        Returns:
            Фраза в нужном падеже
        """
        words = phrase.split()
        inflected_words = []
        
        for word in words:
            # Пропускаем очень короткие слова и предлоги
            if len(word) <= 2 and word.lower() in ['в', 'на', 'с', 'к', 'о', 'у', 'из', 'до', 'от', 'по']:
                inflected_words.append(word)
            else:
                inflected_words.append(self.inflect_word(word, case))
        
        return ' '.join(inflected_words)
    
    def generate_all_forms(self, query: str) -> Dict[str, str]:
        """
        Генерирует все падежные формы запроса
        
        Args:
            query: Исходный запрос
            
        Returns:
            Словарь с формами для всех падежей
        """
        forms = {}
        
        for case_code, case_name in self.CASES.items():
            forms[case_name] = self.inflect_phrase(query, case_code)
        
        return forms
    
    def generate_all_forms_detailed(self, query: str) -> Dict[str, Dict[str, str]]:
        """
        Генерирует детальную информацию о формах
        
        Args:
            query: Исходный запрос
            
        Returns:
            Словарь с кодами падежей и полными названиями
        """
        result = {}
        
        for case_code, case_name in self.CASES.items():
            inflected = self.inflect_phrase(query, case_code)
            result[case_name] = {
                'code': case_code,
                'name_ru': self._get_russian_case_name(case_code),
                'form': inflected,
            }
        
        return result
    
    def _get_russian_case_name(self, case_code: str) -> str:
        """Возвращает русское название падежа"""
        names = {
            'nomn': 'Именительный',
            'gent': 'Родительный',
            'datv': 'Дательный',
            'accs': 'Винительный',
            'ablt': 'Творительный',
            'loct': 'Предложный',
        }
        return names.get(case_code, case_code)
    
    def generate_forms_batch(self, queries: List[str]) -> List[Dict[str, any]]:
        """
        Пакетная генерация форм для множества запросов
        
        Args:
            queries: Список запросов
            
        Returns:
            Список словарей с формами
        """
        results = []
        
        for query in queries:
            forms = self.generate_all_forms(query)
            results.append({
                'original': query,
                'forms': forms,
            })
        
        return results
    
    def get_case_form(self, query: str, case_name: str) -> Optional[str]:
        """
        Получает конкретную падежную форму
        
        Args:
            query: Запрос
            case_name: Название падежа (nominative, genitive, etc.)
            
        Returns:
            Форма в нужном падеже или None
        """
        # Находим код падежа по названию
        case_code = None
        for code, name in self.CASES.items():
            if name == case_name:
                case_code = code
                break
        
        if not case_code:
            return None
        
        return self.inflect_phrase(query, case_code)
    
    def analyze_word_morphology(self, word: str) -> Dict[str, any]:
        """
        Детальный морфологический анализ слова
        
        Args:
            word: Слово
            
        Returns:
            Словарь с морфологической информацией
        """
        parsed = self.morph.parse(word)[0]
        
        return {
            'word': word,
            'normal_form': parsed.normal_form,
            'pos': parsed.tag.POS,  # Часть речи
            'case': parsed.tag.case,  # Падеж
            'gender': parsed.tag.gender,  # Род
            'number': parsed.tag.number,  # Число
            'score': parsed.score,  # Уверенность разбора
        }


# Вспомогательные функции

def create_url_slug(text: str, case: str = 'nomn') -> str:
    """
    Создает URL-friendly слаг из текста
    
    Args:
        text: Исходный текст
        case: Падеж для склонения
        
    Returns:
        URL слаг
    """
    generator = FormsGenerator()
    inflected = generator.inflect_phrase(text, case)
    
    # Транслитерация (базовая)
    translit = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd',
        'е': 'e', 'ё': 'e', 'ж': 'zh', 'з': 'z', 'и': 'i',
        'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
        'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't',
        'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch',
        'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '',
        'э': 'e', 'ю': 'yu', 'я': 'ya',
    }
    
    slug = inflected.lower()
    result = []
    
    for char in slug:
        if char in translit:
            result.append(translit[char])
        elif char.isalnum() or char in '-_':
            result.append(char)
        else:
            result.append('-')
    
    # Убираем множественные дефисы
    slug = ''.join(result)
    slug = '-'.join(filter(None, slug.split('-')))
    
    return slug






