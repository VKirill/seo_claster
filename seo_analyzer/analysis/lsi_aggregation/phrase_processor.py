"""
Обработка и агрегация LSI фраз
"""

from collections import Counter
from typing import Dict, List, Any


class PhraseProcessor:
    """Процессор для обработки и агрегации LSI фраз"""
    
    @staticmethod
    def aggregate_phrases(
        phrases_list: List[Dict[str, Any]],
        top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Агрегировать список LSI фраз, оставляя только самые специфичные
        
        Args:
            phrases_list: Список фраз с частотой и источником
            top_n: Количество топ фраз для возврата
            
        Returns:
            Топ агрегированных фраз без дублирующихся подстрок
        """
        # Группируем по тексту фразы
        phrase_data = {}
        
        for item in phrases_list:
            if not isinstance(item, dict):
                continue
            
            phrase_text = item.get('phrase', '')
            
            # Если phrase_text - словарь (неправильный формат), пытаемся извлечь строку
            if isinstance(phrase_text, dict):
                phrase_text = phrase_text.get('phrase', '') if isinstance(phrase_text.get('phrase'), str) else ''
            
            # Если phrase_text все еще не строка - пропускаем
            if not isinstance(phrase_text, str) or not phrase_text.strip():
                continue
            
            frequency = item.get('frequency', 1)
            source = item.get('source', 'unknown')
            
            if phrase_text not in phrase_data:
                phrase_data[phrase_text] = {
                    'total_frequency': 0,
                    'sources': [],
                    'queries_count': 0,
                    'word_count': len(phrase_text.split())
                }
            
            phrase_data[phrase_text]['total_frequency'] += frequency
            phrase_data[phrase_text]['sources'].append(source)
            phrase_data[phrase_text]['queries_count'] += 1
        
        # Удаляем дублирующиеся подстроки
        phrase_data = PhraseProcessor._remove_substring_duplicates(phrase_data)
        
        # Сортируем: сначала по длине (специфичности), потом по частоте
        sorted_phrases = sorted(
            phrase_data.items(),
            key=lambda x: (x[1]['word_count'], x[1]['total_frequency']),
            reverse=True
        )[:top_n]
        
        # Формируем результат
        result = []
        for phrase_text, data in sorted_phrases:
            # Основной источник
            source_counter = Counter(data['sources'])
            main_source = source_counter.most_common(1)[0][0] if data['sources'] else 'unknown'
            
            result.append({
                'phrase': phrase_text,
                'frequency': data['total_frequency'],
                'queries_count': data['queries_count'],
                'main_source': main_source,
                'relevance_score': PhraseProcessor._calculate_relevance(
                    data['total_frequency'],
                    data['queries_count'],
                    data['word_count']
                )
            })
        
        return result
    
    @staticmethod
    def _remove_substring_duplicates(
        phrase_data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Удалить короткие фразы, которые полностью входят в более длинные
        
        Args:
            phrase_data: Словарь с данными фраз
            
        Returns:
            Отфильтрованный словарь фраз
        """
        phrases_by_length = sorted(
            phrase_data.keys(),
            key=lambda x: len(x.split()),
            reverse=True
        )
        
        to_remove = set()
        
        for long_phrase in phrases_by_length:
            if long_phrase in to_remove:
                continue
                
            long_freq = phrase_data[long_phrase]['total_frequency']
            
            # Проверяем все более короткие фразы
            for short_phrase in phrases_by_length:
                if short_phrase in to_remove:
                    continue
                
                if len(short_phrase.split()) >= len(long_phrase.split()):
                    continue
                
                # Проверяем вхождение подстроки
                if PhraseProcessor._is_substring(short_phrase, long_phrase):
                    short_freq = phrase_data[short_phrase]['total_frequency']
                    
                    # Удаляем короткую если она не намного частотнее (не более 2x)
                    if short_freq <= long_freq * 2:
                        to_remove.add(short_phrase)
        
        return {
            phrase: data 
            for phrase, data in phrase_data.items() 
            if phrase not in to_remove
        }
    
    @staticmethod
    def _is_substring(short: str, long: str) -> bool:
        """
        Проверить входит ли короткая фраза в длинную (все слова подряд)
        
        Args:
            short: Короткая фраза (например "система контроль")
            long: Длинная фраза (например "система контроль управление")
        
        Returns:
            True если короткая полностью входит в длинную
        """
        short_words = short.split()
        long_words = long.split()
        
        if len(short_words) >= len(long_words):
            return False
        
        # Ищем последовательное вхождение
        for i in range(len(long_words) - len(short_words) + 1):
            if long_words[i:i + len(short_words)] == short_words:
                return True
        
        return False
    
    @staticmethod
    def _calculate_relevance(
        total_frequency: int,
        queries_count: int,
        word_count: int
    ) -> float:
        """
        Рассчитать релевантность фразы для кластера с учетом специфичности
        
        Args:
            total_frequency: Общая частота встречаемости
            queries_count: В скольких запросах встречается
            word_count: Количество слов в фразе
        
        Returns:
            Оценка релевантности (выше = лучше)
        """
        # Базовая формула
        base_score = total_frequency * (1 + queries_count * 0.1)
        
        # Бонус за специфичность (длинные фразы)
        specificity_bonus = 1.0 + (word_count - 2) * 0.3
        
        return base_score * specificity_bonus

