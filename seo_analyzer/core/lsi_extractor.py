"""
LSI Extractor Module
Извлечение LSI фраз из SERP результатов (заголовки, сниппеты, пассажи)
ОПТИМИЗИРОВАН: использует общий кэшированный лемматизатор
"""

import re
from collections import Counter
from typing import List, Dict, Set, Tuple

from .lemmatizer import lemmatize_word, lemmatize_phrase


class LSIExtractor:
    """Извлечение LSI фраз из текста SERP с кэшированием лемматизации"""
    
    def __init__(
        self,
        ngram_range: Tuple[int, int] = (2, 4),
        min_frequency: int = 2,
        use_lemmatization: bool = True,
        top_n: int = 10
    ):
        self.ngram_range = ngram_range
        self.min_frequency = min_frequency
        self.use_lemmatization = use_lemmatization
        self.top_n = top_n
        
        # Стоп-слова для LSI (предлоги, союзы, частицы)
        self.stopwords = {
            'в', 'на', 'и', 'с', 'по', 'для', 'из', 'к', 'о', 'от', 'до', 'при',
            'у', 'за', 'как', 'что', 'это', 'или', 'но', 'а', 'же', 'бы', 'ли',
            'не', 'ни', 'под', 'над', 'без', 'через', 'про', 'вы', 'мы', 'он',
            'она', 'оно', 'они', 'я', 'ты', 'мой', 'твой', 'наш', 'ваш', 'его',
            'её', 'их', 'этот', 'тот', 'такой', 'весь', 'сам', 'который'
        }
    
    def extract_from_serp_documents(
        self,
        documents: List[Dict],
        original_query: str = None
    ) -> List[Dict[str, any]]:
        """
        Извлечь LSI фразы из списка SERP документов
        
        Args:
            documents: Список документов с полями title, snippet, passages
            original_query: Оригинальный запрос (для исключения)
            
        Returns:
            Список LSI фраз с частотой и источником
        """
        all_texts = []
        
        # Собираем все тексты
        for doc in documents:
            title = doc.get('title', '')
            snippet = doc.get('snippet', '')
            passages = doc.get('passages', '')
            
            if title:
                all_texts.append(('title', title))
            if snippet:
                all_texts.append(('snippet', snippet))
            if passages:
                all_texts.append(('passages', passages))
        
        # Извлекаем n-граммы из всех текстов
        all_ngrams = []
        
        for source, text in all_texts:
            ngrams = self._extract_ngrams(text)
            for ngram in ngrams:
                all_ngrams.append({
                    'phrase': ngram,
                    'source': source
                })
        
        # Подсчитываем частоты
        phrase_counter = Counter([item['phrase'] for item in all_ngrams])
        
        # Фильтруем по минимальной частоте
        filtered_phrases = {
            phrase: count 
            for phrase, count in phrase_counter.items()
            if count >= self.min_frequency
        }
        
        # Исключаем оригинальный запрос
        if original_query:
            query_normalized = self._normalize_text(original_query)
            filtered_phrases = {
                phrase: count
                for phrase, count in filtered_phrases.items()
                if not self._is_substring_match(query_normalized, phrase)
            }
        
        # Сортируем по частоте и берем топ
        sorted_phrases = sorted(
            filtered_phrases.items(),
            key=lambda x: x[1],
            reverse=True
        )[:self.top_n]
        
        # Формируем результат
        result = []
        for phrase, frequency in sorted_phrases:
            # Определяем основной источник
            sources = [item['source'] for item in all_ngrams if item['phrase'] == phrase]
            main_source = Counter(sources).most_common(1)[0][0]
            
            result.append({
                'phrase': phrase,
                'frequency': frequency,
                'source': main_source
            })
        
        return result
    
    def _extract_ngrams(self, text: str) -> List[str]:
        """Извлечь n-граммы из текста"""
        # Нормализация текста
        text = self._normalize_text(text)
        
        # Токенизация
        tokens = self._tokenize(text)
        
        # Лемматизация если нужно (с кэшированием)
        if self.use_lemmatization:
            tokens = [lemmatize_word(token) for token in tokens]
        
        # Фильтрация стоп-слов и коротких токенов
        tokens = [
            token for token in tokens 
            if token not in self.stopwords and len(token) > 2
        ]
        
        # Генерация n-грамм
        ngrams = []
        for n in range(self.ngram_range[0], self.ngram_range[1] + 1):
            for i in range(len(tokens) - n + 1):
                ngram = ' '.join(tokens[i:i+n])
                ngrams.append(ngram)
        
        return ngrams
    
    def _normalize_text(self, text: str) -> str:
        """Нормализация текста"""
        # Удаляем HTML теги
        text = re.sub(r'<[^>]+>', '', text)
        
        # Удаляем специальные символы
        text = re.sub(r'[^\w\s\-]', ' ', text)
        
        # Множественные пробелы в один
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip().lower()
    
    def _tokenize(self, text: str) -> List[str]:
        """Токенизация текста"""
        # Простая токенизация по пробелам и дефисам
        tokens = re.findall(r'\b\w+\b', text)
        return tokens
    
    def _lemmatize(self, word: str) -> str:
        """
        Лемматизация слова (устаревший метод, использует кэшированную версию)
        Оставлен для обратной совместимости
        """
        return lemmatize_word(word)
    
    def _is_substring_match(self, query: str, phrase: str) -> bool:
        """Проверка является ли фраза частью запроса"""
        # Удаляем пробелы для сравнения
        query_words = set(query.lower().split())
        phrase_words = set(phrase.lower().split())
        
        # Если все слова фразы есть в запросе подряд - это совпадение
        if phrase_words.issubset(query_words):
            return True
        
        return False
    
    def aggregate_cluster_lsi(
        self,
        cluster_queries_lsi: List[List[Dict]],
        top_n: int = 10
    ) -> List[Dict[str, any]]:
        """
        Агрегировать LSI фразы для кластера
        
        Args:
            cluster_queries_lsi: Список LSI фраз для каждого запроса кластера
            top_n: Количество топ фраз для кластера
            
        Returns:
            Топ LSI фразы для всего кластера
        """
        all_phrases = []
        
        # Собираем все фразы
        for query_lsi in cluster_queries_lsi:
            all_phrases.extend(query_lsi)
        
        # Группируем по фразам и суммируем частоты
        phrase_frequencies = {}
        phrase_sources = {}
        
        for item in all_phrases:
            phrase = item['phrase']
            frequency = item['frequency']
            source = item['source']
            
            if phrase not in phrase_frequencies:
                phrase_frequencies[phrase] = 0
                phrase_sources[phrase] = []
            
            phrase_frequencies[phrase] += frequency
            phrase_sources[phrase].append(source)
        
        # Сортируем и берем топ
        sorted_phrases = sorted(
            phrase_frequencies.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]
        
        # Формируем результат
        result = []
        for phrase, total_frequency in sorted_phrases:
            # Основной источник
            sources = phrase_sources[phrase]
            main_source = Counter(sources).most_common(1)[0][0]
            
            result.append({
                'phrase': phrase,
                'frequency': total_frequency,
                'source': main_source,
                'queries_count': len([s for s in sources])  # В скольких запросах встречается
            })
        
        return result
    
    def extract_from_text(self, text: str) -> List[str]:
        """
        Простое извлечение n-грамм из произвольного текста
        
        Returns:
            Список уникальных n-грамм
        """
        ngrams = self._extract_ngrams(text)
        return list(set(ngrams))

