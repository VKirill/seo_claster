"""
Извлечение ключевых слов и фраз из поисковых запросов.

Использует Natasha для синтаксического анализа и определения
главных слов и именных групп через dependency parsing.
"""
from typing import List, Dict, Optional
from natasha import (
    Segmenter,
    NewsEmbedding,
    NewsMorphTagger,
    NewsSyntaxParser,
    NewsNERTagger,
    LOC,
    Doc
)


class KeyPhrasesExtractor:
    """Извлекает ключевые слова и фразы из поисковых запросов через Natasha"""
    
    def __init__(self):
        """Инициализация экстрактора с Natasha компонентами"""
        self.segmenter = Segmenter()
        self.emb = NewsEmbedding()
        self.morph_tagger = NewsMorphTagger(self.emb)
        self.syntax_parser = NewsSyntaxParser(self.emb)
        self.ner_tagger = NewsNERTagger(self.emb)  # Для определения гео-сущностей
        self.enabled = True
        
        # Стоп-слова (предлоги, союзы, частицы)
        self.stopwords = {
            'в', 'на', 'с', 'для', 'по', 'к', 'из', 'от', 'у', 'о', 'про',
            'и', 'или', 'но', 'а', 'что', 'как', 'это', 'то', 'же',
            'не', 'ни', 'ли', 'бы', 'только', 'даже', 'еще', 'уже'
        }
    
    def extract_key_phrases(self, query: str) -> Dict[str, any]:
        """
        Извлекает ключевые фразы из запроса через Natasha syntax parsing.
        
        Args:
            query: Поисковый запрос
            
        Returns:
            Словарь с извлеченными данными:
            {
                'main_words': ['слово1', 'слово2'],  # Главные слова (существительные, глаголы)
                'noun_phrases': ['фраза1', 'фраза2'],  # Именные группы через синтаксис
                'key_phrase': 'главная фраза',  # Самая важная фраза
                'nouns': ['сущ1', 'сущ2'],  # Только существительные
            }
        """
        try:
            # Создаем Doc и применяем pipeline
            doc = Doc(query.lower())
            doc.segment(self.segmenter)
            doc.tag_morph(self.morph_tagger)
            doc.parse_syntax(self.syntax_parser)
            doc.tag_ner(self.ner_tagger)  # NER для гео-сущностей
            
            main_words = []  # Существительные и глаголы
            nouns = []  # Только существительные
            noun_chunks = []  # Именные группы через синтаксис
            
            # Проходим по токенам
            for token in doc.tokens:
                if token.text in self.stopwords or len(token.text) <= 2:
                    continue
                
                # Lemma может быть None - используем text как fallback
                lemma = token.lemma if token.lemma else token.text
                
                # Существительные (самые важные)
                if token.pos == 'NOUN':
                    main_words.append(lemma)
                    nouns.append(lemma)
                
                # Глаголы (для коммерческих запросов)
                elif token.pos in ['VERB']:
                    main_words.append(lemma)
            
            # Извлекаем именные группы через dependency parsing
            # БЕЗ фильтрации - контекст важен!
            noun_chunks = self._extract_noun_chunks_from_syntax(doc)
            
            # Определяем ключевую фразу (передаем doc для умной фильтрации)
            key_phrase = self._determine_key_phrase_from_syntax(
                query, main_words, nouns, noun_chunks, doc
            )
            
            return {
                'main_words': main_words,
                'noun_phrases': noun_chunks,
                'key_phrase': key_phrase,
                'nouns': nouns
            }
            
        except Exception as e:
            # Fallback - возвращаем сам запрос
            return {
                'main_words': [],
                'noun_phrases': [],
                'key_phrase': query,
                'nouns': []
            }
    
    def _extract_noun_chunks_from_syntax(self, doc: Doc) -> List[str]:
        """
        Извлекает именные группы (noun chunks) через синтаксический анализ.
        
        Используется dependency parsing для определения:
        - nmod (nominal modifier): существительное + существительное
        - amod (adjectival modifier): прилагательное + существительное
        - obl (oblique): обстоятельство (например, "в школе", "для офиса")
        - nsubj (nominal subject): подлежащее
        - obj (object): объект
        
        ВАЖНО: Предложные обороты ВКЛЮЧАЮТСЯ, т.к. они задают контекст!
        
        Например: 
        - "биометрическая скуд в школе"
        - "система контроля доступа"
        - "скуд для офиса"
        """
        if not doc.tokens:
            return []
        
        noun_chunks = []
        processed_tokens = set()
        
        # Проходим по всем токенам
        for token in doc.tokens:
            if token.id in processed_tokens:
                continue
            
            # Ищем существительные с зависимостями
            # + ADJ которые могут быть ошибочно определены (например "скуд")
            if token.pos == 'NOUN' or (token.pos == 'ADJ' and token.rel == 'root'):
                chunk_tokens = [token]
                
                # Рекурсивно собираем всю цепочку модификаторов (включая предлоги)
                self._collect_modifiers_recursive(
                    doc, token, chunk_tokens, processed_tokens
                )
                
                # Если нашли группу из 2+ слов ИЛИ это root-токен
                if len(chunk_tokens) >= 2 or token.rel == 'root':
                    # Сортируем по позиции в тексте
                    chunk_tokens.sort(key=lambda t: t.start if t.start is not None else 0)
                    # Используем lemma или text как fallback
                    phrase = ' '.join([t.lemma if t.lemma else t.text for t in chunk_tokens])
                    noun_chunks.append(phrase)
                    processed_tokens.add(token.id)
        
        return noun_chunks
    
    def _collect_modifiers_recursive(
        self, 
        doc: Doc, 
        head_token, 
        chunk_tokens: list, 
        processed_tokens: set
    ):
        """
        Рекурсивно собирает все модификаторы для данного токена.
        ВКЛЮЧАЯ предложные обороты (контекст важен!)
        """
        for other_token in doc.tokens:
            if other_token.head_id == head_token.id and other_token.id not in processed_tokens:
                # Прилагательное-модификатор (amod)
                if other_token.rel == 'amod' and other_token.pos in ['ADJ']:
                    chunk_tokens.insert(0, other_token)  # Прилагательное перед существительным
                    processed_tokens.add(other_token.id)
                
                # nmod с ADJ (ошибка Natasha - определяет существительные как ADJ)
                # Например: "скуд" определяется как ADJ в "тз на скуд болид"
                elif other_token.rel == 'nmod' and other_token.pos == 'ADJ':
                    # Ищем предлог
                    preposition = None
                    for t in doc.tokens:
                        if t.head_id == other_token.id and t.rel == 'case' and t.pos == 'ADP':
                            preposition = t
                            break
                    
                    # Добавляем предлог (если есть) + "прилагательное" (на самом деле сущ.)
                    if preposition and preposition.id not in processed_tokens:
                        chunk_tokens.append(preposition)
                        processed_tokens.add(preposition.id)
                    
                    chunk_tokens.append(other_token)
                    processed_tokens.add(other_token.id)
                    
                    # Рекурсивно ищем модификаторы
                    self._collect_modifiers_recursive(
                        doc, other_token, chunk_tokens, processed_tokens
                    )
                
                # Существительное-модификатор (nmod, obl) - ВКЛЮЧАЯ с предлогами
                elif other_token.rel in ['nmod', 'obl'] and other_token.pos == 'NOUN':
                    # Ищем предлог перед этим существительным
                    preposition = None
                    for t in doc.tokens:
                        if t.head_id == other_token.id and t.rel == 'case' and t.pos == 'ADP':
                            preposition = t
                            break
                    
                    # Добавляем предлог (если есть) + существительное
                    if preposition and preposition.id not in processed_tokens:
                        chunk_tokens.append(preposition)  # "в", "для", "на"
                        processed_tokens.add(preposition.id)
                    
                    chunk_tokens.append(other_token)  # Существительное
                    processed_tokens.add(other_token.id)
                    
                    # Рекурсивно ищем модификаторы этого существительного
                    self._collect_modifiers_recursive(
                        doc, other_token, chunk_tokens, processed_tokens
                    )
    
    def _should_exclude_tail_word(self, word: str, doc: Doc) -> bool:
        """
        Определяет, является ли слово "хвостовым" атрибутом через Natasha.
        
        Исключаем:
        - Географические сущности (LOC) если они в конце
        - Существительные с rel=advmod (обстоятельства типа "цена", "стоимость")
        - Вторичные nmod (зависят от модификатора, а не от root)
        - Глаголы в инфинитиве (купить, заказать)
        - Прилагательные-атрибуты (дешево, недорого)
        
        Args:
            word: Слово для проверки
            doc: Документ Natasha с разметкой
            
        Returns:
            True если слово нужно исключить из key_phrase
        """
        # Ищем токен с этим словом
        for token in doc.tokens:
            if (token.lemma and token.lemma == word) or token.text == word:
                # 1. Географическая сущность (LOC)
                for span in doc.spans:
                    if span.type == LOC and word in span.text.lower():
                        return True
                
                # 2. Существительное с rel=advmod (обстоятельство)
                if token.pos == 'NOUN' and token.rel == 'advmod':
                    return True
                
                # 3. Вторичный nmod: зависит от модификатора (obl), а не от root или первичного nmod
                if token.pos == 'NOUN' and token.rel == 'nmod':
                    # Ищем голову (head_token)
                    head_token = None
                    for t in doc.tokens:
                        if t.id == token.head_id:
                            head_token = t
                            break
                    
                    # Если голова - obl (обстоятельство), то это вторичный атрибут
                    # Но если голова - nmod и зависит от root, то это ЧАСТЬ основной фразы!
                    # Пример: "система контроля доступа" - "доступа" зависит от "контроля" (nmod от root)
                    if head_token:
                        if head_token.rel == 'obl':
                            # Вторичный атрибут для обстоятельства (например, "цена офиса")
                            return True
                        elif head_token.rel == 'nmod':
                            # Проверяем, зависит ли голова от root
                            # Если да - это цепочка nmod (система→контроля→доступа), оставляем
                            # Если нет - это вторичный атрибут
                            grandparent_id = head_token.head_id
                            grandparent_token = None
                            for t in doc.tokens:
                                if t.id == grandparent_id:
                                    grandparent_token = t
                                    break
                            
                            # Если дедушка - root, то это основная цепочка
                            if grandparent_token and grandparent_token.rel == 'root':
                                return False  # НЕ исключаем
                            else:
                                return True  # Исключаем вторичный nmod
                
                # 4. Глагол в инфинитиве (купить, заказать)
                if token.pos == 'VERB' and token.feats:
                    # Проверяем VerbForm=Inf
                    feats_str = str(token.feats)
                    if 'VerbForm=Inf' in feats_str:
                        return True
                
                # 5. Прилагательное-атрибут с низким приоритетом
                if token.pos == 'ADJ' and token.rel in ['advmod', 'amod'] and token.head_id != token.id:
                    # Если это не главное прилагательное
                    return True
        
        return False
    
    def _determine_key_phrase_from_syntax(
        self,
        query: str,
        main_words: List[str],
        nouns: List[str],
        noun_chunks: List[str],
        doc: Doc = None
    ) -> str:
        """
        Определяет самую важную (ключевую) фразу через синтаксический анализ.
        
        Приоритет:
        1. Самая длинная именная группа (контекст важен!)
        2. Убираем лишние слова в конце ДИНАМИЧЕСКИ через Natasha (без хардкода)
        3. Fallback на существительные
        """
        # Если есть именные группы - берем самую длинную
        if noun_chunks:
            best_chunk = max(noun_chunks, key=lambda x: len(x.split()))
            
            # Убираем лишние слова в конце ДИНАМИЧЕСКИ
            words = best_chunk.split()
            if doc:
                while words and self._should_exclude_tail_word(words[-1], doc):
                    words.pop()
            
            if words:
                return ' '.join(words)
            else:
                return best_chunk  # Fallback если все слова исключили
        
        # Если нет именных групп, но есть несколько существительных
        # берем первые 2-3 (основные понятия запроса)
        if len(nouns) >= 2:
            # Убираем лишние слова ДИНАМИЧЕСКИ через Natasha
            filtered_nouns = nouns
            if doc:
                filtered_nouns = [n for n in nouns if not self._should_exclude_tail_word(n, doc)]
            
            if filtered_nouns:
                return ' '.join(filtered_nouns[:3])
            else:
                return ' '.join(nouns[:2])
        
        # Если есть хотя бы одно существительное - берем его
        if nouns:
            return nouns[0]
        
        # Если есть главные слова - объединяем их
        if main_words:
            return ' '.join(main_words[:2])
        
        # Fallback - весь запрос без стоп-слов
        words = [w for w in query.lower().split() if w not in self.stopwords]
        return ' '.join(words[:3]) if words else query
    
    def extract_batch(self, queries: List[str]) -> List[Dict[str, any]]:
        """
        Извлекает ключевые фразы для списка запросов.
        
        Args:
            queries: Список запросов
            
        Returns:
            Список словарей с результатами
        """
        results = []
        
        for query in queries:
            result = self.extract_key_phrases(query)
            result['query'] = query
            results.append(result)
        
        return results
    
    def get_main_words_string(self, query: str) -> str:
        """
        Возвращает главные слова в виде строки (для CSV колонки).
        
        Args:
            query: Запрос
            
        Returns:
            Строка с главными словами через запятую
        """
        result = self.extract_key_phrases(query)
        if result['main_words']:
            return ', '.join(result['main_words'])
        return ''
    
    def get_key_phrase(self, query: str) -> str:
        """
        Возвращает ключевую фразу (для CSV колонки).
        
        Args:
            query: Запрос
            
        Returns:
            Ключевая фраза
        """
        result = self.extract_key_phrases(query)
        return result['key_phrase']

