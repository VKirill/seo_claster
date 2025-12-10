"""
Расширенный классификатор интента на основе SERP контента
Определяет базовый интент (commercial/informational) + модификаторы

Использует:
1. Анализ контента (title, snippet, passages)
2. Анализ доменов (маркетплейсы vs информационные сайты)
"""

from typing import Dict, List, Set, Optional
import re

from ...core.domain_classifier import DomainClassifier


class EnhancedSERPIntentClassifier:
    """
    Расширенный классификатор интента на основе контента SERP
    
    Определяет:
    - Базовый интент: commercial / informational
    - Модификаторы: _geo, _work, _handmade, _bu, _opt, _review
    
    Примеры:
    - commercial_geo - коммерческий запрос с гео
    - informational_geo - информационный запрос с гео
    - commercial_opt - оптовые продажи
    - informational_review - обзоры и отзывы
    """
    
    def __init__(
        self,
        commercial_words: Set[str],
        info_words: Set[str],
        geo_words: Set[str] = None,
        work_words: Set[str] = None,
        handmade_words: Set[str] = None,
        bu_words: Set[str] = None,
        opt_words: Set[str] = None,
        review_words: Set[str] = None,
        domain_classifier: Optional[DomainClassifier] = None
    ):
        """
        Args:
            commercial_words: Коммерческие слова из commercial.txt
            info_words: Информационные слова из info.txt
            geo_words: Географические слова (из всех гео-словарей)
            work_words: Слова о работе из work.txt
            handmade_words: DIY слова из handmade.txt
            bu_words: Б/У слова из bu.txt
            opt_words: Оптовые слова из opt.txt
            review_words: Обзоры/отзывы из Review.txt
        """
        self.commercial_words = {w.lower() for w in commercial_words}
        self.info_words = {w.lower() for w in info_words}
        self.geo_words = {w.lower() for w in geo_words} if geo_words else set()
        self.work_words = {w.lower() for w in work_words} if work_words else set()
        self.handmade_words = {w.lower() for w in handmade_words} if handmade_words else set()
        self.bu_words = {w.lower() for w in bu_words} if bu_words else set()
        self.opt_words = {w.lower() for w in opt_words} if opt_words else set()
        self.review_words = {w.lower() for w in review_words} if review_words else set()
        
        # Классификатор доменов для дополнительной проверки
        self.domain_classifier = domain_classifier
        if domain_classifier:
            print(f"  ✓ Включен анализ доменов ({domain_classifier.get_domain_stats()['total_count']} доменов)")
    
    def _find_words_in_text(self, text: str, word_set: Set[str]) -> tuple[int, List[str]]:
        """
        Находит ЦЕЛЫЕ слова из словаря в тексте (не подстроки!)
        
        Использует границы слов \b для точного поиска.
        "бу" найдётся в "бу товар", но НЕ найдётся в "попробуйте" или "бабушки"
        
        Returns:
            (количество_вхождений, найденные_уникальные_слова)
        """
        text_lower = text.lower()
        found_words = []
        count = 0
        
        for word in word_set:
            # Ищем целое слово с границами \b
            # Для фраз (с пробелами) ищем как есть
            if ' ' in word:
                # Для фраз типа "своими руками" ищем точное вхождение
                pattern = r'\b' + re.escape(word) + r'\b'
            else:
                # Для одиночных слов
                pattern = r'\b' + re.escape(word) + r'\b'
            
            matches = re.findall(pattern, text_lower)
            if matches:
                found_words.append(word)
                count += len(matches)  # Считаем все вхождения
        
        return count, found_words
    
    def analyze_documents(
        self,
        documents: List[Dict],
        top_n: int = 30
    ) -> Dict[str, any]:
        """
        Анализирует документы из SERP и определяет расширенный интент
        
        Args:
            documents: Список документов с полями title, snippet, passages, url
            top_n: Количество документов для анализа
            
        Returns:
            Dict с результатами:
            {
                'base_intent': str,           # 'commercial' или 'informational'
                'full_intent': str,           # Полный интент с модификаторами
                'confidence': float,          # Уверенность 0-1
                'commercial_score': int,      # Количество коммерческих слов
                'info_score': int,            # Количество информационных слов
                'domain_commercial_ratio': float,  # Доля коммерческих доменов
                'domain_info_ratio': float,        # Доля информационных доменов
                'has_geo': bool,              # Есть гео?
                'has_work': bool,             # Про работу?
                'has_handmade': bool,         # DIY?
                'has_bu': bool,               # Б/У?
                'has_opt': bool,              # Опт?
                'has_review': bool,           # Обзоры?
                'modifiers': List[str],       # Список модификаторов
                'scores': Dict[str, int]      # Детальные скоры
            }
        """
        # Собираем весь текст И URL
        all_text = []
        urls = []
        docs_analyzed = 0
        
        for doc in documents[:top_n]:
            title = doc.get('title', '') or ''
            snippet = doc.get('snippet', '') or ''
            passages = doc.get('passages', '') or ''
            url = doc.get('url', '')
            
            doc_text = f"{title} {snippet} {passages}"
            all_text.append(doc_text)
            
            if url:
                urls.append(url)
            
            docs_analyzed += 1
        
        combined_text = ' '.join(all_text).lower()
        
        # 1. Анализируем контент (слова)
        comm_count, comm_words = self._find_words_in_text(combined_text, self.commercial_words)
        info_count, info_words = self._find_words_in_text(combined_text, self.info_words)
        
        # 2. Анализируем домены (если есть классификатор)
        domain_comm_ratio = 0.0
        domain_info_ratio = 0.0
        domain_intent_boost = 0
        
        if self.domain_classifier and urls:
            domain_comm_ratio, domain_info_ratio = self.domain_classifier.analyze_serp_urls(urls)
            
            # Бустим скоры на основе доменов (сильный сигнал)
            if domain_comm_ratio >= 0.6:  # 60%+ коммерческих доменов
                domain_intent_boost = int(comm_count * 0.5)  # +50% к скору
                comm_count += domain_intent_boost
            elif domain_info_ratio >= 0.6:  # 60%+ информационных доменов
                domain_intent_boost = int(info_count * 0.5)
                info_count += domain_intent_boost
        
        # Определяем базовый интент
        total_score = comm_count + info_count
        
        if total_score == 0:
            base_intent = 'commercial'
            confidence = 0.5
        elif comm_count > info_count:
            base_intent = 'commercial'
            confidence = comm_count / total_score
        elif info_count > comm_count:
            base_intent = 'informational'
            confidence = info_count / total_score
        else:
            base_intent = 'commercial'
            confidence = 0.5
        
        # Анализируем модификаторы
        geo_count, geo_words_found = self._find_words_in_text(combined_text, self.geo_words)
        work_count, work_words_found = self._find_words_in_text(combined_text, self.work_words)
        handmade_count, handmade_words_found = self._find_words_in_text(combined_text, self.handmade_words)
        bu_count, bu_words_found = self._find_words_in_text(combined_text, self.bu_words)
        opt_count, opt_words_found = self._find_words_in_text(combined_text, self.opt_words)
        review_count, review_words_found = self._find_words_in_text(combined_text, self.review_words)
        
        # Определяем наличие модификаторов
        # Единый порог: минимум 6 вхождений для присвоения группы
        # Исключение: б/у (короткий специфичный маркер) - достаточно 2 раз
        MODIFIER_THRESHOLD = 6
        
        has_geo = geo_count >= MODIFIER_THRESHOLD
        has_work = work_count >= MODIFIER_THRESHOLD
        has_handmade = handmade_count >= MODIFIER_THRESHOLD
        has_bu = bu_count >= 2  # Для б/у более низкий порог (специфичный маркер)
        has_opt = opt_count >= MODIFIER_THRESHOLD
        has_review = review_count >= MODIFIER_THRESHOLD
        
        # Формируем список модификаторов
        modifiers = []
        if has_geo:
            modifiers.append('geo')
        if has_work:
            modifiers.append('work')
        if has_handmade:
            modifiers.append('handmade')
        if has_bu:
            modifiers.append('bu')
        if has_opt:
            modifiers.append('opt')
        if has_review:
            modifiers.append('review')
        
        # Формируем полный интент
        if modifiers:
            full_intent = f"{base_intent}_{'_'.join(modifiers)}"
        else:
            full_intent = base_intent
        
        return {
            'base_intent': base_intent,
            'full_intent': full_intent,
            'confidence': confidence,
            'commercial_score': comm_count,
            'info_score': info_count,
            'domain_commercial_ratio': domain_comm_ratio,
            'domain_info_ratio': domain_info_ratio,
            'has_geo': has_geo,
            'has_work': has_work,
            'has_handmade': has_handmade,
            'has_bu': has_bu,
            'has_opt': has_opt,
            'has_review': has_review,
            'modifiers': modifiers,
            'scores': {
                'commercial': comm_count,
                'informational': info_count,
                'geo': geo_count,
                'work': work_count,
                'handmade': handmade_count,
                'bu': bu_count,
                'opt': opt_count,
                'review': review_count,
                'domain_boost': domain_intent_boost
            },
            'found_words': {
                'commercial': comm_words[:5],
                'informational': info_words[:5],
                'geo': geo_words_found[:3],
                'work': work_words_found[:3],
                'handmade': handmade_words_found[:3],
                'bu': bu_words_found[:3],
                'opt': opt_words_found[:3],
                'review': review_words_found[:3]
            },
            'documents_analyzed': docs_analyzed
        }

