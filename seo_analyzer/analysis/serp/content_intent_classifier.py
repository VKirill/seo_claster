"""
Определение интента на основе анализа контента SERP
Анализирует title, snippet, passages из выдачи и определяет преобладающий тип
"""

from typing import Dict, List
import re
from collections import Counter


class SERPContentIntentClassifier:
    """
    Классификатор интента на основе контента SERP
    
    Анализирует слова из title/snippet/passages документов в SERP
    и сравнивает с словарями commercial.txt и info.txt
    """
    
    def __init__(self, commercial_words: set, info_words: set):
        """
        Args:
            commercial_words: Набор коммерческих слов из commercial.txt
            info_words: Набор информационных слов из info.txt
        """
        self.commercial_words = {w.lower() for w in commercial_words}
        self.info_words = {w.lower() for w in info_words}
    
    def analyze_documents(
        self,
        documents: List[Dict],
        top_n: int = 30
    ) -> Dict[str, any]:
        """
        Анализирует документы из SERP
        
        Args:
            documents: Список документов с полями title, snippet, passages
            top_n: Количество документов для анализа (по умолчанию топ-30)
            
        Returns:
            Dict с результатами анализа:
            {
                'commercial_score': int,  # Количество коммерческих слов
                'info_score': int,        # Количество информационных слов
                'intent': str,            # 'commercial' или 'informational'
                'confidence': float,      # Уверенность 0-1
                'commercial_words_found': List[str],  # Найденные комм. слова
                'info_words_found': List[str],        # Найденные инфо слова
                'documents_analyzed': int             # Количество проанализированных документов
            }
        """
        # Собираем весь текст из топ-N документов
        all_text = []
        docs_analyzed = 0
        
        for doc in documents[:top_n]:
            title = doc.get('title', '') or ''
            snippet = doc.get('snippet', '') or ''
            passages = doc.get('passages', '') or ''
            
            # Объединяем все тексты
            doc_text = f"{title} {snippet} {passages}"
            all_text.append(doc_text)
            docs_analyzed += 1
        
        combined_text = ' '.join(all_text).lower()
        
        # Токенизируем (разбиваем на слова)
        words = re.findall(r'\b\w+\b', combined_text)
        word_set = set(words)
        
        # Ищем коммерческие слова
        commercial_found = []
        for word in self.commercial_words:
            # Проверяем точное совпадение или вхождение
            if word in word_set or any(word in w for w in words):
                commercial_found.append(word)
        
        # Ищем информационные слова
        info_found = []
        for word in self.info_words:
            if word in word_set or any(word in w for w in words):
                info_found.append(word)
        
        # Подсчитываем вхождения (с учетом повторов)
        commercial_score = sum(1 for word in words if any(cw in word for cw in self.commercial_words))
        info_score = sum(1 for word in words if any(iw in word for iw in self.info_words))
        
        # Определяем интент
        total_score = commercial_score + info_score
        
        if total_score == 0:
            # Нет явных маркеров - считаем коммерческим (как раньше)
            intent = 'commercial'
            confidence = 0.5
        elif commercial_score > info_score:
            intent = 'commercial'
            confidence = commercial_score / total_score
        elif info_score > commercial_score:
            intent = 'informational'
            confidence = info_score / total_score
        else:
            # Равные скоры - берем коммерческий
            intent = 'commercial'
            confidence = 0.5
        
        return {
            'commercial_score': commercial_score,
            'info_score': info_score,
            'intent': intent,
            'confidence': confidence,
            'commercial_words_found': commercial_found[:30],  # Топ-30
            'info_words_found': info_found[:30],  # Топ-30
            'total_words_analyzed': len(words),
            'documents_analyzed': docs_analyzed
        }
    
    def classify_serp_intent(
        self,
        documents: List[Dict],
        threshold: float = 0.6
    ) -> str:
        """
        Упрощенный метод - возвращает только интент
        
        Args:
            documents: Список документов из SERP
            threshold: Порог уверенности (по умолчанию 0.6)
            
        Returns:
            'commercial' или 'informational'
        """
        result = self.analyze_documents(documents)
        
        # Если уверенность низкая - возвращаем commercial по умолчанию
        if result['confidence'] < threshold:
            return 'commercial'
        
        return result['intent']

