"""
Анализ контекста появления слов
"""

from typing import List, Dict, Set


class ContextAnalyzer:
    """Анализатор контекста"""
    
    @staticmethod
    def analyze_contexts(queries: List[str]) -> Dict[str, Set[str]]:
        """
        Анализирует контексты появления слов
        
        Args:
            queries: Список запросов
            
        Returns:
            Словарь слово → набор контекстов (соседние слова)
        """
        context_counter = {}
        
        for query in queries:
            words = query.split()
            
            for i, word in enumerate(words):
                if len(word) <= 2:
                    continue
                
                word_lower = word.lower()
                
                if word_lower not in context_counter:
                    context_counter[word_lower] = set()
                
                # Добавляем предыдущее и следующее слово как контекст
                if i > 0:
                    context_counter[word_lower].add(words[i-1].lower())
                if i < len(words) - 1:
                    context_counter[word_lower].add(words[i+1].lower())
        
        return context_counter

