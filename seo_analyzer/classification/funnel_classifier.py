"""Классификация запросов по воронке продаж"""

from typing import Dict, List
from enum import Enum
from tqdm import tqdm


class FunnelStage(Enum):
    """Этапы воронки продаж"""
    AWARENESS = "Awareness"           # Осведомленность
    INTEREST = "Interest"             # Интерес
    CONSIDERATION = "Consideration"   # Рассмотрение
    INTENT = "Intent"                 # Намерение
    PURCHASE = "Purchase"             # Покупка


class FunnelClassifier:
    """Классификатор запросов по воронке продаж на основе файлов из keyword_group"""
    
    def __init__(self, commercial_words: set = None, info_words: set = None):
        """
        Инициализация классификатора
        
        Args:
            commercial_words: Коммерческие слова из commercial.txt
            info_words: Информационные слова из info.txt
        """
        self.commercial_words = commercial_words or set()
        self.info_words = info_words or set()
        
        # Разделяем информационные слова по этапам воронки
        awareness_keywords = [w for w in self.info_words if any(x in w.lower() for x in ['что', 'расшифровка', 'определение', 'означает'])]
        interest_keywords = [w for w in self.info_words if any(x in w.lower() for x in ['вид', 'тип', 'работа', 'принцип', 'устройство', 'схема'])]
        consideration_keywords = [w for w in self.info_words if any(x in w.lower() for x in ['обзор', 'сравнение', 'отличие', 'преимущество', 'лучш'])]
        
        # Разделяем коммерческие слова по этапам
        intent_keywords = [w for w in self.commercial_words if any(x in w.lower() for x in ['цена', 'стоимость', 'прайс'])]
        purchase_keywords = [w for w in self.commercial_words if any(x in w.lower() for x in ['купить', 'заказать', 'приобрести', 'продажа', 'доставка'])]
        
        # Паттерны для каждого этапа воронки (теперь из файлов)
        self.funnel_patterns = {
            FunnelStage.AWARENESS: {
                'keywords': awareness_keywords if awareness_keywords else ['что такое', 'расшифровка', 'определение'],
                'weight': 5,
            },
            FunnelStage.INTEREST: {
                'keywords': interest_keywords if interest_keywords else ['виды', 'как работает', 'принцип'],
                'weight': 4,
            },
            FunnelStage.CONSIDERATION: {
                'keywords': consideration_keywords if consideration_keywords else ['обзор', 'сравнение', 'отзывы'],
                'weight': 3,
            },
            FunnelStage.INTENT: {
                'keywords': intent_keywords if intent_keywords else ['цена', 'стоимость'],
                'weight': 2,
            },
            FunnelStage.PURCHASE: {
                'keywords': purchase_keywords if purchase_keywords else ['купить', 'заказать'],
                'weight': 1,
            },
        }
    
    def calculate_stage_scores(self, query: str) -> Dict[str, float]:
        """
        Вычисляет скоры для каждого этапа воронки
        
        Args:
            query: Запрос
            
        Returns:
            Словарь со скорами
        """
        query_lower = query.lower()
        scores = {}
        
        for stage, config in self.funnel_patterns.items():
            score = 0.0
            keywords = config['keywords']
            weight = config['weight']
            
            for keyword in keywords:
                if keyword in query_lower:
                    score += weight
            
            scores[stage.value] = score
        
        return scores
    
    def classify_funnel_stage(self, query: str) -> Dict[str, any]:
        """
        Определяет этап воронки для запроса
        
        Args:
            query: Запрос
            
        Returns:
            Словарь с информацией об этапе
        """
        scores = self.calculate_stage_scores(query)
        
        # Определяем максимальный скор
        max_score = max(scores.values())
        
        if max_score == 0:
            # Если нет явных маркеров, считаем Interest (средний этап)
            funnel_stage = FunnelStage.INTEREST.value
            confidence = 0.3
        else:
            # Находим этап с максимальным скором
            funnel_stage = max(scores.items(), key=lambda x: x[1])[0]
            # Уверенность = скор / максимально возможный скор для этого этапа
            confidence = min(max_score / 10.0, 1.0)
        
        return {
            'funnel_stage': funnel_stage,
            'funnel_confidence': confidence,
            'stage_scores': scores,
        }
    
    def get_stage_priority(self, stage: str) -> int:
        """
        Возвращает приоритет этапа (для сортировки)
        
        Args:
            stage: Название этапа
            
        Returns:
            Приоритет (1 - самый высокий)
        """
        priorities = {
            FunnelStage.PURCHASE.value: 1,
            FunnelStage.INTENT.value: 2,
            FunnelStage.CONSIDERATION.value: 3,
            FunnelStage.INTEREST.value: 4,
            FunnelStage.AWARENESS.value: 5,
        }
        return priorities.get(stage, 99)
    
    def classify_batch(self, queries: List[str]) -> List[Dict[str, any]]:
        """
        🚀 ОПТИМИЗИРОВАННАЯ версия: пакетная классификация с прогресс-баром
        
        Args:
            queries: Список запросов
            
        Returns:
            Список результатов
        """
        results = []
        
        # Показываем прогресс-бар только для больших датасетов
        show_progress = len(queries) >= 100
        iterator = tqdm(queries, desc="Классификация по воронке", disable=not show_progress)
        
        for query in iterator:
            result = self.classify_funnel_stage(query)
            result['query'] = query
            result['funnel_priority'] = self.get_stage_priority(result['funnel_stage'])
            results.append(result)
        
        return results
    
    def get_funnel_distribution(self, queries: List[str]) -> Dict[str, int]:
        """
        Получает распределение запросов по воронке
        
        Args:
            queries: Список запросов
            
        Returns:
            Словарь с количеством запросов на каждом этапе
        """
        distribution = {stage.value: 0 for stage in FunnelStage}
        
        for query in queries:
            stage_info = self.classify_funnel_stage(query)
            stage = stage_info['funnel_stage']
            distribution[stage] += 1
        
        return distribution

