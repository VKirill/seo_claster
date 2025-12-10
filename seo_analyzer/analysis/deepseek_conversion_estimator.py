"""
Оценка конверсии и стоимости лида через DeepSeek AI.

Анализирует топ-15 высокочастотных запросов для определения:
- Средней стоимости лида (lead_cost_rub)
- Процента конверсии (conversion_rate_percent)
"""

from typing import Dict, List, Optional, Tuple
from pathlib import Path
from .deepseek_api_client import DeepSeekAPIClient


class DeepSeekConversionEstimator:
    """Оценка конверсии через DeepSeek API."""
    
    def __init__(self, api_key: Optional[str] = None, top_n: int = 15):
        """
        Инициализация.
        
        Args:
            api_key: API ключ DeepSeek
            top_n: Количество ВЧ запросов для анализа (по умолчанию 15)
        """
        self.api_key = api_key
        self.top_n = top_n
        self.api_client = DeepSeekAPIClient(api_key) if api_key else None
        self.system_prompt = self._load_system_prompt()
        
    def _load_system_prompt(self) -> str:
        """Загрузка system prompt из файла."""
        prompt_file = Path("prompts/deepseek_lead_cost_analysis.txt")
        
        if prompt_file.exists():
            with open(prompt_file, 'r', encoding='utf-8') as f:
                return f.read()
        else:
            # Fallback prompt если файл не найден
            return """Ты — анализатор стоимости лидов. 
На основе списка поисковых запросов рассчитай стоимость лида и % конверсии.
Ответ предоставь ТОЛЬКО в формате JSON: 
{"lead_cost_rub": <число>, "conversion_rate_percent": <число>}"""
    
    def estimate_from_queries(
        self,
        queries: List[Dict[str, any]],
        frequency_column: str = 'frequency_exact'
    ) -> Tuple[float, float]:
        """
        Оценка конверсии на основе списка запросов.
        
        Args:
            queries: Список словарей с данными запросов
            frequency_column: Название колонки с частотой
            
        Returns:
            Tuple[lead_cost_rub, conversion_rate_percent]
        """
        if not self.api_key:
            return self._fallback_estimates()
        
        # Отбираем топ-N запросов по частоте
        top_queries = sorted(
            queries,
            key=lambda q: q.get(frequency_column, 0),
            reverse=True
        )[:self.top_n]
        
        if not top_queries:
            return self._fallback_estimates()
        
        # Формируем user message
        user_message = self._format_queries_message(top_queries, frequency_column)
        
        # Запрос к API
        try:
            result = self.api_client.call_api(self.system_prompt, user_message)
            return self.api_client.parse_conversion_response(result)
        except Exception as e:
            print(f"⚠️  DeepSeek API error: {e}, использую fallback значения")
            return self._fallback_estimates()
    
    def _format_queries_message(
        self,
        queries: List[Dict],
        frequency_column: str
    ) -> str:
        """Форматирование списка запросов для отправки в API."""
        lines = ["Список запросов:"]
        
        for i, q in enumerate(queries, 1):
            keyword = q.get('keyword', q.get('query', 'неизвестно'))
            freq = q.get(frequency_column, 0)
            lines.append(f"{i}. {keyword} - {freq} показов")
        
        return "\n".join(lines)
    
    
    def _fallback_estimates(self) -> Tuple[float, float]:
        """Fallback значения если API недоступен."""
        # Дефолтные значения для российского рынка
        lead_cost_rub = 2500  # средняя стоимость лида
        conversion_rate_percent = 2.0  # средний % конверсии
        
        return lead_cost_rub, conversion_rate_percent


def estimate_conversion_for_dataframe(
    df,
    api_key: Optional[str] = None,
    top_n: int = 15,
    frequency_column: str = 'frequency_exact'
) -> Tuple[float, float]:
    """
    Оценка конверсии для DataFrame с запросами.
    
    Args:
        df: DataFrame с запросами
        api_key: API ключ DeepSeek
        top_n: Количество ВЧ запросов для анализа
        frequency_column: Название колонки с частотой
        
    Returns:
        Tuple[lead_cost_rub, conversion_rate_percent]
    """
    estimator = DeepSeekConversionEstimator(api_key=api_key, top_n=top_n)
    
    # Конвертируем DataFrame в список словарей
    queries = df.to_dict('records')
    
    return estimator.estimate_from_queries(queries, frequency_column)

