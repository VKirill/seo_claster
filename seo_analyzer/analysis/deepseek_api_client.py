"""
Клиент для работы с DeepSeek API.

Низкоуровневый модуль для выполнения запросов к API и парсинга ответов.
"""

import requests
import json
from typing import Dict, Tuple


class DeepSeekAPIClient:
    """Клиент для работы с DeepSeek API."""
    
    API_URL = "https://api.deepseek.com/v1/chat/completions"
    MODEL = "deepseek-chat"
    
    def __init__(self, api_key: str):
        """
        Инициализация клиента.
        
        Args:
            api_key: API ключ DeepSeek
        """
        self.api_key = api_key
    
    def call_api(self, system_prompt: str, user_message: str) -> Dict:
        """
        Вызов DeepSeek API.
        
        Args:
            system_prompt: Системный промпт
            user_message: Сообщение пользователя
            
        Returns:
            Dict: Ответ от API
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        payload = {
            "model": self.MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            "temperature": 1.0,  # Рекомендуемое значение для Data Analysis
            "max_tokens": 200
        }
        
        response = requests.post(
            self.API_URL,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        response.raise_for_status()
        return response.json()
    
    def parse_conversion_response(self, response: Dict) -> Tuple[float, float]:
        """
        Парсинг ответа от DeepSeek API для конверсии.
        
        Args:
            response: Ответ от API
            
        Returns:
            Tuple[lead_cost_rub, conversion_rate_percent]
            
        Raises:
            ValueError: Если не удалось распарсить ответ
        """
        try:
            content = response['choices'][0]['message']['content']
            
            # Парсим JSON из ответа
            data = json.loads(content)
            
            lead_cost = float(data.get('lead_cost_rub', 0))
            conversion_rate = float(data.get('conversion_rate_percent', 0))
            
            # Валидация
            if lead_cost <= 0 or conversion_rate <= 0:
                raise ValueError(f"Invalid values: lead_cost={lead_cost}, conversion_rate={conversion_rate}")
            
            return lead_cost, conversion_rate
            
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            raise ValueError(f"Failed to parse DeepSeek response: {e}")
