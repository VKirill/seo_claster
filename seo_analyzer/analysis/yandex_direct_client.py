"""
Клиент для работы с Yandex Direct API (Forecast методы).

Получает прогнозы трафика, ставок CPC и данные о конкуренции
для ключевых слов через sandbox или production API.
"""

import requests
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path


def load_minus_words(file_path: str = "yandex_direct_minus_words.txt") -> List[str]:
    """
    Загрузка минус-слов из текстового файла.
    
    Args:
        file_path: Путь к файлу с минус-словами
        
    Returns:
        Список минус-слов
    """
    if not Path(file_path).exists():
        return []
    
    minus_words = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Пропускаем пустые строки и комментарии
                if line and not line.startswith('#'):
                    minus_words.append(line)
    except Exception as e:
        print(f"⚠️  Ошибка загрузки минус-слов из {file_path}: {e}")
        return []
    
    return minus_words


class YandexDirectClient:
    """Клиент для Yandex Direct API v4 (Live/JSON)."""
    
    # API endpoints - Forecast методы работают ТОЛЬКО через Sandbox URL!
    # https://yandex.ru/dev/direct/doc/dg-v4/concepts/forecast.html
    FORECAST_URL = "https://api-sandbox.direct.yandex.ru/live/v4/json/"
    
    # Лимиты API
    MAX_PHRASES_PER_REQUEST = 100  # API поддерживает до 100 фраз в одном запросе!
    REQUEST_DELAY = 0.1  # секунды между БАТЧАМИ (внутри батча задержки нет)
    
    def __init__(self, token: str, use_sandbox: bool = False, geo_id: int = 213,
                 minus_words_file: str = "yandex_direct_minus_words.txt"):
        """
        Инициализация клиента.
        
        Args:
            token: OAuth токен для Yandex Direct API
            use_sandbox: Параметр не используется (Forecast всегда через Sandbox URL)
            geo_id: ID региона (213 = Москва, 1 = Москва и область, 225 = Россия)
            minus_words_file: Путь к файлу с минус-словами
        """
        self.token = token
        # Forecast методы ВСЕГДА используют Sandbox URL (это особенность API)
        self.base_url = self.FORECAST_URL
        self.geo_id = geo_id
        self.last_request_time = 0
        
        # Загрузка минус-слов
        self.minus_words = load_minus_words(minus_words_file)
        if self.minus_words:
            print(f"ℹ️  Загружено минус-слов для Direct: {len(self.minus_words)}")
        else:
            print(f"ℹ️  Минус-слова для Direct не используются (файл пуст или не найден)")
        
    def _wait_for_rate_limit(self, skip_wait: bool = False):
        """
        Ожидание между запросами для соблюдения лимитов.
        
        Args:
            skip_wait: Пропустить ожидание (для операций внутри одного батча)
        """
        if skip_wait:
            self.last_request_time = time.time()
            return
            
        elapsed = time.time() - self.last_request_time
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()
        
    def _make_request(self, method: str, params: Dict, debug: bool = False, skip_wait: bool = False) -> Dict:
        """
        Выполнение запроса к API.
        
        Args:
            method: Название метода API
            params: Параметры запроса
            debug: Включить детальное логирование запроса/ответа
            skip_wait: Пропустить задержку (для операций внутри батча)
            
        Returns:
            Dict с ответом API
            
        Raises:
            Exception: При ошибках API
        """
        self._wait_for_rate_limit(skip_wait=skip_wait)
        
        # Используем токен И в заголовке И в теле (согласно документации Live API v4)
        payload = {
            "method": method,
            "token": self.token,  # Токен в теле запроса
            "param": params
        }
        
        # Детальное логирование запроса
        if debug:
            import json
            print("\n" + "="*80)
            print("🔍 DEBUG: Yandex Direct API Request")
            print("="*80)
            print(f"URL: {self.base_url}")
            print(f"Method: {method}")
            print(f"Authorization: Bearer {self.token[:10]}...")
            print("\nRequest Body:")
            print(json.dumps(payload, indent=2, ensure_ascii=False))
            print("="*80)
        
        # Отправляем данные как UTF-8 строку (API требует явную UTF-8 кодировку)
        import json as json_lib
        payload_str = json_lib.dumps(payload, ensure_ascii=False)
        payload_bytes = payload_str.encode('utf-8')
        
        response = requests.post(
            self.base_url,
            data=payload_bytes,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {self.token}"
            },
            timeout=30
        )
        
        # Детальное логирование ответа
        if debug:
            import json
            print("\n" + "="*80)
            print("🔍 DEBUG: Yandex Direct API Response")
            print("="*80)
            print(f"Status Code: {response.status_code}")
            print("\nResponse Body:")
            try:
                print(json.dumps(response.json(), indent=2, ensure_ascii=False))
            except:
                print(response.text)
            print("="*80 + "\n")
        
        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code} - {response.text}")
            
        data = response.json()
        
        if "error" in data:
            error_msg = data["error"].get("error_string", str(data["error"]))
            raise Exception(f"API returned error: {error_msg}")
            
        return data.get("data", {})
        
    def create_forecast(self, phrases: List[str], debug: bool = False, skip_wait: bool = False) -> int:
        """
        Создание прогноза для списка фраз.
        
        Args:
            phrases: Список ключевых фраз (до 100 штук)
            debug: Включить детальное логирование
            skip_wait: Пропустить задержку перед запросом
            
        Returns:
            ID созданного прогноза
        """
        if len(phrases) > self.MAX_PHRASES_PER_REQUEST:
            raise ValueError(f"Maximum {self.MAX_PHRASES_PER_REQUEST} phrases per request")
            
        # Фильтр: только фразы до 6 слов
        valid_phrases = [p for p in phrases if len(p.split()) <= 6]
        
        if not valid_phrases:
            raise ValueError("No valid phrases (must be <= 6 words)")
            
        params = {
            "Phrases": valid_phrases,
            "GeoID": [self.geo_id],
            "Currency": "RUB",
            "AuctionBids": "Yes"
        }
        
        # Добавляем минус-слова если они есть
        if self.minus_words:
            params["CommonMinusWords"] = self.minus_words
        
        result = self._make_request("CreateNewForecast", params, debug=debug, skip_wait=skip_wait)
        return result
        
    def get_forecast(self, forecast_id: int, debug: bool = False, skip_wait: bool = False, max_retries: int = 30) -> Dict:
        """
        Получение результатов прогноза с автоматическим ожиданием генерации.
        
        Args:
            forecast_id: ID прогноза из create_forecast()
            debug: Включить детальное логирование
            skip_wait: Пропустить задержку перед запросом
            max_retries: Максимальное количество попыток (по 3 сек каждая)
            
        Returns:
            Dict с данными прогноза
        """
        import time
        
        result = None
        
        for attempt in range(max_retries):
            result = self._make_request("GetForecast", forecast_id, debug=(debug and attempt == 0), skip_wait=True)
            
            # Проверяем error_code 74 = "Прогноз генерируется"
            if isinstance(result, dict) and result.get('error_code') == 74:
                if attempt == 0 and debug:
                    print(f"⏳ Прогноз {forecast_id} генерируется, ожидание (макс {max_retries * 3} сек)...")
                if debug and attempt > 0 and attempt % 5 == 0:
                    print(f"   Попытка {attempt + 1}/{max_retries}...")
                time.sleep(3)  # Ждём 3 секунды перед следующей попыткой
                continue
            
            # Проверяем что есть данные Phrases
            if isinstance(result, dict) and 'Phrases' in result:
                if debug and attempt > 0:
                    print(f"✅ Данные получены через {(attempt + 1) * 3} сек")
                return result
            
            # Если нет ошибки 74 и нет Phrases - возвращаем как есть
            if debug:
                print(f"⚠️  Получен неожиданный ответ (нет Phrases и нет error_code 74)")
            return result
        
        # Если так и не дождались - возвращаем последний результат
        if debug:
            print(f"⚠️  Не дождались генерации прогноза за {max_retries * 3} сек")
        return result if result is not None else {}
    
    def get_forecast_list(self, debug: bool = False) -> List[Dict]:
        """
        Получение списка всех прогнозов (сформированных и формируемых).
        
        Используется для проверки готовности прогнозов и очистки старых отчётов.
        API возвращает до 5 прогнозов, отсортированных по убыванию ForecastID.
        
        Args:
            debug: Включить детальное логирование
            
        Returns:
            List[Dict]: Список объектов ForecastStatusInfo с полями:
                - ForecastID: int - идентификатор отчёта
                - StatusForecast: str - состояние (Done/Pending/Failed)
        """
        try:
            result = self._make_request("GetForecastList", None, debug=debug, skip_wait=True)
            return result if isinstance(result, list) else []
        except Exception as e:
            if debug:
                print(f"⚠️  Ошибка получения списка прогнозов: {e}")
            return []
    
    def delete_forecast(self, forecast_id: int, debug: bool = False, skip_wait: bool = False) -> bool:
        """
        Удаление прогноза после получения данных.
        
        Освобождает ресурсы API и очищает историю прогнозов.
        
        Args:
            forecast_id: ID прогноза для удаления
            debug: Включить детальное логирования
            skip_wait: Пропустить задержку перед запросом
            
        Returns:
            True если удаление успешно
        """
        try:
            self._make_request("DeleteForecastReport", forecast_id, debug=debug, skip_wait=skip_wait)
            if debug:
                print(f"✅ Прогноз {forecast_id} удален")
            return True
        except Exception as e:
            if debug:
                print(f"⚠️  Не удалось удалить прогноз {forecast_id}: {e}")
            return False
    
    def cleanup_old_forecasts(self, debug: bool = False) -> int:
        """
        Очистка всех старых прогнозов из системы.
        
        Получает список всех прогнозов и удаляет их.
        Рекомендуется вызывать перед началом сбора данных.
        
        Args:
            debug: Включить детальное логирование
            
        Returns:
            int: Количество удалённых прогнозов
        """
        forecasts = self.get_forecast_list(debug=debug)
        
        if not forecasts:
            if debug:
                print("✓ Нет старых прогнозов для удаления")
            return 0
        
        deleted_count = 0
        
        if debug:
            print(f"🗑️  Найдено {len(forecasts)} старых прогнозов, удаление...")
        
        for forecast in forecasts:
            forecast_id = forecast.get('ForecastID')
            status = forecast.get('StatusForecast')
            
            if forecast_id:
                if debug:
                    print(f"   Удаление прогноза {forecast_id} (статус: {status})...")
                
                if self.delete_forecast(forecast_id, debug=False, skip_wait=True):
                    deleted_count += 1
        
        if debug:
            print(f"✓ Удалено прогнозов: {deleted_count}/{len(forecasts)}")
        
        return deleted_count

