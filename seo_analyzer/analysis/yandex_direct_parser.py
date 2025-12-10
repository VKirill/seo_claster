"""
Парсер данных из Yandex Direct API.

Извлекает и структурирует данные о трафике, CPC, CTR и конкуренции.
"""

from typing import Dict, List, Optional
from datetime import datetime


class YandexDirectParser:
    """Парсер ответов Yandex Direct API."""
    
    @staticmethod
    def parse_forecast_response(response: Dict) -> List[Dict]:
        """
        Парсинг ответа GetForecast.
        
        Args:
            response: Ответ от API метода GetForecast
            
        Returns:
            List[Dict]: Список данных по каждой фразе
        """
        results = []
        
        common_data = response.get("Common", {})
        phrases_data = response.get("Phrases", [])
        
        for phrase_info in phrases_data:
            parsed = YandexDirectParser._parse_phrase_data(phrase_info, common_data)
            results.append(parsed)
            
        return results
        
    @staticmethod
    def _parse_phrase_data(phrase_info: Dict, common_data: Dict) -> Dict:
        """Парсинг данных одной фразы."""
        # Очищаем фразу от минус-слов для удобочитаемости
        phrase_raw = phrase_info.get("Phrase", "")
        # Берем только первую часть до минус-слов
        phrase = phrase_raw.split(" -")[0] if " -" in phrase_raw else phrase_raw
        
        # Показы и клики
        shows = int(phrase_info.get("Shows", 0))
        clicks = int(phrase_info.get("Clicks", 0))
        
        # CTR
        ctr = float(phrase_info.get("CTR", 0))
        premium_ctr = float(phrase_info.get("PremiumCTR", 0))
        
        # Данные по первой позиции (из API напрямую)
        first_place_clicks = int(phrase_info.get("FirstPlaceClicks", 0))
        first_place_ctr = float(phrase_info.get("FirstPlaceCTR", 0))
        premium_clicks = int(phrase_info.get("PremiumClicks", 0))
        
        # CPC из аукционных ставок
        auction_bids = phrase_info.get("AuctionBids", [])
        cpc_data = YandexDirectParser._parse_auction_bids(auction_bids)
        
        # Первая позиция (P11) - самая дорогая Premium позиция
        first_place_bid = 0.0
        first_place_price = 0.0
        if auction_bids:
            # Ищем позицию P11 (первая Premium позиция)
            for bid in auction_bids:
                if bid.get("Position") == "P11":
                    first_place_bid = float(bid.get("Bid", 0))
                    first_place_price = float(bid.get("Price", 0))
                    break
        
        return {
            "phrase": phrase,
            "shows": shows,
            "clicks": clicks,
            "ctr": ctr,
            "premium_ctr": premium_ctr,
            "first_place_clicks": first_place_clicks,
            "first_place_ctr": first_place_ctr,
            "premium_clicks": premium_clicks,
            "min_cpc": cpc_data["min_cpc"],
            "avg_cpc": cpc_data["avg_cpc"],
            "max_cpc": cpc_data["max_cpc"],
            "recommended_cpc": cpc_data["recommended_cpc"],
            "competition_level": YandexDirectParser._calculate_competition(cpc_data, shows),
            "first_place_bid": first_place_bid,
            "first_place_price": first_place_price,
            "timestamp": datetime.now().isoformat()
        }
        
    @staticmethod
    def _parse_auction_bids(auction_bids: List[Dict]) -> Dict:
        """Извлечение данных о ставках из аукциона."""
        if not auction_bids:
            return {
                "min_cpc": 0.0,
                "avg_cpc": 0.0,
                "max_cpc": 0.0,
                "recommended_cpc": 0.0
            }
            
        prices = [float(bid.get("Price", 0)) for bid in auction_bids if bid.get("Price")]
        
        if not prices:
            return {
                "min_cpc": 0.0,
                "avg_cpc": 0.0,
                "max_cpc": 0.0,
                "recommended_cpc": 0.0
            }
            
        # Рекомендуемая ставка - обычно первая позиция Premium
        recommended = prices[0] if len(prices) > 0 else 0.0
        
        return {
            "min_cpc": min(prices),
            "avg_cpc": sum(prices) / len(prices),
            "max_cpc": max(prices),
            "recommended_cpc": recommended
        }
        
    @staticmethod
    def _calculate_competition(cpc_data: Dict, shows: int) -> str:
        """
        Определение уровня конкуренции.
        
        Args:
            cpc_data: Данные о ставках
            shows: Количество показов
            
        Returns:
            str: "low", "medium", "high"
        """
        avg_cpc = cpc_data["avg_cpc"]
        
        if avg_cpc == 0 or shows < 100:
            return "low"
        elif avg_cpc < 50:
            return "medium" if shows > 1000 else "low"
        elif avg_cpc < 150:
            return "medium" if shows > 5000 else "medium"
        else:
            return "high"

