"""Построитель ценовой информации для кластера"""

from typing import Dict
import pandas as pd


class PriceInfoBuilder:
    """Добавляет ценовую информацию для коммерческих запросов"""
    
    def add(self, cluster_info: Dict, df: pd.DataFrame):
        """
        Добавляет ценовую информацию в cluster_info
        
        Args:
            cluster_info: Словарь с информацией о кластере
            df: DataFrame кластера
        """
        if 'serp_avg_price' not in df.columns:
            return
        
        # Берем только запросы с ценами
        prices_df = df[df['serp_avg_price'].notna()]
        
        if len(prices_df) == 0:
            return
        
        # Добавляем усредненные данные
        self._add_aggregated_prices(cluster_info, prices_df, df)
        
        # Добавляем статистику
        self._add_price_stats(cluster_info, prices_df, df)
    
    def _add_aggregated_prices(self, cluster_info: Dict, prices_df: pd.DataFrame, df: pd.DataFrame):
        """Добавляет агрегированные ценовые данные"""
        # Средняя цена
        avg_price = pd.to_numeric(prices_df['serp_avg_price'], errors='coerce').mean()
        if not pd.isna(avg_price):
            cluster_info['serp_avg_price'] = round(avg_price, 2)
        
        # Медианная цена
        if 'serp_median_price' in df.columns:
            median_prices = pd.to_numeric(prices_df['serp_median_price'], errors='coerce').dropna()
            if len(median_prices) > 0:
                cluster_info['serp_median_price'] = round(median_prices.mean(), 2)
        
        # Диапазон цен
        if 'serp_min_price' in df.columns:
            min_prices = pd.to_numeric(prices_df['serp_min_price'], errors='coerce').dropna()
            if len(min_prices) > 0:
                cluster_info['serp_min_price'] = int(min_prices.min())
        
        if 'serp_max_price' in df.columns:
            max_prices = pd.to_numeric(prices_df['serp_max_price'], errors='coerce').dropna()
            if len(max_prices) > 0:
                cluster_info['serp_max_price'] = int(max_prices.max())
        
        # Валюта
        if 'serp_currency' in df.columns:
            currencies = prices_df['serp_currency'].mode()
            if len(currencies) > 0:
                cluster_info['serp_currency'] = str(currencies.iloc[0])
        
        # Среднее количество предложений
        if 'serp_offers_count' in df.columns:
            offers_count = pd.to_numeric(prices_df['serp_offers_count'], errors='coerce').dropna()
            if len(offers_count) > 0:
                cluster_info['serp_offers_count'] = round(offers_count.mean(), 1)
        
        # Среднее количество предложений со скидками
        if 'serp_offers_with_discount' in df.columns:
            offers_with_discount = pd.to_numeric(prices_df['serp_offers_with_discount'], errors='coerce').dropna()
            if len(offers_with_discount) > 0:
                cluster_info['serp_offers_with_discount'] = round(offers_with_discount.mean(), 1)
        
        # Средний процент скидки
        if 'serp_avg_discount_percent' in df.columns:
            discounts = pd.to_numeric(prices_df['serp_avg_discount_percent'], errors='coerce').dropna()
            if len(discounts) > 0:
                cluster_info['serp_avg_discount_percent'] = round(discounts.mean(), 1)
    
    def _add_price_stats(self, cluster_info: Dict, prices_df: pd.DataFrame, df: pd.DataFrame):
        """Добавляет статистику по ценам"""
        price_stats = {}
        
        # Количество запросов с ценами
        price_stats['queries_with_prices'] = len(prices_df)
        
        # Процент запросов с ценами
        price_stats['price_coverage'] = round(len(prices_df) / len(df) * 100, 1)
        
        # Процент предложений со скидками
        if 'serp_offers_with_discount' in df.columns and 'serp_offers_count' in df.columns:
            offers_with_discount_num = pd.to_numeric(prices_df['serp_offers_with_discount'], errors='coerce')
            offers_count_num = pd.to_numeric(prices_df['serp_offers_count'], errors='coerce')
            discount_df = prices_df[offers_count_num > 0]
            if len(discount_df) > 0:
                discount_ratio = (pd.to_numeric(discount_df['serp_offers_with_discount'], errors='coerce') / 
                                 pd.to_numeric(discount_df['serp_offers_count'], errors='coerce')).mean()
                if not pd.isna(discount_ratio):
                    price_stats['discount_ratio'] = round(discount_ratio * 100, 1)
        
        # Добавляем статистику
        if price_stats:
            cluster_info['price_stats'] = price_stats

