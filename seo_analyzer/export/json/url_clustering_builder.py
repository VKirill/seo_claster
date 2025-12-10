"""Построитель информации о URL кластеризации"""

from typing import Dict, List
from collections import Counter
import pandas as pd


class URLClusteringBuilder:
    """Добавляет информацию о URL кластеризации"""
    
    def add(self, cluster_info: Dict, df: pd.DataFrame):
        """
        Добавляет информацию о URL в cluster_info
        
        Args:
            cluster_info: Словарь с информацией о кластере
            df: DataFrame кластера
        """
        # Проверяем наличие колонки с полными данными документов
        documents_col = 'serp_documents'
        has_full_documents = documents_col in df.columns
        
        # Используем serp_documents если доступно, иначе serp_main_pages
        if not has_full_documents:
            serp_col = 'serp_main_pages'
            if serp_col not in df.columns:
                return
        
        all_urls = []
        url_sets = []
        url_to_titles = {}  # Словарь для хранения title по URL
        url_to_snippets = {}  # Словарь для хранения snippet по URL
        
        for _, row in df.iterrows():
            if has_full_documents:
                # Извлекаем URL, title и snippet из полных данных документов
                documents = row[documents_col]
                if not documents:
                    continue
                    
                urls = []
                
                for doc in documents[:30]:  # Берем топ-30
                    if isinstance(doc, dict):
                        url = doc.get('url', '')
                        if url:
                            norm_url = self._normalize_url(url)
                            if norm_url:
                                urls.append(norm_url)
                                # Сохраняем title для этого URL
                                title = doc.get('title', '')
                                if title and norm_url not in url_to_titles:
                                    url_to_titles[norm_url] = title
                                # Сохраняем snippet для этого URL
                                snippet = doc.get('snippet', '')
                                if snippet and norm_url not in url_to_snippets:
                                    url_to_snippets[norm_url] = snippet
            else:
                # Старый способ - только URL без title
                urls = self._extract_urls(row[serp_col])
                urls = urls[:30]
            
            if urls:
                url_set = set(urls)
                url_sets.append(url_set)
                all_urls.extend(list(url_set))
        
        if not url_sets:
            return
            
        # URL которые есть во всех запросах (пересечение)
        common_urls = set.intersection(*url_sets)
        
        # Добавляем common_urls с title и snippet
        common_urls_list = []
        for url in sorted(list(common_urls)):
            url_info = {'url': url}
            if url in url_to_titles:
                url_info['title'] = url_to_titles[url]
            if url in url_to_snippets:
                url_info['snippet'] = url_to_snippets[url]
            common_urls_list.append(url_info)
        
        cluster_info['common_urls'] = common_urls_list
        
        # URL на основе которых скорее всего было объединение (частые)
        if all_urls:
            counter = Counter(all_urls)
            min_count = 2 if len(df) > 1 else 1
            
            popular = []
            for url, count in counter.most_common(15):
                if count >= min_count:
                    url_info = {
                        'url': url,
                        'count': count,
                        'percentage': int((count / len(df)) * 100)
                    }
                    # Добавляем title если есть
                    if url in url_to_titles:
                        url_info['title'] = url_to_titles[url]
                    # Добавляем snippet если есть
                    if url in url_to_snippets:
                        url_info['snippet'] = url_to_snippets[url]
                    popular.append(url_info)
            
            cluster_info['clustering_basis_urls'] = popular
    
    def _extract_urls(self, serp_data) -> List[str]:
        """Извлекает нормализованные URL"""
        urls = []
        if isinstance(serp_data, list):
            for url in serp_data:
                if isinstance(url, str):
                    norm = self._normalize_url(url)
                    if norm:
                        urls.append(norm)
        elif isinstance(serp_data, str):
            for url in serp_data.split(','):
                norm = self._normalize_url(url.strip())
                if norm:
                    urls.append(norm)
        return urls
    
    def _normalize_url(self, url: str) -> str:
        """Нормализует URL"""
        if not url:
            return ""
        url = url.replace('https://', '').replace('http://', '')
        url = url.split(' ')[0]
        url = url.replace('www.', '')
        if url.endswith('/'):
            url = url[:-1]
        return url.lower()

