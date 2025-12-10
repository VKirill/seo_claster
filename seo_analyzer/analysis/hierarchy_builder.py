"""Построитель иерархии сайта на основе SERP и AI анализа"""

from typing import Dict, List
import pandas as pd
from .breadcrumb_aggregator import BreadcrumbAggregator
from .deepseek_hierarchy import DeepSeekHierarchyAnalyzer
from .hierarchy_formatter import HierarchyFormatter


class HierarchyBuilder:
    """Построение иерархии сайта из breadcrumbs и семантики"""
    
    def __init__(
        self, 
        deepseek_api_key: str,
        max_urls_per_query: int = 3,
        db_path = None,
        stop_domains_file = None,
        collect_breadcrumbs: bool = False,
        use_breadcrumbs: bool = False
    ):
        """
        Инициализация
        
        Args:
            deepseek_api_key: API ключ DeepSeek
            max_urls_per_query: Макс URL для сканирования (по умолчанию 3 для ТОП-3)
            db_path: Путь к БД
            stop_domains_file: Файл со стоп-доменами
            collect_breadcrumbs: Собирать ли breadcrumbs со страниц (False = не скачивать)
            use_breadcrumbs: Использовать ли breadcrumbs для построения иерархии
        """
        self.collect_breadcrumbs = collect_breadcrumbs
        self.use_breadcrumbs = use_breadcrumbs
        
        # Создаем aggregator только если нужны breadcrumbs
        if collect_breadcrumbs or use_breadcrumbs:
            self.aggregator = BreadcrumbAggregator(
                max_urls_per_query=max_urls_per_query,
                db_path=db_path,
                stop_domains_file=stop_domains_file
            )
        else:
            self.aggregator = None
            
        self.ai_analyzer = DeepSeekHierarchyAnalyzer(deepseek_api_key)
        self.formatter = HierarchyFormatter(self.ai_analyzer)
    
    def build_hierarchy_from_dataframe(
        self, 
        df: pd.DataFrame,
        use_clusters: bool = True
    ) -> Dict:
        """
        Построить иерархию из DataFrame с SERP данными
        
        Args:
            df: DataFrame с колонками 'keyword', 'serp_urls', 'semantic_cluster_id'
            use_clusters: Использовать кластеры для группировки
            
        Returns:
            Словарь с иерархией и метаданными
        """
        print("\n🏗️  Построение иерархии сайта...")
        
        breadcrumbs_dict = {}
        deduplicated = {}
        unique_hierarchies = set()
        
        # Шаг 1: Извлечение breadcrumbs (если включено)
        if self.collect_breadcrumbs and self.use_breadcrumbs and self.aggregator:
            breadcrumbs_dict = self.aggregator.extract_from_dataframe(df)
            
            if not breadcrumbs_dict:
                print("⚠️  Не удалось извлечь breadcrumbs, продолжаем без них")
            else:
                # Шаг 2: Дедупликация
                deduplicated = self.aggregator.deduplicate_breadcrumbs(breadcrumbs_dict)
                print(f"  ✓ Извлечено {len(deduplicated)} уникальных breadcrumbs")
                
                # Шаг 3: Получение уникальных иерархий
                unique_hierarchies = self.aggregator.get_unique_hierarchies(deduplicated)
                print(f"  ✓ Найдено {len(unique_hierarchies)} уникальных иерархий")
        else:
            print("  ℹ️  Сбор breadcrumbs отключен (collect_breadcrumbs=False или use_breadcrumbs=False)")
        
        # Шаг 4: AI анализ (работает с breadcrumbs или без них)
        all_hierarchies = self._analyze_hierarchies(df, deduplicated, unique_hierarchies, use_clusters)
        
        print("  ✓ AI анализ завершен")
        
        # Шаг 5: Объединение результатов
        result = {
            "success": True,
            "total_breadcrumbs": len(breadcrumbs_dict),
            "unique_breadcrumbs": len(deduplicated),
            "unique_hierarchies": len(unique_hierarchies),
            "hierarchies": all_hierarchies,
        }
        
        # Добавляем статистику только если aggregator доступен
        if self.aggregator:
            result["statistics"] = self.aggregator.get_hierarchy_stats(deduplicated)
        else:
            result["statistics"] = {}
        
        return result
    
    def _analyze_hierarchies(
        self,
        df: pd.DataFrame,
        deduplicated: Dict,
        unique_hierarchies: set,
        use_clusters: bool
    ) -> List[Dict]:
        """Анализ иерархий с помощью AI"""
        all_hierarchies = []
        
        if use_clusters and 'semantic_cluster_id' in df.columns:
            hierarchies_by_cluster = self._group_by_clusters(df, deduplicated)
            
            for cluster_id, cluster_data in hierarchies_by_cluster.items():
                print(f"  🤖 AI анализ кластера {cluster_id}...")
                
                breadcrumbs_list = cluster_data['breadcrumbs']
                semantic_context = cluster_data.get('context', '')
                
                ai_result = self.ai_analyzer.analyze_breadcrumbs(
                    breadcrumbs_list,
                    semantic_context
                )
                
                if 'hierarchy' in ai_result and ai_result['hierarchy']:
                    all_hierarchies.append({
                        'cluster_id': cluster_id,
                        'hierarchy': ai_result,
                        'context': semantic_context
                    })
        else:
            # Анализ всех breadcrumbs вместе
            print("  🤖 AI анализ всех breadcrumbs...")
            
            all_breadcrumbs = list(unique_hierarchies)
            breadcrumbs_list = [list(bc) for bc in all_breadcrumbs]
            
            ai_result = self.ai_analyzer.analyze_breadcrumbs(breadcrumbs_list)
            
            all_hierarchies = [{
                'cluster_id': 'all',
                'hierarchy': ai_result,
                'context': 'Вся семантика'
            }]
        
        return all_hierarchies
    
    def _group_by_clusters(self, df: pd.DataFrame, deduplicated: Dict) -> Dict:
        """Группировать breadcrumbs по кластерам"""
        clusters = {}
        
        for _, row in df.iterrows():
            keyword = row.get('keyword', '')
            cluster_id = row.get('semantic_cluster_id', -1)
            
            if keyword not in deduplicated:
                continue
            
            if cluster_id not in clusters:
                clusters[cluster_id] = {
                    'breadcrumbs': [],
                    'keywords': [],
                    'context': ''
                }
            
            breadcrumb = deduplicated[keyword]
            
            # Добавляем только если еще нет такого
            if breadcrumb not in clusters[cluster_id]['breadcrumbs']:
                clusters[cluster_id]['breadcrumbs'].append(breadcrumb)
            
            clusters[cluster_id]['keywords'].append(keyword)
        
        # Формируем контекст для каждого кластера
        for cluster_id, data in clusters.items():
            keywords = data['keywords'][:5]  # Топ 5 запросов
            data['context'] = f"Кластер {cluster_id}: {', '.join(keywords)}"
        
        return clusters
    
    def format_for_excel(self, hierarchy_result: Dict) -> pd.DataFrame:
        """Форматировать результат для Excel"""
        return self.formatter.format_for_excel(hierarchy_result)
