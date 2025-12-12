"""Выбор и фильтрация breadcrumbs"""

from typing import List, Dict, Set
from collections import Counter


class BreadcrumbSelector:
    """Выбор лучших breadcrumbs и статистика"""
    
    @staticmethod
    def deduplicate_breadcrumbs(
        breadcrumbs_dict: Dict[str, List[List[str]]]
    ) -> Dict[str, List[str]]:
        """
        Удалить дубликаты breadcrumbs и выбрать лучший
        
        Args:
            breadcrumbs_dict: Словарь с breadcrumbs
            
        Returns:
            Словарь {query: [лучший breadcrumb]}
        """
        deduplicated = {}
        
        for query, breadcrumbs_list in breadcrumbs_dict.items():
            if not breadcrumbs_list:
                continue
            
            # Выбираем самый длинный и информативный breadcrumb
            best = BreadcrumbSelector._select_best_breadcrumb(breadcrumbs_list)
            if best:
                deduplicated[query] = best
        
        return deduplicated
    
    @staticmethod
    def _select_best_breadcrumb(breadcrumbs_list: List[List[str]]) -> List[str]:
        """
        Выбрать лучший breadcrumb из списка
        
        Args:
            breadcrumbs_list: Список breadcrumbs
            
        Returns:
            Лучший breadcrumb
        """
        if not breadcrumbs_list:
            return []
        
        # Критерии выбора:
        # 1. Самый длинный (больше уровней)
        # 2. Не содержит "404", "error" и т.д.
        # 3. Уникальные элементы
        
        valid_breadcrumbs = []
        
        for bc in breadcrumbs_list:
            # Фильтруем плохие
            if any(bad in ' '.join(bc).lower() for bad in ['404', 'error', 'ошибка']):
                continue
            
            valid_breadcrumbs.append(bc)
        
        if not valid_breadcrumbs:
            return []
        
        # Сортируем по длине (убываение)
        valid_breadcrumbs.sort(key=len, reverse=True)
        
        return valid_breadcrumbs[0]
    
    @staticmethod
    def get_unique_hierarchies(
        deduplicated: Dict[str, List[str]]
    ) -> Set[tuple]:
        """
        Получить уникальные иерархии
        
        Args:
            deduplicated: Словарь с deduplicated breadcrumbs
            
        Returns:
            Множество уникальных иерархий (как tuple)
        """
        unique = set()
        
        for breadcrumb in deduplicated.values():
            if breadcrumb:
                unique.add(tuple(breadcrumb))
        
        return unique
    
    @staticmethod
    def get_hierarchy_stats(
        deduplicated: Dict[str, List[str]]
    ) -> Dict[str, int]:
        """
        Статистика по иерархиям
        
        Args:
            deduplicated: Словарь с deduplicated breadcrumbs
            
        Returns:
            Словарь {иерархия: количество}
        """
        counter = Counter()
        
        for breadcrumb in deduplicated.values():
            if breadcrumb:
                hierarchy_str = ' > '.join(breadcrumb)
                counter[hierarchy_str] += 1
        
        return dict(counter.most_common())





