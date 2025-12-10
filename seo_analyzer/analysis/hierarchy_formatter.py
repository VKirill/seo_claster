"""Форматирование результатов иерархии для экспорта"""

from typing import Dict
import pandas as pd


class HierarchyFormatter:
    """Форматирование иерархии для различных форматов экспорта"""
    
    def __init__(self, ai_analyzer):
        """
        Инициализация
        
        Args:
            ai_analyzer: Экземпляр DeepSeekHierarchyAnalyzer
        """
        self.ai_analyzer = ai_analyzer
    
    def format_for_excel(self, hierarchy_result: Dict) -> pd.DataFrame:
        """
        Форматировать результат для Excel
        
        Args:
            hierarchy_result: Результат build_hierarchy_from_dataframe
            
        Returns:
            DataFrame для Excel
        """
        if not hierarchy_result.get('success'):
            return pd.DataFrame([{
                'Ошибка': hierarchy_result.get('error', 'Неизвестная ошибка')
            }])
        
        rows = []
        
        hierarchies = hierarchy_result.get('hierarchies', [])
        
        for h in hierarchies:
            cluster_id = h.get('cluster_id', 'unknown')
            hierarchy_data = h.get('hierarchy', {})
            context = h.get('context', '')
            
            # Форматируем иерархию
            formatted = self.ai_analyzer.format_hierarchy_for_excel(hierarchy_data)
            
            for row in formatted:
                row['Кластер'] = cluster_id
                row['Контекст'] = context
                rows.append(row)
            
            # Добавляем рекомендации
            recommendations = hierarchy_data.get('recommendations', [])
            if recommendations:
                rows.append({
                    'Кластер': cluster_id,
                    'Контекст': context,
                    'Уровень': '',
                    'Название': 'РЕКОМЕНДАЦИИ',
                    'URL slug': '',
                    'Полный путь': '\n'.join(recommendations)
                })
            
            # Добавляем SEO заметки
            seo_notes = hierarchy_data.get('seo_notes', '')
            if seo_notes:
                rows.append({
                    'Кластер': cluster_id,
                    'Контекст': context,
                    'Уровень': '',
                    'Название': 'SEO ЗАМЕТКИ',
                    'URL slug': '',
                    'Полный путь': seo_notes
                })
        
        if not rows:
            return pd.DataFrame([{
                'Информация': 'Нет данных для отображения'
            }])
        
        return pd.DataFrame(rows)


