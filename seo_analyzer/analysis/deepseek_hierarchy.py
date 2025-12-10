"""Интеграция с DeepSeek API для анализа иерархии сайта"""

from typing import List, Dict, Optional
from .deepseek_api_client import DeepSeekAPIClient
from .hierarchy_prompt_builder import HierarchyPromptBuilder


class DeepSeekHierarchyAnalyzer:
    """Анализ иерархии сайта с помощью DeepSeek AI"""
    
    def __init__(self, api_key: str):
        """
        Инициализация
        
        Args:
            api_key: API ключ DeepSeek
        """
        self.api_client = DeepSeekAPIClient(api_key)
        self.prompt_builder = HierarchyPromptBuilder()
    
    def analyze_breadcrumbs(
        self, 
        breadcrumbs_list: List[List[str]],
        semantic_context: Optional[str] = None
    ) -> Dict:
        """
        Проанализировать breadcrumbs и построить иерархию
        
        Args:
            breadcrumbs_list: Список breadcrumbs с разных сайтов
            semantic_context: Дополнительный контекст (например, семантика кластера)
            
        Returns:
            Словарь с иерархией и рекомендациями
        """
        if not breadcrumbs_list:
            return {"error": "Нет breadcrumbs для анализа"}
        
        # Формируем промпт
        prompt = self.prompt_builder.create_hierarchy_prompt(
            breadcrumbs_list, 
            semantic_context
        )
        
        try:
            # Вызываем API
            response = self.api_client.chat_completion(
                prompt,
                system_message="Ты - эксперт по структуре сайтов и SEO. Отвечай только валидным JSON."
            )
            
            if response:
                # Парсим ответ
                hierarchy_data = self.api_client.parse_json_response(response)
                return hierarchy_data
            else:
                return {"error": "Не удалось получить ответ от DeepSeek"}
        
        except Exception as e:
            print(f"⚠️  Ошибка DeepSeek API: {e}")
            return {"error": str(e)}
    
    def format_hierarchy_for_excel(self, hierarchy_data: Dict) -> List[Dict]:
        """
        Форматировать иерархию для Excel
        
        Args:
            hierarchy_data: Данные иерархии от DeepSeek
            
        Returns:
            Список строк для Excel
        """
        rows = []
        
        hierarchy = hierarchy_data.get('hierarchy', [])
        
        def flatten_hierarchy(items: List[Dict], parent_path: str = ""):
            """Рекурсивно преобразовать иерархию в плоский список"""
            for item in items:
                level = item.get('level', 1)
                name = item.get('name', '')
                slug = item.get('slug', '')
                
                full_path = f"{parent_path} > {name}" if parent_path else name
                
                rows.append({
                    'Уровень': level,
                    'Название': name,
                    'URL slug': slug,
                    'Полный путь': full_path
                })
                
                # Рекурсия для детей
                children = item.get('children', [])
                if children:
                    flatten_hierarchy(children, full_path)
        
        flatten_hierarchy(hierarchy)
        
        return rows
