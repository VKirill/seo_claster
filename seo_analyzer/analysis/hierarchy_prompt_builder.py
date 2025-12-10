"""Построение промптов для анализа иерархии"""

from typing import List, Optional
from pathlib import Path


class HierarchyPromptBuilder:
    """Построение промптов для DeepSeek AI"""
    
    # Путь к файлу с промптом
    PROMPT_FILE = Path(__file__).parent.parent.parent / "prompts" / "hierarchy_analysis_prompt.txt"
    
    @classmethod
    def load_prompt_template(cls) -> str:
        """
        Загрузить шаблон промпта из файла
        
        Returns:
            Текст шаблона промпта
        """
        try:
            if cls.PROMPT_FILE.exists():
                return cls.PROMPT_FILE.read_text(encoding='utf-8')
            else:
                # Фоллбэк на встроенный промпт если файл не найден
                print(f"⚠️  Файл промпта не найден: {cls.PROMPT_FILE}")
                print(f"   Используется встроенный промпт")
                return cls._get_default_prompt()
        except Exception as e:
            print(f"⚠️  Ошибка загрузки промпта из файла: {e}")
            print(f"   Используется встроенный промпт")
            return cls._get_default_prompt()
    
    @staticmethod
    def _get_default_prompt() -> str:
        """Встроенный промпт по умолчанию (если файл недоступен)"""
        return """Ты - эксперт по структуре сайтов и информационной архитектуре.

Проанализируй следующие breadcrumbs (хлебные крошки) с разных сайтов конкурентов:{SEMANTIC_CONTEXT}

BREADCRUMBS:
{BREADCRUMBS_LIST}

ЗАДАЧА:
На основе этих breadcrumbs построй оптимальную иерархию для нового сайта.

ТРЕБОВАНИЯ:
1. Удали дубликаты и объедини схожие категории
2. Создай логичную многоуровневую структуру (2-4 уровня)
3. Используй понятные названия категорий на русском языке
4. Выдели основные разделы (категории первого уровня)
5. Для каждой категории предложи подкатегории
6. Учти SEO-оптимизацию структуры

ФОРМАТ ОТВЕТА (строго JSON):
{{
  "hierarchy": [
    {{
      "level": 1,
      "name": "Название категории",
      "slug": "url-slug",
      "children": [
        {{
          "level": 2,
          "name": "Подкатегория",
          "slug": "url-slug",
          "children": []
        }}
      ]
    }}
  ],
  "recommendations": [
    "Рекомендация 1",
    "Рекомендация 2"
  ],
  "seo_notes": "Заметки по SEO"
}}

Верни ТОЛЬКО валидный JSON без дополнительного текста."""
    
    @classmethod
    def create_hierarchy_prompt(
        cls,
        breadcrumbs_list: List[List[str]],
        semantic_context: Optional[str] = None
    ) -> str:
        """
        Создать промпт для анализа иерархии
        
        Args:
            breadcrumbs_list: Список breadcrumbs
            semantic_context: Дополнительный контекст
            
        Returns:
            Текст промпта
        """
        # Загружаем шаблон из файла
        template = cls.load_prompt_template()
        
        # Форматируем breadcrumbs
        breadcrumbs_text = "\n".join([
            f"{i+1}. {' > '.join(bc)}" 
            for i, bc in enumerate(breadcrumbs_list)
        ])
        
        # Форматируем контекст
        context_text = f"\n\nСемантический контекст: {semantic_context}" if semantic_context else ""
        
        # Подставляем значения в шаблон
        prompt = template.replace("{BREADCRUMBS_LIST}", breadcrumbs_text)
        prompt = prompt.replace("{SEMANTIC_CONTEXT}", context_text)
        
        return prompt

