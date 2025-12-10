"""
Page Mapper
Маппер запросов на типы и URL целевых страниц
"""

from typing import Dict, List, Optional

from .type_determiner import PageType, PageTypeDeterminer


class PageMapper:
    """Маппер запросов на типы и URL целевых страниц"""
    
    def __init__(self):
        """Инициализация маппера"""
        self.type_determiner = PageTypeDeterminer()
        
        # Шаблоны URL
        self.url_templates = {
            PageType.MAIN: "/",
            PageType.CATEGORY: "/{category}/",
            PageType.SUBCATEGORY: "/{category}/{subcategory}/",
            PageType.PRODUCT: "/{category}/{product}/",
            PageType.COMMERCIAL: "/{category}/kupit/",
            PageType.GEO: "/{category}/{city}/",
            PageType.ARTICLE: "/articles/{slug}/",
            PageType.BRAND: "/{category}/brand/{brand}/",
        }
    
    def determine_page_type(
        self,
        query: str,
        intent: str,
        has_brand: bool = False,
        has_geo: bool = False,
        funnel_stage: Optional[str] = None
    ) -> str:
        """Определяет тип целевой страницы"""
        return self.type_determiner.determine(
            query, intent, has_brand, has_geo, funnel_stage
        )
    
    def generate_url(
        self,
        page_type: str,
        query: str,
        category: str = "skud",
        subcategory: Optional[str] = None,
        city: Optional[str] = None,
        brand: Optional[str] = None
    ) -> str:
        """Генерирует предложенный URL"""
        try:
            page_type_enum = PageType(page_type)
        except ValueError:
            page_type_enum = PageType.CATEGORY
        
        template = self.url_templates.get(page_type_enum, "/{category}/")
        slug = self._create_slug(query)
        
        url = template.format(
            category=category,
            subcategory=subcategory or slug,
            product=slug,
            city=city or "moscow",
            brand=self._create_slug(brand) if brand else "brands",
            slug=slug
        )
        
        return url
    
    def _create_slug(self, text: str) -> str:
        """Создает URL-slug из текста"""
        if not text:
            return "page"
        
        translit_map = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd',
            'е': 'e', 'ё': 'e', 'ж': 'zh', 'з': 'z', 'и': 'i',
            'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
            'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't',
            'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch',
            'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '',
            'э': 'e', 'ю': 'yu', 'я': 'ya', ' ': '-'
        }
        
        slug = text.lower()
        result = []
        
        for char in slug:
            if char in translit_map:
                result.append(translit_map[char])
            elif char.isalnum() or char in '-_':
                result.append(char)
        
        slug = ''.join(result)
        while '--' in slug:
            slug = slug.replace('--', '-')
        
        slug = slug.strip('-')
        return slug[:100]
    
    def map_query_to_page(
        self,
        query: str,
        intent: Optional[str] = None,
        has_brand: bool = False,
        has_geo: bool = False,
        funnel_stage: Optional[str] = None,
        brand: Optional[str] = None,
        city: Optional[str] = None
    ) -> Dict[str, str]:
        """Полное определение целевой страницы для запроса"""
        page_type = self.determine_page_type(query, intent, has_brand, has_geo, funnel_stage)
        suggested_url = self.generate_url(page_type, query, category="skud", brand=brand, city=city)
        return {'target_page_type': page_type, 'suggested_url': suggested_url}
    
    def map_batch(
        self,
        queries: List[str],
        intents: Optional[List[str]] = None,
        brands: Optional[List[str]] = None,
        geos: Optional[List[bool]] = None
    ) -> List[Dict[str, str]]:
        """Пакетное определение страниц"""
        results = []
        
        for i, query in enumerate(queries):
            intent = intents[i] if intents else None
            brand = brands[i] if brands else None
            has_geo = geos[i] if geos else False
            has_brand = brand is not None
            
            result = self.map_query_to_page(
                query, intent, has_brand, has_geo,
                brand=brand
            )
            result['query'] = query
            results.append(result)
        
        return results

