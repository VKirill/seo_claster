"""
Обработка извлечения ключевых фраз и NER
"""

import asyncio
from seo_analyzer.analysis.key_phrases_extractor import KeyPhrasesExtractor
from seo_analyzer.analysis.ner_extractor import NERExtractor


class ExtractionHandler:
    """Обработчик извлечения данных"""
    
    def __init__(self):
        """Инициализация обработчика"""
        self.key_phrases_extractor = KeyPhrasesExtractor()
        self.ner_extractor = NERExtractor()
    
    async def extract_key_phrases(self, queries_list, df, print_stage):
        """
        Извлечь ключевые фразы
        
        Args:
            queries_list: Список запросов
            df: DataFrame
            print_stage: Функция для логирования
            
        Returns:
            DataFrame с добавленными колонками
        """
        print_stage("🔄 Извлечение ключевых фраз...")
        
        if self.key_phrases_extractor.enabled:
            main_words_results = await asyncio.to_thread(
                lambda: [self.key_phrases_extractor.get_main_words_string(kw) for kw in queries_list]
            )
            key_phrase_results = await asyncio.to_thread(
                lambda: [self.key_phrases_extractor.get_key_phrase(kw) for kw in queries_list]
            )
            
            df['main_words'] = main_words_results
            df['key_phrase'] = key_phrase_results
            
            print_stage(f"✓ Извлечены ключевые фразы")
        else:
            print_stage(f"⚠️ Извлечение ключевых фраз недоступно")
        
        return df
    
    async def extract_ner(self, queries_list, df, print_stage):
        """
        Извлечь именованные сущности (NER)
        
        Args:
            queries_list: Список запросов
            df: DataFrame
            print_stage: Функция для логирования
            
        Returns:
            DataFrame с добавленными колонками
        """
        print_stage("🔄 Извлечение именованных сущностей (NER)...")
        
        if self.ner_extractor.enabled:
            ner_entities_results = await asyncio.to_thread(
                lambda: [self.ner_extractor.get_entities_string(kw) for kw in queries_list]
            )
            ner_locations_results = await asyncio.to_thread(
                lambda: [self.ner_extractor.get_locations_string(kw) for kw in queries_list]
            )
            
            df['ner_entities'] = ner_entities_results
            df['ner_locations'] = ner_locations_results
            
            print_stage(f"✓ Извлечены именованные сущности")
        else:
            df['ner_entities'] = ''
            df['ner_locations'] = ''
            print_stage(f"⚠️ NER извлечение недоступно (требует natasha)")
        
        return df

