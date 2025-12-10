"""Этап 5: Генерация падежных форм"""

from seo_analyzer.core.forms_generator import FormsGenerator


async def generate_forms_stage(args, analyzer):
    """
    Генерация падежных форм
    
    Args:
        args: Аргументы командной строки
        analyzer: Экземпляр SEOAnalyzer
        
    Returns:
        None (данные сохраняются в analyzer)
    """
    print("📝 ЭТАП 8: Генерация словоформ")
    print("-" * 80)
    
    # Проверяем что DataFrame не пустой
    if len(analyzer.df) == 0:
        print("⚠️  DataFrame пустой, пропускаем генерацию словоформ")
        print()
        return
    
    print("🔄 Генерация падежных форм (это может занять время)...")
    
    analyzer.forms_generator = FormsGenerator()
    
    # Генерируем формы для топ запросов (для экономии времени)
    top_n = min(1000, len(analyzer.df))
    top_queries = analyzer.df.nlargest(top_n, 'frequency_world')['keyword'].tolist()
    
    forms_results = analyzer.forms_generator.generate_forms_batch(top_queries)
    
    # Создаем словарь для быстрого поиска
    forms_dict = {r['original']: r['forms'] for r in forms_results}
    
    # Добавляем формы в DataFrame
    for case_name in ['nominative', 'genitive', 'dative', 'accusative', 
                      'instrumental', 'prepositional']:
        analyzer.df[f'form_{case_name}'] = analyzer.df['keyword'].map(
            lambda x: forms_dict.get(x, {}).get(case_name, x)
        )
    
    print(f"✓ Формы сгенерированы для {len(forms_dict)} запросов")
    print()

