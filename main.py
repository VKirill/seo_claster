"""
SEO Analyzer - Основной pipeline (фасад для обратной совместимости)
Универсальный анализатор SEO запросов с полной кластеризацией
"""

import asyncio
from pipeline import SEOAnalyzer, main as pipeline_main


# Для обратной совместимости - экспортируем класс
__all__ = ['SEOAnalyzer']


# Точка входа остается в main.py для обратной совместимости
if __name__ == "__main__":
    asyncio.run(pipeline_main())
