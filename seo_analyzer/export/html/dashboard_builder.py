"""Построение HTML дашборда"""

from pathlib import Path
from typing import Dict, Optional
import pandas as pd

from .stats_collector import collect_stats, collect_clusters_data
from .style_manager import get_css_styles
from .template_engine import get_javascript, generate_html_template
from .section_generator import (
    generate_intent_section,
    generate_funnel_section,
    generate_clusters_section
)


def build_dashboard(
    df: pd.DataFrame,
    output_path: Path,
    stats: Optional[Dict] = None
) -> bool:
    """
    Строит HTML дашборд с результатами анализа
    
    Args:
        df: DataFrame с результатами
        output_path: Путь для сохранения
        stats: Статистика анализа (опционально)
        
    Returns:
        True если успешно
    """
    try:
        print(f"💾 Генерация HTML дашборда: {output_path.name}...")
        
        # Собираем статистику
        if stats is None:
            stats = collect_stats(df)
        
        # Собираем данные по кластерам
        clusters_data = collect_clusters_data(df)
        
        # Генерируем секции
        intent_section = generate_intent_section(stats)
        funnel_section = generate_funnel_section(stats)
        clusters_section = generate_clusters_section(clusters_data)
        
        # Получаем стили и скрипты
        css_styles = get_css_styles()
        javascript = get_javascript()
        
        # Генерируем HTML
        html = generate_html_template(
            stats=stats,
            intent_section=intent_section,
            funnel_section=funnel_section,
            clusters_section=clusters_section,
            css_styles=css_styles,
            javascript=javascript
        )
        
        # Сохраняем
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"✓ HTML дашборд создан: {output_path}")
        return True
        
    except Exception as e:
        print(f"⚠️ Ошибка генерации HTML: {e}")
        return False

