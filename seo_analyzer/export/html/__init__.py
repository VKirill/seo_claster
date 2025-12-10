"""HTML модули для генерации дашборда"""

from .dashboard_builder import build_dashboard
from .stats_collector import collect_stats, collect_clusters_data
from .style_manager import get_css_styles
from .template_engine import get_javascript, generate_html_template
from .section_generator import (
    generate_intent_section,
    generate_funnel_section,
    generate_clusters_section
)

__all__ = [
    'build_dashboard',
    'collect_stats',
    'collect_clusters_data',
    'get_css_styles',
    'get_javascript',
    'generate_html_template',
    'generate_intent_section',
    'generate_funnel_section',
    'generate_clusters_section',
]

