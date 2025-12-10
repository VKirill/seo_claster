"""
Модуль расчета SEO метрик.

Включает различные формулы KEI, метрики сложности, конкуренции и ROI.
"""

from .kei_calculator import (
    kei_standard,
    kei_devaka,
    kei_base_exact_ratio
)

from .kei_direct_basic import (
    kei_direct_competition_score
)

from .kei_direct_advanced import (
    kei_direct_traffic_potential,
    kei_direct_budget_required
)

from .kei_direct_budget import (
    calculate_monthly_budget
)

from .soltyk_advanced_calculator import (
    kei_soltyk_effectiveness_coefficient
)

__all__ = [
    # Стандартные KEI
    'kei_standard',
    'kei_devaka',
    'kei_base_exact_ratio',
    # KEI с данными Yandex Direct
    'kei_direct_competition_score',
    'kei_direct_traffic_potential',
    'kei_direct_budget_required',
    # Soltyk продвинутые формулы
    'kei_soltyk_effectiveness_coefficient',
    # Бюджетирование
    'calculate_monthly_budget',
]
