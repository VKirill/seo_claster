"""Утилиты для применения локальных оверрайдов конфигурации."""

from typing import Any, Dict, Optional


def load_local_module():
    """Безопасно загружает config_local.py, если он есть рядом с проектом."""
    try:
        import config_local

        return config_local
    except ImportError:
        return None


def merge_overrides(base: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Поверхностно мерджит словари конфигурации с учетом частичных оверрайдов."""
    merged = {**base}
    if not override:
        return merged

    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value

    return merged

