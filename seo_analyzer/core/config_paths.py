"""Базовые пути проекта и вспомогательные функции."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
KEYWORD_GROUP_DIR = PROJECT_ROOT / "keyword_group"
KEYWORDS_STOP_DIR = PROJECT_ROOT / "keywords_stop"
# Не используется в текущем пайплайне, оставлено закомментированным:
# SEMANTIKA_DIR = PROJECT_ROOT / "semantika"
OUTPUT_DIR = PROJECT_ROOT / "output"


def get_output_dir() -> Path:
    """Создает директорию output при первом обращении и возвращает путь."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    return OUTPUT_DIR

