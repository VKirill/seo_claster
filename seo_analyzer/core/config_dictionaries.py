"""Настройки словарей классификации и географии."""

from typing import Any, Dict

KEYWORD_DICTIONARIES: Dict[str, Dict[str, Any]] = {
    "commercial": {"file": "commercial.txt", "weight": 3, "flag": "is_commercial"},
    "info": {"file": "info.txt", "weight": 4, "flag": "is_informational"},
    "opt": {"file": "opt.txt", "weight": 2, "flag": "is_wholesale"},
    "fast": {"file": "fast.txt", "weight": 1, "flag": "is_urgent"},
    "handmade": {"file": "handmade.txt", "weight": 1, "flag": "is_diy"},
    "funny": {"file": "funny.txt", "weight": 1, "flag": "is_entertainment"},
    "work": {"file": "work.txt", "weight": 1, "flag": "is_job_related"},
    "bu": {"file": "bu.txt", "weight": 1, "flag": "is_used"},
    "Review": {"file": "Review.txt", "weight": 2, "flag": "is_review"},
}

GEO_DICTIONARIES: Dict[str, str] = {
    "Russian": "Russian.txt",
    "Moscow": "Moscow.txt",
    "Kazakhstan": "Kazakhstan.txt",
    "Belarus": "Belarus.txt",
    "Ukraine": "Ukraine.txt",
    "Country": "Country.txt",
}

