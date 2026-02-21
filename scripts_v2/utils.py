"""
utils.py — Shared utilities for the scripts_v2 pipeline.
"""

from __future__ import annotations

import re
import time
from functools import wraps

import requests

# ── Italian month map ─────────────────────────────────────────────────────────

_MONTHS_IT = [
    "", "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
    "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre",
]


def parse_italian_date(day: str, month_name: str, year: str) -> str:
    """Convert Italian date parts to ISO YYYY-MM-DD."""
    months = {m: i for i, m in enumerate(_MONTHS_IT) if m}
    month_num = months.get(month_name.lower(), 1)
    return f"{year}-{month_num:02d}-{int(day):02d}"


def extract_law_references(titolo: str) -> list[dict]:
    """Parse law cross-references from a bill title.

    Detects two patterns:
      - Conversion:    "Conversione in legge del decreto-legge DD MONTH YYYY, n. N"
      - Modification:  "Modifiche/Integrazioni alla legge DD MONTH YYYY, n. N"

    Args:
        titolo: The titoloAtto string from the Normattiva API.

    Returns:
        list of dicts, each with keys:
          type      – "converts" | "modifies" | "integrates"
          act_type  – e.g. "decreto-legge", "decreto_legislativo"
          date      – ISO date string YYYY-MM-DD
          number    – law number as string
    """
    refs: list[dict] = []

    conversion_re = re.compile(
        r"Conversione\s+in\s+legge.*?del\s+(decreto-legge|decreto\s+legislativo)"
        r"\s+(\d{1,2})\s+(\w+)\s+(\d{4}),?\s*n\.\s*(\d+)",
        re.IGNORECASE,
    )
    modification_re = re.compile(
        r"(Modifiche|Integrazioni).*?(legge|decreto-legge|decreto\s+legislativo)"
        r"\s+(\d{1,2})\s+(\w+)\s+(\d{4}),?\s*n\.\s*(\d+)",
        re.IGNORECASE,
    )

    for m in conversion_re.finditer(titolo):
        act_type, day, month, year, number = m.groups()
        refs.append({
            "type":     "converts",
            "act_type": act_type.lower().replace(" ", "_"),
            "date":     parse_italian_date(day, month, year),
            "number":   number,
        })

    for m in modification_re.finditer(titolo):
        mod_type, act_type, day, month, year, number = m.groups()
        refs.append({
            "type":     "modifies" if mod_type.lower() == "modifiche" else "integrates",
            "act_type": act_type.lower().replace(" ", "_"),
            "date":     parse_italian_date(day, month, year),
            "number":   number,
        })

    return refs


def retry_request(max_retries: int = 3, initial_delay: float = 2.0, backoff_factor: float = 2.0):
    """Decorator: retry HTTP calls with exponential backoff on network errors.

    Args:
        max_retries:    How many times to retry before giving up.
        initial_delay:  Seconds to wait before the first retry.
        backoff_factor: Multiplier applied to the delay on each subsequent retry.

    Usage:
        @retry_request(max_retries=3, initial_delay=2)
        def my_fetch(session, url): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, ConnectionError, TimeoutError) as exc:
                    if attempt < max_retries:
                        print(f"  ⚠ Retry {attempt + 1}/{max_retries} after {delay}s: {str(exc)[:100]}")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        print(f"  ✗ Failed after {max_retries} retries: {str(exc)[:100]}")
                        raise
        return wrapper
    return decorator
