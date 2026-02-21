"""
Normattiva.it API functions

This module provides functions for interacting with the Normattiva.it API:
- Fetch law permalinks and metadata
- Fetch full text HTML via export endpoint
- Search for laws using ricerca/semplice API
"""

import requests
import time
from functools import wraps

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
NORMATTIVA_SITE = "https://www.normattiva.it"
HEADERS = {"Content-Type": "application/json"}


def retry_request(max_retries=3, initial_delay=2):
    """Decorator to retry HTTP requests with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from lib.state import REQUEST_FAILURES

            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        print(f"    Retry {attempt + 1}/{max_retries - 1} after {delay}s...")
                        time.sleep(delay)
                        delay *= 2
                    else:
                        # Track failure
                        REQUEST_FAILURES.append({
                            "function": func.__name__,
                            "args": str(args)[:100],
                            "error": str(e)[:200],
                            "attempts": max_retries
                        })

            raise last_exception

        return wrapper
    return decorator


@retry_request(max_retries=3, initial_delay=2)
def fetch_normattiva_permalink(session, data_gu: str, codice: str) -> dict:
    """Fetch permalink and lavori preparatori URL from normattiva.it.

    Note: This function still needs to fetch the HTML page because the API
    doesn't return the lavori_preparatori URL. However, for data_emanazione
    and other metadata, the API response (from search) already has this data.

    Args:
        session: Requests session
        data_gu: Date of Gazzetta Ufficiale publication (YYYY-MM-DD)
        codice: Codice redazionale (e.g., 24G00231)

    Returns:
        dict: Metadata including permalink and lavori_preparatori URL
    """
    from bs4 import BeautifulSoup

    permalink_url = (
        f"{NORMATTIVA_SITE}/atto/caricaDettaglioAtto"
        f"?atto.dataPubblicazioneGazzetta={data_gu}"
        f"&atto.codiceRedazionale={codice}"
    )

    resp = session.get(permalink_url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Extract lavori preparatori link (not available in API)
    lavori_preparatori = ""
    lavori_link = soup.find("a", string=lambda text: text and "lavori preparatori" in text.lower())
    if lavori_link and lavori_link.get("href"):
        lavori_preparatori = lavori_link["href"]

    return {
        "permalink": permalink_url,
        "lavori_preparatori": lavori_preparatori
    }


@retry_request(max_retries=3, initial_delay=2)
def fetch_full_text_via_export(session, data_gu: str, codice: str) -> str:
    """Fetch complete act HTML using the export endpoint.

    This replaces article-by-article scraping with a single request
    that returns all articles in one response.

    Args:
        session: Requests session
        data_gu: Date of Gazzetta Ufficiale publication (YYYY-MM-DD)
        codice: Codice redazionale (e.g., 24G00231)

    Returns:
        str: Complete HTML with all articles
    """
    if not data_gu or not codice:
        return ""

    export_url = (
        f"{NORMATTIVA_SITE}/esporta/attoCompleto"
        f"?atto.dataPubblicazioneGazzetta={data_gu}"
        f"&atto.codiceRedazionale={codice}"
    )

    resp = session.get(export_url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _parse_structured_query(search_text: str) -> dict | None:
    """Parse a structured search query to extract date, number, and type.

    Detects patterns like:
    - "decreto-legge 27 dicembre 2025 n. 196"
    - "legge 15 marzo 2024 n. 42"

    Args:
        search_text: The search query

    Returns:
        dict: Parsed data with date, number, type, or None if not structured
    """
    import re
    from datetime import datetime

    # Pattern: (type) (day) (month) (year) n. (number)
    pattern = r"(decreto-legge|decreto\s+legislativo|legge)\s+(\d{1,2})\s+(\w+)\s+(\d{4})[,\s]*n\.\s*(\d+)"
    match = re.search(pattern, search_text, re.IGNORECASE)

    if not match:
        return None

    act_type, day, month_name, year, number = match.groups()

    # Parse Italian date
    months = {
        'gennaio': 1, 'febbraio': 2, 'marzo': 3, 'aprile': 4,
        'maggio': 5, 'giugno': 6, 'luglio': 7, 'agosto': 8,
        'settembre': 9, 'ottobre': 10, 'novembre': 11, 'dicembre': 12
    }
    month_num = months.get(month_name.lower())

    if not month_num:
        return None

    try:
        date_obj = datetime(int(year), month_num, int(day))
        return {
            'type': act_type.lower().replace(' ', '_'),
            'date': date_obj.strftime('%Y-%m-%d'),
            'number': number,
            'year': year,
            'month': str(month_num),
            'day': day
        }
    except ValueError:
        return None


def _search_with_ricerca_avanzata(parsed: dict, max_results: int = 10) -> list:
    """Search using ricerca/avanzata with precise date filters.

    Args:
        parsed: Parsed query dict with date, number, type
        max_results: Maximum results to return

    Returns:
        list: Matching atti
    """
    search_url = f"{BASE_URL}/ricerca/avanzata"

    # Search on the specific day only
    payload = {
        "dataInizioPubProvvedimento": parsed['date'],
        "dataFinePubProvvedimento": parsed['date'],
        "paginazione": {
            "paginaCorrente": 1,
            "numeroElementiPerPagina": max_results
        }
    }

    try:
        session = requests.Session()
        resp = session.post(search_url, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        lista_atti = data.get("listaAtti", [])

        # Filter by number if we have it
        if parsed.get('number'):
            lista_atti = [
                atto for atto in lista_atti
                if atto.get("numeroProvvedimento") == parsed['number']
            ]

        return lista_atti

    except requests.exceptions.RequestException as e:
        print(f"Warning: Advanced search failed - {str(e)}")
        return []


def search_laws(search_text: str, max_results: int = 10, order: str = "recente") -> list:
    """Search for laws using intelligent query parsing.

    This function:
    1. Tries to parse structured queries (e.g., "decreto-legge 27 dicembre 2025 n. 196")
    2. Uses ricerca/avanzata for precise date-based searches
    3. Falls back to ricerca/semplice for general text searches

    Args:
        search_text: Keywords to search for (in title or text)
        max_results: Maximum number of results to return (default: 10)
        order: Sort order - "recente" (newest first) or "vecchio" (oldest first)

    Returns:
        list: List of matching atti (laws) with metadata
    """
    # Try parsing as structured query first
    parsed = _parse_structured_query(search_text)

    if parsed:
        print(f"Detected structured query: {parsed['type']} {parsed['date']} n. {parsed['number']}")
        print("Using ricerca/avanzata for precise search...")
        results = _search_with_ricerca_avanzata(parsed, max_results)

        if results:
            return results

        print("No results from advanced search, falling back to simple search...")

    # Fall back to simple search
    search_url = f"{BASE_URL}/ricerca/semplice"
    payload = {
        "testoRicerca": search_text,
        "orderType": order,
        "paginazione": {
            "paginaCorrente": 1,
            "numeroElementiPerPagina": max_results
        }
    }

    try:
        session = requests.Session()
        resp = session.post(search_url, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        lista_atti = data.get("listaAtti", [])
        return lista_atti

    except requests.exceptions.RequestException as e:
        print(f"Error: Search failed - {str(e)}")
        return []
    except Exception as e:
        print(f"Error: {str(e)}")
        return []


def find_exact_match(search_text: str, date: str = None, number: str = None) -> dict | None:
    """Search for a law and return the exact match by date and number.

    Args:
        search_text: Search keywords
        date: Expected date in YYYY-MM-DD format (optional)
        number: Expected number (optional)

    Returns:
        dict: Matching atto if found, None otherwise
    """
    results = search_laws(search_text, max_results=5)

    if not results:
        return None

    # If date and number provided, find exact match
    if date and number:
        for atto in results:
            if (atto.get("numeroProvvedimento") == number and
                atto.get("dataGU") == date):
                return atto
        return None

    # Otherwise return first result
    return results[0] if results else None
