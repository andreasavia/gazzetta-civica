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
from pathlib import Path

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
    """Fetch permalink and detailed metadata from normattiva.it.

    Args:
        session: Requests session
        data_gu: Date of Gazzetta Ufficiale publication (YYYY-MM-DD)
        codice: Codice redazionale (e.g., 24G00231)

    Returns:
        dict: Metadata including permalink, lavori_preparatori URL, etc.
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

    # Extract data-emanazione from page
    data_emanazione = ""
    tipo_atto = ""
    numero_provvedimento = ""

    span_descrizione = soup.find("span", class_="descrizioneAtto")
    if span_descrizione:
        full_text = span_descrizione.get_text(strip=True)
        parts = full_text.split(',')
        if len(parts) >= 2:
            tipo_numero = parts[0].strip()
            data_emanazione_text = parts[1].strip()

            tipo_numero_parts = tipo_numero.rsplit(' ', 1)
            if len(tipo_numero_parts) == 2:
                tipo_atto = tipo_numero_parts[0].strip()
                numero_provvedimento = tipo_numero_parts[1].replace("n.", "").strip()

            if data_emanazione_text:
                from datetime import datetime
                try:
                    date_obj = datetime.strptime(data_emanazione_text, "%d %B %Y")
                    data_emanazione = date_obj.strftime("%Y-%m-%d")
                except:
                    pass

    # Extract lavori preparatori link
    lavori_preparatori = ""
    lavori_link = soup.find("a", string=lambda text: text and "lavori preparatori" in text.lower())
    if lavori_link and lavori_link.get("href"):
        lavori_preparatori = lavori_link["href"]

    return {
        "permalink": permalink_url,
        "data_emanazione": data_emanazione,
        "tipo": tipo_atto,
        "numero": numero_provvedimento,
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


def search_laws(search_text: str, max_results: int = 10, order: str = "recente") -> list:
    """Search for laws using the ricerca/semplice API.

    Args:
        search_text: Keywords to search for (in title or text)
        max_results: Maximum number of results to return (default: 10)
        order: Sort order - "recente" (newest first) or "vecchio" (oldest first)

    Returns:
        list: List of matching atti (laws) with metadata
    """
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
