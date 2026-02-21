#!/usr/bin/env python3
"""
ricerca_normattiva.py — Search Italian norms by year/month via ricerca/avanzata.

Usage:
  python ricerca_normattiva.py 2026 1

API Behavior:
  - The date parameters (dataInizioPubProvvedimento, dataFinePubProvvedimento) filter by
    **Gazzetta Ufficiale publication date** (dataGU), not the law's emanation date
  - This means a law emanated on Dec 30, 2025 but published in GU on Jan 2, 2026
    will be retrieved when searching for January 2026 publications

File Organization:
  - Laws are stored in content/leggi/{year}/{month}/{day}/n. {numero}/ based on
    their **emanation date** (data-emanazione), not publication date
  - This creates a chronological organization by when laws were enacted, even though
    they may be published in the Gazzetta Ufficiale days or weeks later
  - Folder structure: content/leggi/YYYY/MM/DD/n. numero/LEGGE....md
    Example: content/leggi/2025/12/30/n. 199/LEGGE 30 dicembre 2025, n. 199.md
"""

import argparse
import calendar
import csv
import json
import html as html_module
import re
import time
import yaml
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from functools import wraps

# Import Camera.it and Senato.it functions from separate modules
from lib.camera import fetch_camera_metadata
from lib.senato import fetch_senato_metadata

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS = {"Content-Type": "application/json"}
OUTPUT_DIR = Path(__file__).parent.parent / "data"
VAULT_DIR = Path(__file__).parent.parent / "content" / "leggi"
NORMATTIVA_SITE = "https://www.normattiva.it"
MANUAL_OVERRIDES_FILE = Path(__file__).parent / "normattiva_overrides.yaml"

# Global list to track failed requests for manual review
REQUEST_FAILURES = []

# Global list to track missing law references for PR warnings
MISSING_REFERENCES = []


def load_manual_overrides():
    """Load manual overrides from YAML file.

    Returns:
        dict: Dictionary mapping codice-redazionale to field overrides
    """
    if not MANUAL_OVERRIDES_FILE.exists():
        return {}

    try:
        with MANUAL_OVERRIDES_FILE.open('r', encoding='utf-8') as f:
            overrides = yaml.safe_load(f) or {}
            return overrides
    except Exception as e:
        print(f"  ⚠ Warning: Could not load manual overrides: {str(e)[:100]}")
        return {}


def apply_manual_overrides(atto: dict, overrides: dict) -> dict:
    """Apply manual overrides to an atto.

    Args:
        atto: Atto dictionary with scraped data
        overrides: Dictionary of overrides from YAML file

    Returns:
        dict: Atto with overrides applied
    """
    codice = atto.get("codiceRedazionale", "")
    if not codice or codice not in overrides:
        return atto

    atto_overrides = overrides[codice]
    print(f"    Applying {len(atto_overrides)} manual override(s) for {codice}")

    # Apply each override, converting field names to match internal format
    for field, value in atto_overrides.items():
        # Convert hyphenated field names to underscored (lavori-preparatori → lavori_preparatori)
        internal_field = field.replace("-", "_")
        atto[internal_field] = value
        print(f"      • {field}: {str(value)[:60]}{'...' if len(str(value)) > 60 else ''}")

    return atto


def parse_italian_date(day: str, month_name: str, year: str) -> str:
    """Convert Italian date to YYYY-MM-DD format.

    Args:
        day: Day of month (1-31)
        month_name: Italian month name (gennaio, febbraio, etc.)
        year: Four-digit year

    Returns:
        str: Date in YYYY-MM-DD format
    """
    months = {
        'gennaio': 1, 'febbraio': 2, 'marzo': 3, 'aprile': 4,
        'maggio': 5, 'giugno': 6, 'luglio': 7, 'agosto': 8,
        'settembre': 9, 'ottobre': 10, 'novembre': 11, 'dicembre': 12
    }
    month_num = months.get(month_name.lower(), 1)
    return f"{year}-{month_num:02d}-{int(day):02d}"


def extract_law_references(title_atto: str) -> list:
    """Extract references to other laws from the title.

    Detects patterns like:
    - "Conversione in legge del decreto-legge DD MONTH YYYY, n. NNN"
    - "Modifiche alla legge DD MONTH YYYY, n. NNN"

    Args:
        title_atto: The title of the law (titoloAtto field)

    Returns:
        list: List of dicts with: type, act_type, date, number
    """
    references = []

    # Pattern 1: Conversione in legge del decreto-legge
    conversion_pattern = r"Conversione\s+in\s+legge.*?del\s+(decreto-legge|decreto\s+legislativo)\s+(\d{1,2})\s+(\w+)\s+(\d{4}),?\s*n\.\s*(\d+)"

    # Pattern 2: Modifiche/Integrazioni alla legge
    modification_pattern = r"(Modifiche|Integrazioni).*?(legge|decreto-legge|decreto\s+legislativo)\s+(\d{1,2})\s+(\w+)\s+(\d{4}),?\s*n\.\s*(\d+)"

    for match in re.finditer(conversion_pattern, title_atto, re.IGNORECASE):
        act_type, day, month_name, year, number = match.groups()
        references.append({
            'type': 'converts',
            'act_type': act_type.lower().replace(' ', '_'),
            'date': parse_italian_date(day, month_name, year),
            'number': number
        })

    for match in re.finditer(modification_pattern, title_atto, re.IGNORECASE):
        mod_type, act_type, day, month_name, year, number = match.groups()
        references.append({
            'type': 'modifies' if mod_type.lower() == 'modifiche' else 'integrates',
            'act_type': act_type.lower().replace(' ', '_'),
            'date': parse_italian_date(day, month_name, year),
            'number': number
        })

    return references


def find_referenced_law(ref: dict, vault_dir: Path) -> str | None:
    """Find the markdown file for a referenced law.

    Args:
        ref: Reference dict with date, number, act_type
        vault_dir: Base directory for laws (content/leggi/)

    Returns:
        str: Relative file path from project root if found, None otherwise
    """
    # Parse date components
    date_obj = datetime.strptime(ref['date'], '%Y-%m-%d')
    year, month, day = date_obj.year, date_obj.month, date_obj.day
    number = ref['number']

    # Construct expected path
    search_path = vault_dir / str(year) / f"{month:02d}" / f"{day:02d}" / f"n. {number}"

    if search_path.exists():
        # Find the markdown file in that directory
        md_files = list(search_path.glob("*.md"))
        if md_files:
            # Return relative path from project root (content/leggi/...)
            relative_path = md_files[0].relative_to(vault_dir.parent.parent)
            return str(relative_path)

    return None


def search_and_fetch_missing_law(ref: dict, session) -> dict | None:
    """Search for a missing law using the ricerca/semplice API and fetch its metadata.

    Args:
        ref: Reference dict with date, number, act_type
        session: Requests session for API calls

    Returns:
        dict: Atto dictionary if found and fetched successfully, None otherwise
    """
    # Construct search query from reference
    # Example: "decreto-legge 27 dicembre 2025 n. 196"
    date_obj = datetime.strptime(ref['date'], '%Y-%m-%d')

    # Convert act_type back to Italian format
    act_type_map = {
        'decreto-legge': 'decreto-legge',
        'decreto_legge': 'decreto-legge',
        'decreto-legislativo': 'decreto legislativo',
        'decreto_legislativo': 'decreto legislativo',
        'legge': 'legge'
    }
    act_type_italian = act_type_map.get(ref['act_type'], ref['act_type'])

    # Format date in Italian
    month_names = ['', 'gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno',
                   'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre']
    month_name = month_names[date_obj.month]

    search_text = f"{act_type_italian} {date_obj.day} {month_name} {date_obj.year} n. {ref['number']}"

    print(f"    Searching for: {search_text}")

    # Call ricerca/semplice API
    search_url = f"{BASE_URL}/ricerca/semplice"
    payload = {
        "testoRicerca": search_text,
        "orderType": "recente",
        "paginazione": {
            "paginaCorrente": 1,
            "numeroElementiPerPagina": 5
        }
    }

    try:
        resp = session.post(search_url, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        lista_atti = data.get("listaAtti", [])
        if not lista_atti:
            print(f"    ✗ Not found in search results")
            return None

        # Find exact match by number and date
        for atto in lista_atti:
            if (atto.get("numeroProvvedimento") == ref['number'] and
                atto.get("dataGU") == ref['date']):
                print(f"    ✓ Found: {atto.get('codiceRedazionale')}")
                # Convert to our internal format
                return {
                    "codiceRedazionale": atto.get("codiceRedazionale"),
                    "dataGU": atto.get("dataGU"),
                    "descrizioneAtto": atto.get("descrizioneAtto"),
                    "titoloAtto": atto.get("titoloAtto", "").strip("[] "),
                    "numeroProvvedimento": atto.get("numeroProvvedimento"),
                    "denominazioneAtto": atto.get("denominazioneAtto"),
                    "_auto_fetched": True  # Mark as auto-fetched to prevent recursive fetching
                }

        print(f"    ✗ No exact match in {len(lista_atti)} results")
        return None

    except Exception as e:
        print(f"    ✗ Search failed: {str(e)[:50]}")
        return None


# denominazioneAtto  →  segmento URN di normattiva.it
URN_TIPO = {
    "COSTITUZIONE":                                 "costituzione",
    "DECRETO":                                      "decreto",
    "DECRETO DEL CAPO DEL GOVERNO":                 "decreto:capo:governo",
    "DECRETO DEL CAPO DEL GOVERNO, PRIMO MINISTRO SEGRETARIO DI STATO": "decreto:capo:governo:primo-ministro-segretario-di-stato",
    "DECRETO DEL CAPO PROVVISORIO DELLO STATO":     "decreto:capo-provvisorio:stato",
    "DECRETO DEL DUCE":                             "decreto:duce",
    "DECRETO DEL DUCE DEL FASCISMO, CAPO DEL GOVERNO": "decreto:duce:fascismo:capo:governo",
    "DECRETO DEL PRESIDENTE DEL CONSIGLIO DEI MINISTRI": "decreto:presidente:consiglio-dei-ministri",
    "DECRETO DEL PRESIDENTE DELLA REPUBBLICA":      "decreto:presidente:repubblica",
    "DECRETO-LEGGE":                                "decreto-legge",
    "DECRETO-LEGGE LUOGOTENENZIALE":                "decreto-legge-luogotenenziale",
    "DECRETO LEGISLATIVO":                          "decreto-legislativo",
    "DECRETO LEGISLATIVO DEL CAPO PROVVISORIO DELLO STATO": "decreto-legislativo:capo-provvisorio:stato",
    "DECRETO LEGISLATIVO LUOGOTENENZIALE":          "decreto-legislativo-luogotenenziale",
    "DECRETO LEGISLATIVO PRESIDENZIALE":            "decreto-legislativo-presidenziale",
    "DECRETO LUOGOTENENZIALE":                      "decreto-luogotenenziale",
    "DECRETO MINISTERIALE":                         "decreto-ministeriale",
    "DECRETO PRESIDENZIALE":                        "decreto-presidenziale",
    "DECRETO REALE":                                "decreto-reale",
    "DELIBERAZIONE":                                "deliberazione",
    "DETERMINAZIONE DEL COMMISSARIO PER LE FINANZE": "determinazione:commissario:finanze",
    "DETERMINAZIONE DEL COMMISSARIO PER LA PRODUZIONE BELLICA": "determinazione:commissario:produzione-bellica",
    "DETERMINAZIONE INTERCOMMISSARIALE":            "determinazione-intercommissariale",
    "LEGGE":                                        "legge",
    "LEGGE COSTITUZIONALE":                         "legge-costituzionale",
    "ORDINANZA":                                    "ordinanza",
    "REGIO DECRETO":                                "regio-decreto",
    "REGIO DECRETO-LEGGE":                          "regio-decreto-legge",
    "REGIO DECRETO LEGISLATIVO":                    "regio-decreto-legislativo",
    "REGOLAMENTO":                                  "regolamento",
}

# One column per approfondimento type (order preserved in CSV)
APPROFONDIMENTO_COLUMNS = [
    "atti_aggiornati",
    "atti_correlati",
    "lavori_preparatori",
    "aggiornamenti_atto",
    "note_atto",
    "relazioni",
    "aggiornamenti_titolo",
    "aggiornamenti_struttura",
    "atti_parlamentari",
    "atti_attuativi",
]

# display text on the N2Ls page (lowercase)  →  column name
TEXT_TO_COLUMN = {
    "atti aggiornati":              "atti_aggiornati",
    "atti correlati":               "atti_correlati",
    "lavori preparatori":           "lavori_preparatori",
    "aggiornamenti all'atto":       "aggiornamenti_atto",
    "note atto":                    "note_atto",
    "relazioni":                    "relazioni",
    "aggiornamenti al titolo":      "aggiornamenti_titolo",
    "aggiornamenti alla struttura": "aggiornamenti_struttura",
    "atti parlamentari":            "atti_parlamentari",
    "atti attuativi":               "atti_attuativi",
}


def retry_request(max_retries=3, initial_delay=2, backoff_factor=2):
    """Decorator to retry HTTP requests with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay between retries in seconds (default: 2)
        backoff_factor: Multiplier for delay on each retry (default: 2)

    Tracks failures in global REQUEST_FAILURES list for manual review.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, requests.HTTPError, ConnectionError, TimeoutError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        print(f"    ⚠ Retry {attempt + 1}/{max_retries} after {delay}s: {str(e)[:100]}")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        # Final failure - track for manual review
                        failure_info = {
                            "function": func.__name__,
                            "args": str(args)[:200],
                            "error": str(e)[:500],
                            "timestamp": datetime.now().isoformat(),
                        }
                        REQUEST_FAILURES.append(failure_info)
                        print(f"    ✗ Failed after {max_retries} retries: {str(e)[:100]}")
                        # Return appropriate empty value depending on function
                        if func.__name__ in ['fetch_normattiva_permalink', 'fetch_camera_metadata',
                                             'fetch_senato_metadata', 'fetch_approfondimenti']:
                            return {}
                        elif func.__name__ == 'ricerca_avanzata':
                            return {"listaAtti": []}
                        elif func.__name__ in ['fetch_parliamentary_group']:
                            return ""
                        elif func.__name__ in ['fetch_relatori_names']:
                            return []
                        raise
                except Exception as e:
                    # Non-network errors shouldn't be retried
                    raise

            return {}
        return wrapper
    return decorator


@retry_request(max_retries=3, initial_delay=2)
def fetch_normattiva_permalink(session, data_gu: str, codice: str) -> dict:
    """Fetch the permalink from Normattiva and extract URN and vigenza date.
    Returns dict with 'normattiva_uri' and 'data_vigenza' (in yyyy-mm-dd format).

    Raises:
        Exception: If HTTP request fails or parsing fails
    """
    result = {"normattiva_uri": "", "data_vigenza": ""}

    if not data_gu or not codice:
        return result

    # Load the main page to get session and extract "Entrata in vigore"
    main_url = f"https://www.normattiva.it/atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta={data_gu}&atto.codiceRedazionale={codice}"
    main_resp = session.get(main_url, timeout=30)
    main_resp.raise_for_status()

    # Extract "Entrata in vigore del provvedimento" date from main page (dd/mm/yyyy format)
    vigenza_match = re.search(r'Entrata in vigore del provvedimento:\s*(\d{2}/\d{2}/\d{4})', main_resp.text)
    vigenza_iso = ""
    if vigenza_match:
        vigenza_ddmmyyyy = vigenza_match.group(1)
        # Convert dd/mm/yyyy to yyyy-mm-dd
        vigenza_date = datetime.strptime(vigenza_ddmmyyyy, "%d/%m/%Y")
        vigenza_iso = vigenza_date.strftime("%Y-%m-%d")
        result["data_vigenza"] = vigenza_iso

    # Fetch the permalink to get the correct URN
    permalink_url = f"https://www.normattiva.it/do/atto/vediPermalink?atto.dataPubblicazioneGazzetta={data_gu}&atto.codiceRedazionale={codice}"
    resp = session.get(permalink_url, timeout=30)
    resp.raise_for_status()

    # Extract URN-NIR permalink (includes !vig= date)
    urn_match = re.search(r'href="(https://www\.normattiva\.it/uri-res/N2Ls\?urn:nir:[^"]+)"', resp.text)
    if not urn_match:
        raise ValueError(f"Could not find URN in permalink page for {codice}")

    urn = urn_match.group(1).strip()

    # Override !vig= parameter with the correct vigenza date from API
    if vigenza_iso:
        # Replace existing !vig= or append if not present
        if "!vig=" in urn:
            # Replace the existing !vig= parameter
            urn = re.sub(r'!vig=[^&\s]*', f'!vig={vigenza_iso}', urn)
        else:
            # Append !vig= parameter
            urn += f'!vig={vigenza_iso}'

    result["normattiva_uri"] = urn

    return result


def extract_links(html):
    """Extract normattiva / senato / camera links from an approfondimento HTML fragment."""
    links = []
    for m in re.finditer(r'href="([^"]*)"', html):
        href = m.group(1).replace("&amp;", "&")
        if href.startswith("/atto/"):
            href = NORMATTIVA_SITE + href
        if any(x in href for x in ("caricaDettaglioAtto", "senato.it", "camera.it")):
            if href not in links:
                links.append(href)
    return links


def extract_text_content(html):
    """Extract cleaned text content from HTML, preserving some structure."""
    # Remove script and style elements
    soup = BeautifulSoup(html, 'html.parser')
    for script in soup(["script", "style"]):
        script.decompose()

    # Get text
    text = soup.get_text(separator='\n', strip=True)

    # Clean up excessive whitespace while preserving line breaks
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    return '\n'.join(lines)

@retry_request(max_retries=3, initial_delay=2)
def fetch_full_text_via_export(session, data_gu: str, codice: str) -> str:
    """
    Fetch complete act HTML using the export endpoint.

    This replaces the article-by-article scraping with a single request
    that returns all articles in one response. The export endpoint provides
    the complete text with semantic Akoma Ntoso (AKN) HTML classes.

    Args:
        session: requests.Session with established cookies
        data_gu: Data pubblicazione GU (YYYY-MM-DD format)
        codice: Codice redazionale

    Returns:
        str: Complete HTML content with all articles
             Returns empty string if fetch fails

    Raises:
        Exception: If HTTP request fails
    """
    if not data_gu or not codice:
        return ""

    # Export endpoint returns all articles in single response
    # No need for vigenza parameter - returns current version
    export_url = f"https://www.normattiva.it/esporta/attoCompleto?atto.dataPubblicazioneGazzetta={data_gu}&atto.codiceRedazionale={codice}"

    resp = session.get(export_url, timeout=30)
    resp.raise_for_status()

    return resp.text


@retry_request(max_retries=3, initial_delay=2)
def fetch_approfondimenti(session, uri):
    """Load the N2Ls page, find active approfondimento endpoints, fetch and parse links.
    Returns dict: {column_name: "link1; link2; ...", "gu_link": "..."} for all APPROFONDIMENTO_COLUMNS.

    For lavori_preparatori: if no links are found, extracts raw text content as fallback.

    Raises:
        Exception: If HTTP request fails
    """
    result = {col: "" for col in APPROFONDIMENTO_COLUMNS}
    result["gu_link"] = ""

    resp = session.get(uri, timeout=30)
    resp.raise_for_status()

    # Extract GU link (gazzettaufficiale.it)
    gu_match = re.search(r'href="(https?://www\.gazzettaufficiale\.it/[^"]+)"', resp.text)
    if gu_match:
        result["gu_link"] = gu_match.group(1).replace("&amp;", "&")

    # Find every <a> that has a data-href; match its text to a column
    for m in re.finditer(r'<a\s[^>]*data-href="([^"]+)"[^>]*>\s*(.*?)\s*</a>', resp.text, re.DOTALL):
        data_href = m.group(1).replace("&amp;", "&")
        text = html_module.unescape(re.sub(r'\s+', ' ', m.group(2)).strip().lower())

        col = TEXT_TO_COLUMN.get(text)
        if not col:
            continue

        sub = session.get(NORMATTIVA_SITE + data_href, timeout=30)
        sub.raise_for_status()

        if "Sessione Scaduta" in sub.text:
            raise RuntimeError(f"Session expired when fetching approfondimenti for {uri}")

        links = extract_links(sub.text)
        if links:
            result[col] = "\n".join(links)
        elif col == "lavori_preparatori":
            # For lavori_preparatori, if no camera/senato links found,
            # extract the raw text content as fallback (issue #59)
            text_content = extract_text_content(sub.text)
            if text_content:
                result[col] = text_content

    return result


@retry_request(max_retries=3, initial_delay=2)
def ricerca_avanzata(data_inizio: str, data_fine: str, pagina: int = 1, per_pagina: int = 100) -> dict:
    """POST ricerca/avanzata filtrata per intervallo di date di pubblicazione.

    IMPORTANT: The date parameters filter by Gazzetta Ufficiale publication date (dataGU),
    NOT by the law's emanation date (data-emanazione). Laws are returned based on when
    they were published in the official gazette, regardless of when they were enacted.

    Args:
        data_inizio: Data di pubblicazione GU a partire da (formato YYYY-MM-DD)
        data_fine: Data di pubblicazione GU fino a (formato YYYY-MM-DD)
        pagina: Numero di pagina (default 1)
        per_pagina: Risultati per pagina (default 100)

    Returns:
        dict: Response from API with 'listaAtti' containing matching laws
    """
    payload = {
        "dataInizioPubProvvedimento": data_inizio,  # Filters by GU publication date
        "dataFinePubProvvedimento": data_fine,      # Filters by GU publication date
        "paginazione": {
            "paginaCorrente": str(pagina),
            "numeroElementiPerPagina": str(per_pagina),
        },
    }
    resp = requests.post(f"{BASE_URL}/ricerca/avanzata", json=payload, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()


def save_csv(atti: list, path: Path) -> None:
    if not atti:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=atti[0].keys(), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(atti)
    print(f"  CSV:  {path} ({len(atti)} rows)")


def save_json(data, path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  JSON: {path}")


def check_norm_exists(atto: dict, vault_dir: Path) -> bool:
    """Check if a norm's markdown file already exists.

    Args:
        atto: Atto dict with dataEmanazione and numeroProvvedimento
        vault_dir: Base vault directory path

    Returns:
        bool: True if the norm's directory exists, False otherwise
    """
    data_emanazione = atto.get("dataEmanazione", "")[:10]
    numero_provv = atto.get("numeroProvvedimento", "0")

    try:
        eman_date = datetime.strptime(data_emanazione, "%Y-%m-%d")
        year = str(eman_date.year)
        month = f"{eman_date.month:02d}"
        day = f"{eman_date.day:02d}"
    except ValueError:
        return False

    folder_name = f"n. {numero_provv}"
    norm_dir = vault_dir / year / month / day / folder_name

    return norm_dir.exists()


def check_pr_branch_exists(codice: str) -> bool:
    """Check if a PR branch already exists for this norm.

    Args:
        codice: Codice redazionale

    Returns:
        bool: True if a branch exists with pattern legge/{codice}, False otherwise
    """
    import subprocess
    try:
        # List all remote branches matching the pattern
        result = subprocess.run(
            ["git", "branch", "-r"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            branches = result.stdout
            # Check if branch matches legge/{codice}
            pattern = f"origin/legge/{codice}"
            return pattern in branches
    except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError):
        # If git command fails, continue processing (don't skip)
        pass
    return False


def save_markdown(atti: list, vault_dir: Path) -> list:
    """Save each atto as a markdown file, organized by emanation date.

    File structure: content/leggi/{year}/{month}/{day}/n. {numero}/{descrizione}.md

    IMPORTANT: Files are organized by **emanation date** (data-emanazione), not by
    Gazzetta Ufficiale publication date (dataGU). This creates a chronological
    organization based on when laws were enacted, even though they may be retrieved
    based on their later publication date in the official gazette.

    Example:
        A law emanated on 2025-12-30 and published in GU on 2026-01-02 will be stored at:
        content/leggi/2025/12/30/n. 199/LEGGE 30 dicembre 2025, n. 199.md

    Returns:
        list: Metadata about all processed laws
    """
    if not atti:
        return []
    vault_dir.mkdir(parents=True, exist_ok=True)

    law_metadata = []

    for atto in atti:
        codice = atto.get("codiceRedazionale", "unknown")
        descrizione = atto.get("descrizioneAtto", codice)
        titolo = atto.get("titoloAtto", "").strip().strip("[]").strip()
        numero_provv = atto.get("numeroProvvedimento", "0")
        tipo = atto.get("denominazioneAtto", "")
        data_gu = atto.get("dataGU", "")
        numero_gu = atto.get("numeroGU", "")
        data_emanazione = atto.get("dataEmanazione", "")[:10]
        uri = atto.get("normattiva_uri", "")

        # Parse year/month/day from dataEmanazione (NOT dataGU)
        # This organizes files by when the law was enacted, not when it was published
        try:
            eman_date = datetime.strptime(data_emanazione, "%Y-%m-%d")
            year = str(eman_date.year)
            month = f"{eman_date.month:02d}"
            day = f"{eman_date.day:02d}"
        except ValueError:
            year = "unknown"
            month = "00"
            day = "00"

        # Create folder structure based on emanation date: vault/YYYY/MM/DD/n. numero/
        folder_name = f"n. {numero_provv}"
        norm_dir = vault_dir / year / month / day / folder_name
        norm_dir.mkdir(parents=True, exist_ok=True)

        # Main markdown file
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', descrizione)
        filepath = norm_dir / f"{safe_filename}.md"

        lines = []
        # YAML frontmatter only
        lines.append("---")
        lines.append(f"codice-redazionale: {codice}")
        lines.append(f"tipo: {tipo}")
        lines.append(f"numero-atto: {numero_provv}")
        lines.append(f"data-emanazione: {data_emanazione}")
        lines.append(f"data-gu: {data_gu}")
        lines.append(f"numero-gu: {numero_gu}")
        # Add data-vigenza (entry into force date) in yyyy-mm-dd format
        data_vigenza = atto.get("data_vigenza", "")
        if data_vigenza:
            lines.append(f"data-vigenza: {data_vigenza}")
        if uri:
            lines.append(f"normattiva-urn: {uri}")
        # Build normattiva-link with vigenza parameters
        if data_gu and codice:
            normattiva_link = f"https://www.normattiva.it/atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta={data_gu}&atto.codiceRedazionale={codice}"
            if data_vigenza:
                # Convert yyyy-mm-dd to dd/mm/yyyy for the URL parameter
                try:
                    vig_date = datetime.strptime(data_vigenza, "%Y-%m-%d")
                    vig_ddmmyyyy = vig_date.strftime("%d/%m/%Y")
                    normattiva_link += f"&tipoDettaglio=singolavigenza&dataVigenza={vig_ddmmyyyy}"
                except ValueError:
                    # If conversion fails, use as-is
                    normattiva_link += f"&tipoDettaglio=singolavigenza&dataVigenza={data_vigenza}"
            lines.append(f"normattiva-link: {normattiva_link}")
        # GU link extracted from page
        gu_link = atto.get("gu_link", "")
        if gu_link:
            lines.append(f"gu-link: {gu_link}")
        lines.append(f"titolo-atto: \"{titolo}\"")
        lines.append(f"descrizione-atto: \"{descrizione}\"")

        # Build alternative title: "Legge n. 1/26 del 7 gennaio 2026"
        try:
            eman_dt = datetime.strptime(data_emanazione, "%Y-%m-%d")
            months_it = ["", "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
                         "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre"]
            year_short = str(eman_dt.year)[-2:]  # 2026 -> 26
            date_it = f"{eman_dt.day} {months_it[eman_dt.month]} {eman_dt.year}"
            # Simplify tipo: LEGGE -> Legge, DECRETO-LEGGE -> Decreto-legge, etc.
            tipo_simple = tipo.title().replace("Del ", "del ").replace("Dei ", "dei ")
            titolo_alt = f"{tipo_simple} n. {numero_provv}/{year_short} del {date_it}"
            lines.append(f"titolo-alternativo: \"{titolo_alt}\"")
        except ValueError:
            pass

        # Add all approfondimenti as metadata
        for col in APPROFONDIMENTO_COLUMNS:
            content = atto.get(col, "")
            if content:
                col_name = col.replace("_", "-")

                # Handle both string content (from scraping) and list content (from manual overrides)
                if isinstance(content, list):
                    # Content is already a list (from YAML manual override)
                    lines_in_content = content
                else:
                    # Content is a string (from scraping), split by newlines
                    lines_in_content = content.split("\n")

                # Check if content is a URL list or raw text
                is_url_list = all(
                    str(line).strip().startswith("http") or not str(line).strip()
                    for line in lines_in_content
                )

                if is_url_list:
                    # Format as list of links
                    lines.append(f"{col_name}:")
                    for link in lines_in_content:
                        if str(link).strip():
                            lines.append(f"  - {str(link).strip()}")
                else:
                    # Format as multi-line text block using YAML literal style
                    lines.append(f"{col_name}: |")
                    for line in lines_in_content:
                        lines.append(f"  {line}")

        # Camera metadata (from lavori preparatori RDF)
        if atto.get("legislatura"):
            lines.append(f"camera-legislatura: {atto.get('legislatura')}")
        if atto.get("camera-atto"):
            lines.append(f"camera-atto: {atto.get('camera-atto')}")
        if atto.get("camera-atto-iri"):
            lines.append(f"camera-atto-iri: {atto.get('camera-atto-iri')}")
        if atto.get("camera-natura"):
            lines.append(f"camera-natura: \"{atto.get('camera-natura')}\"")
        if atto.get("camera-iniziativa"):
            lines.append(f"camera-iniziativa: \"{atto.get('camera-iniziativa')}\"")
        if atto.get("camera-data-presentazione"):
            lines.append(f"camera-data-presentazione: \"{atto.get('camera-data-presentazione')}\"")
        if atto.get("camera-relazioni"):
            lines.append("camera-relazioni:")
            for relazione in atto.get("camera-relazioni", []):
                lines.append(f"  - {relazione}")
        if atto.get("camera-firmatari"):
            lines.append("camera-firmatari:")
            for dep in atto.get("camera-firmatari", []):
                if dep.get('role'):
                    # Government bill: show ministerial role
                    lines.append(f"  - \"{dep['name']} - {dep['role']}\"")
                elif dep.get('group'):
                    # Parliamentary bill: show parliamentary group
                    lines.append(f"  - \"{dep['name']} - {dep['group']}\"")
                else:
                    lines.append(f"  - \"{dep['name']}\"")
        if atto.get("camera-relatori"):
            lines.append("camera-relatori:")
            for rel in atto.get("camera-relatori", []):
                lines.append(f"  - \"{rel}\"")
        if atto.get("camera-votazione-finale"):
            lines.append(f"camera-votazione-finale: {atto.get('camera-votazione-finale')}")
        if atto.get("camera-dossier"):
            lines.append("camera-dossier:")
            for dossier_link in atto.get("camera-dossier", []):
                lines.append(f"  - {dossier_link}")

        # Senato metadata (from lavori preparatori HTML scraping)
        if atto.get("senato-did"):
            lines.append(f"senato-did: {atto.get('senato-did')}")
        if atto.get("senato-legislatura"):
            lines.append(f"senato-legislatura: {atto.get('senato-legislatura')}")
        if atto.get("senato-numero-fase"):
            lines.append(f"senato-numero-fase: {atto.get('senato-numero-fase')}")
        if atto.get("senato-url"):
            lines.append(f"senato-url: {atto.get('senato-url')}")
        if atto.get("senato-titolo"):
            lines.append(f"senato-titolo: \"{atto.get('senato-titolo')}\"")
        if atto.get("senato-titolo-breve"):
            lines.append(f"senato-titolo-breve: \"{atto.get('senato-titolo-breve')}\"")
        if atto.get("senato-natura"):
            lines.append(f"senato-natura: \"{atto.get('senato-natura')}\"")
        if atto.get("senato-iniziativa"):
            lines.append(f"senato-iniziativa: \"{atto.get('senato-iniziativa')}\"")
        if atto.get("senato-data-presentazione"):
            lines.append(f"senato-data-presentazione: \"{atto.get('senato-data-presentazione')}\"")
        if atto.get("senato-teseo"):
            lines.append("senato-teseo:")
            for term in atto.get("senato-teseo", []):
                lines.append(f"  - \"{term}\"")
        if atto.get("senato-votazioni-url"):
            lines.append(f"senato-votazioni-url: {atto.get('senato-votazioni-url')}")
        if atto.get("senato-votazione-finale"):
            lines.append(f"senato-votazione-finale: {atto.get('senato-votazione-finale')}")
        # Note: senato-votazione-finale-warning is kept in memory for PR descriptions
        # but not written to markdown frontmatter (user preference)
        if atto.get("senato-documenti"):
            lines.append("senato-documenti:")
            for doc_link in atto.get("senato-documenti", []):
                lines.append(f"  - {doc_link}")

        # Extract law references from title (conversions, modifications, etc.)
        references = extract_law_references(atto.get("titoloAtto", ""))
        if references:
            # Group references by type (converts, modifies, integrates)
            refs_by_type = {}
            for ref in references:
                ref_type = ref['type']
                file_path = find_referenced_law(ref, vault_dir)
                if file_path:
                    if ref_type not in refs_by_type:
                        refs_by_type[ref_type] = []
                    refs_by_type[ref_type].append(file_path)
                else:
                    # Track missing reference for PR warning
                    MISSING_REFERENCES.append({
                        "codice": codice,
                        "title": atto.get("titoloAtto", ""),
                        "reference_type": ref_type,
                        "reference_date": ref['date'],
                        "reference_number": ref['number'],
                        "reference_act_type": ref['act_type']
                    })

            # Write file paths as list (always as list for consistency)
            for ref_type, file_paths in refs_by_type.items():
                lines.append(f"{ref_type}:")
                for path in file_paths:
                    lines.append(f"  - \"{path}\"")

        lines.append("---")

        # Write frontmatter
        with filepath.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines))

            # Add full text HTML to the body (complete export as-is)
            full_text_html = atto.get("full_text_html", "")
            if full_text_html:
                f.write("\n\n")  # Add spacing between frontmatter and body

                # Parse HTML and extract the printable content div
                soup = BeautifulSoup(full_text_html, 'html.parser')
                print_div = soup.find('div', id='printThis')

                if print_div:
                    # Write the entire printThis div content
                    f.write(print_div.decode_contents())
                else:
                    # Fallback: write everything if printThis div not found
                    f.write(full_text_html)

        # Track processed laws metadata
        # Get relative path from project root
        relative_path = filepath.relative_to(vault_dir.parent.parent)
        law_metadata.append({
            "codice": codice,
            "descrizione": descrizione,
            "tipo": tipo,
            "numero": numero_provv,
            "data_emanazione": data_emanazione,
            "titolo_alternativo": atto.get("titolo_alternativo", descrizione),
            "filepath": str(relative_path),
            "directory": str(norm_dir.relative_to(vault_dir.parent.parent)),
        })

    print(f"  Vault: {vault_dir}/ ({len(atti)} norms)")
    return law_metadata


def main():
    parser = argparse.ArgumentParser(
        description="Search norms on Normattiva by year and month.",
        epilog="Output is saved to normattiva/ and vault/",
    )
    parser.add_argument("anno", type=int, help="Year (e.g. 2026)")
    parser.add_argument("mese", type=int, help="Month (1-12)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip git operations (branch checks, fetch)")
    args = parser.parse_args()

    # Validate
    if not (1 <= args.mese <= 12):
        parser.error(f"mese must be 1-12, got: {args.mese}")

    # Calculate date range for the month
    # These dates represent the Gazzetta Ufficiale publication date range, NOT emanation date
    _, last_day = calendar.monthrange(args.anno, args.mese)
    data_inizio = f"{args.anno}-{args.mese:02d}-01"
    data_fine = f"{args.anno}-{args.mese:02d}-{last_day:02d}"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_range = f"{args.anno}_{args.mese:02d}"

    print("=" * 60)
    print(f"Ricerca normattiva: {args.anno}/{args.mese:02d}")
    print(f"GU publication date range: {data_inizio} to {data_fine}")
    print("=" * 60 + "\n")

    # Paginate all results
    atti = []
    pagina = 1
    while True:
        print(f"  Pagina {pagina}...")
        results = ricerca_avanzata(data_inizio, data_fine, pagina=pagina)
        batch = results.get("listaAtti", [])
        if not batch:
            break
        atti.extend(batch)
        print(f"    {len(batch)} risultati")
        pagina += 1

    print(f"\n  Totale norme fetched: {len(atti)}")

    # Filter out norms that already exist
    print("\n[Checking for existing norms]")
    original_count = len(atti)
    atti = [atto for atto in atti if not check_norm_exists(atto, VAULT_DIR)]
    filtered_count = original_count - len(atti)

    if filtered_count > 0:
        print(f"  Skipped {filtered_count} existing norms")
    print(f"  Processing {len(atti)} new norms")

    # Filter out norms that already have PR branches
    if not args.dry_run:
        print("\n[Checking for existing PR branches]")
        # Fetch remote branches to get the latest PR branches
        import subprocess
        try:
            print("  Fetching remote branches...")
            subprocess.run(["git", "fetch", "origin"], capture_output=True, timeout=30, check=True)
        except (subprocess.SubprocessError, subprocess.TimeoutExpired, FileNotFoundError) as e:
            print(f"  ⚠ Warning: Could not fetch remote branches: {str(e)[:100]}")
            print("  Continuing with local branch information only...")

        pre_pr_count = len(atti)
        atti = [atto for atto in atti if not check_pr_branch_exists(atto.get("codiceRedazionale", ""))]
        pr_filtered_count = pre_pr_count - len(atti)

        if pr_filtered_count > 0:
            print(f"  Skipped {pr_filtered_count} norms with existing PR branches")
        print(f"  Processing {len(atti)} remaining norms\n")
    else:
        print("\n[Dry run: Skipping PR branch checks]")
        print(f"  Processing all {len(atti)} norms\n")

    if len(atti) == 0:
        print("  No new norms to process. Exiting.")
        print("\n" + "=" * 60)
        print("Done!")
        print("=" * 60)
        return

    # Fetch normattiva.it permalink (URN and vigenza) for each atto
    print("[Fetching normattiva permalinks]")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    for i, atto in enumerate(atti):
        data_gu = atto.get("dataGU", "")
        codice = atto.get("codiceRedazionale", "")
        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)
        permalink_data = fetch_normattiva_permalink(session, data_gu, codice)
        atto["normattiva_uri"] = permalink_data.get("normattiva_uri", "")
        atto["data_vigenza"] = permalink_data.get("data_vigenza", "")
        print(f"vig={atto.get('data_vigenza', '?')}")

    # Fetch approfondimenti for each atto
    print("[Fetching approfondimenti]")
    for i, atto in enumerate(atti):
        uri = atto.get("normattiva_uri")
        if not uri:
            for col in APPROFONDIMENTO_COLUMNS:
                atto[col] = ""
            continue
        print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}...", end=" ", flush=True)
        try:
            appro = fetch_approfondimenti(session, uri)
            atto.update(appro)
            populated = [col for col in APPROFONDIMENTO_COLUMNS if appro[col]]
            print(f"{', '.join(populated) if populated else 'nessuno'}")
        except Exception as e:
            print(f"error ({str(e)[:50]}...)")
            # Initialize empty approfondimenti on error
            for col in APPROFONDIMENTO_COLUMNS:
                atto[col] = ""

    # Apply manual overrides from YAML file (before camera/senato fetching)
    print("\n[Applying manual overrides]")
    manual_overrides = load_manual_overrides()
    if manual_overrides:
        print(f"  Loaded {len(manual_overrides)} override(s) from {MANUAL_OVERRIDES_FILE.name}")
        for i, atto in enumerate(atti):
            atti[i] = apply_manual_overrides(atto, manual_overrides)
    else:
        print(f"  No manual overrides found in {MANUAL_OVERRIDES_FILE.name}")

    # Fetch complete act HTML using export endpoint
    print("\n[Fetching full text HTML via export endpoint]")
    for i, atto in enumerate(atti):
        data_gu = atto.get("dataGU", "")
        codice = atto.get("codiceRedazionale", "")

        if not data_gu or not codice:
            print(f"  [{i+1}/{len(atti)}] {codice}... skipping (missing data)")
            atto["full_text_html"] = ""
            continue

        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        # Fetch complete HTML in single request
        full_html = fetch_full_text_via_export(session, data_gu, codice)

        atto["full_text_html"] = full_html
        if full_html:
            print(f"{len(full_html):,} chars")
        else:
            print("no content found")

    # Save after full text HTML fetching (most time-consuming and important step)
    print("\n[Saving after full text HTML fetch]")
    save_markdown(atti, VAULT_DIR)
    print("  ✅ Markdown files saved with full text HTML\n")

    # Fetch camera.it metadata from lavori_preparatori
    print("[Fetching camera.it metadata and interventi]")
    for i, atto in enumerate(atti):
        lavori = atto.get("lavori_preparatori", "")
        # Handle both string (from scraping) and list (from manual overrides)
        lavori_list = lavori if isinstance(lavori, list) else lavori.split("\n") if lavori else []
        camera_links = [l for l in lavori_list if "camera.it" in str(l)]
        if camera_links:
            print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}...", end=" ", flush=True)

            # Calculate interventi directory path
            interventi_dir = None
            data_emanazione = atto.get("dataEmanazione", "")[:10]
            numero_provv = atto.get("numeroProvvedimento", "")
            if data_emanazione and numero_provv:
                try:
                    eman_date = datetime.strptime(data_emanazione, "%Y-%m-%d")
                    year = str(eman_date.year)
                    month = f"{eman_date.month:02d}"
                    day = f"{eman_date.day:02d}"
                    norm_dir = VAULT_DIR / year / month / day / f"n. {numero_provv}"
                    interventi_dir = str(norm_dir / "interventi")
                except ValueError:
                    pass

            # Fetch metadata and interventi in one call
            camera_meta = fetch_camera_metadata(session, camera_links[0], interventi_dir=interventi_dir)
            atto.update(camera_meta)

            # Print results
            info_parts = [f"legislatura {camera_meta.get('legislatura', '?')}", camera_meta.get('camera-atto', '?')]
            if "_interventi_summary" in camera_meta:
                summary = camera_meta["_interventi_summary"]
                info_parts.append(f"{summary['sessions']} sedute, {summary['interventions']} interventi")
            print(", ".join(filter(None, info_parts)))

            # Add delay to avoid rate limiting
            time.sleep(3)
        else:
            print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}... no camera.it link")

    # Save after camera metadata
    print("\n[Saving after camera.it metadata]")
    save_markdown(atti, VAULT_DIR)
    print("  ✅ Markdown files updated with camera metadata\n")

    # Fetch senato.it metadata from lavori_preparatori
    print("[Fetching senato.it metadata]")
    for i, atto in enumerate(atti):
        lavori = atto.get("lavori_preparatori", "")
        # Handle both string (from scraping) and list (from manual overrides)
        lavori_list = lavori if isinstance(lavori, list) else lavori.split("\n") if lavori else []
        senato_links = [l for l in lavori_list if "senato.it" in str(l) and "ddl" in str(l)]
        if senato_links:
            print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}...", end=" ", flush=True)
            try:
                senato_meta = fetch_senato_metadata(session, senato_links[0])
                atto.update(senato_meta)
                print(f"DDL {senato_meta.get('senato-numero-fase', '?')}, did {senato_meta.get('senato-did', '?')}")
                # Check for voting link warning
                if senato_meta.get("senato-votazione-finale-warning"):
                    print(f"    ⚠ {senato_meta.get('senato-votazione-finale-warning')}")
            except Exception as e:
                print(f"error ({str(e)[:50]}...)")
        else:
            print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}... no senato.it link")

    # Auto-fetch missing referenced laws
    print("\n[Checking for missing law references]")
    missing_to_fetch = []
    for atto in atti:
        # Skip auto-fetched laws to prevent infinite recursion
        if atto.get("_auto_fetched"):
            continue

        references = extract_law_references(atto.get("titoloAtto", ""))
        if references:
            for ref in references:
                # Check if law exists in vault
                file_path = find_referenced_law(ref, VAULT_DIR)
                if not file_path:
                    # Check if already in our fetch list
                    already_listed = any(
                        m['date'] == ref['date'] and m['number'] == ref['number']
                        for m in missing_to_fetch
                    )
                    if not already_listed:
                        missing_to_fetch.append(ref)

    if missing_to_fetch:
        print(f"  Found {len(missing_to_fetch)} missing reference(s), attempting to fetch...")

        newly_fetched = []
        for ref in missing_to_fetch:
            print(f"  • {ref['act_type']} {ref['date']} n. {ref['number']}")
            fetched_atto = search_and_fetch_missing_law(ref, session)
            if fetched_atto:
                newly_fetched.append(fetched_atto)

        if newly_fetched:
            print(f"\n  ✓ Successfully fetched {len(newly_fetched)} missing law(s)")
            print(f"  Processing newly fetched laws...")

            # Process newly fetched laws through the same pipeline
            for i, atto in enumerate(newly_fetched):
                codice = atto.get("codiceRedazionale", "")
                data_gu = atto.get("dataGU", "")

                print(f"\n  [{i+1}/{len(newly_fetched)}] {codice}")

                # Fetch permalink
                try:
                    permalink_data = fetch_normattiva_permalink(session, data_gu, codice)
                    atto.update(permalink_data)
                except Exception as e:
                    print(f"    ⚠ Permalink fetch failed: {str(e)[:50]}")

                # Fetch lavori preparatori
                lavori_url = atto.get("lavori_preparatori")
                if lavori_url:
                    print(f"    Fetching Camera metadata...")
                    try:
                        camera_meta = fetch_camera_metadata(session, lavori_url)
                        atto.update(camera_meta)
                    except Exception as e:
                        print(f"    ⚠ Camera fetch failed: {str(e)[:50]}")

                    if atto.get("senato-url"):
                        print(f"    Fetching Senato metadata...")
                        try:
                            senato_meta = fetch_senato_metadata(session, atto.get("senato-url"))
                            atto.update(senato_meta)
                        except Exception as e:
                            print(f"    ⚠ Senato fetch failed: {str(e)[:50]}")

                # Fetch full text HTML
                print(f"    Fetching full text HTML...")
                try:
                    full_html = fetch_full_text_via_export(session, data_gu, codice)
                    atto["full_text_html"] = full_html
                    if full_html:
                        print(f"    ✓ {len(full_html):,} chars")
                except Exception as e:
                    print(f"    ⚠ Full text fetch failed: {str(e)[:50]}")
                    atto["full_text_html"] = ""

            # Add newly fetched laws to main list
            atti.extend(newly_fetched)
            print(f"\n  Total laws to process: {len(atti)} (original + auto-fetched)")
    else:
        print(f"  ✓ All referenced laws found in vault")

    # Final save with all metadata
    print("\n[Final save - all metadata included]")
    save_json({"listaAtti": atti}, OUTPUT_DIR / f"ricerca_{safe_range}_raw_{timestamp}.json")
    save_csv(atti, OUTPUT_DIR / f"ricerca_{safe_range}_{timestamp}.csv")
    processed_laws = save_markdown(atti, VAULT_DIR)

    # Save processed laws metadata for GitHub Actions workflow
    if processed_laws:
        new_laws_file = OUTPUT_DIR / "new_laws.json"
        save_json({"new_laws": processed_laws}, new_laws_file)
        print(f"  Processed laws metadata: {new_laws_file} ({len(processed_laws)} laws)")

    # Save request failures for manual review in PR
    if REQUEST_FAILURES:
        failures_file = OUTPUT_DIR / "request_failures.json"
        save_json({
            "failures": REQUEST_FAILURES,
            "count": len(REQUEST_FAILURES),
            "timestamp": datetime.now().isoformat()
        }, failures_file)
        print(f"  ⚠ Request failures: {failures_file} ({len(REQUEST_FAILURES)} failures)")
        print(f"    These will be flagged in the PR for manual review")

    # Collect interventi failures from camera metadata fetch
    print("\n[Checking Esame in Assemblea (Assembly debates) results]")
    interventi_failures = [
        {"codice": atto.get("codiceRedazionale"), "error": atto.get("_interventi_error")}
        for atto in atti
        if "_interventi_error" in atto
    ]
    if interventi_failures:
        interventi_failures_file = OUTPUT_DIR / "interventi_failures.json"
        save_json({
            "failures": interventi_failures,
            "count": len(interventi_failures),
            "timestamp": datetime.now().isoformat()
        }, interventi_failures_file)
        print(f"  ⚠ Interventi failures: {interventi_failures_file} ({len(interventi_failures)} failures)")
        print(f"    These will be flagged in the PR for manual review")

    # Save missing law references for PR warnings
    if MISSING_REFERENCES:
        missing_refs_file = OUTPUT_DIR / "missing_references.json"
        save_json({
            "missing_references": MISSING_REFERENCES,
            "count": len(MISSING_REFERENCES),
            "timestamp": datetime.now().isoformat()
        }, missing_refs_file)
        print(f"\n  ⚠ Missing law references: {missing_refs_file} ({len(MISSING_REFERENCES)} missing)")
        print(f"    Referenced laws not found in vault - will be flagged in PR")

    # Preview
    if atti:
        print(f"\n  Prime 10 norme:")
        print(f"  {'codice':<14} {'dataGU':<12} {'descrizione':<45} {'approfondimenti (colonne nel CSV)'}")
        print(f"  {'-'*14} {'-'*12} {'-'*45} {'-'*60}")
        for atto in atti[:10]:
            populated = [col for col in APPROFONDIMENTO_COLUMNS if atto.get(col)]
            print(f"  {atto.get('codiceRedazionale', ''):<14} "
                  f"{atto.get('dataGU', ''):<12} "
                  f"{atto.get('descrizioneAtto', ''):<45} "
                  f"{', '.join(populated)}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
