#!/usr/bin/env python3
"""
senato.py — Extract metadata from Senato.it (Italian Senate)

Provides functions to:
- Fetch and parse bill metadata from Senate pages
- Extract voting information
- Parse TESEO classification
- Extract document links

Usage:
    from senato import fetch_senato_metadata
"""

import re
import time
from functools import wraps
from bs4 import BeautifulSoup
import requests


def retry_request(max_retries=3, initial_delay=2, backoff_factor=2):
    """Decorator to retry HTTP requests with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay between retries in seconds (default: 2)
        backoff_factor: Multiplier for delay on each retry (default: 2)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay

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
                        print(f"    ✗ Failed after {max_retries} retries: {str(e)[:100]}")
                        return {}  # Return empty dict on failure
                except Exception as e:
                    # Non-network errors shouldn't be retried
                    raise

            return {}

        return wrapper
    return decorator


@retry_request(max_retries=3, initial_delay=2)
def fetch_senato_metadata(session, senato_url: str) -> dict:
    """Fetch and parse metadata from a senato.it page by scraping HTML.

    Returns dict with senato-did, senato-numero-fase, senato-titolo, etc.

    Raises:
        Exception: If HTTP request fails or parsing fails
    """
    result = {}

    # Parse URL to extract legislatura and numero_fase
    # URL format: http://www.senato.it/uri-res/N2Ls?urn:senato-it:parl:ddl:senato;19.legislatura;1457
    url_match = re.search(r'(\d+)\.legislatura;(\d+)', senato_url)
    if not url_match:
        raise ValueError(f"Could not parse legislatura/numero_fase from senato URL: {senato_url}")

    legislatura = url_match.group(1)
    numero_fase = url_match.group(2)

    # Resolve URN to get the did parameter by following redirect
    resp = session.get(senato_url, timeout=30, allow_redirects=True)
    resp.raise_for_status()

    # Extract did from final URL
    did_match = re.search(r'[?&]did=(\d+)', resp.url)
    if not did_match:
        raise ValueError(f"Could not find did parameter in senato redirect URL: {resp.url}")

    did = did_match.group(1)

    result["senato-did"] = did
    result["senato-legislatura"] = legislatura
    result["senato-numero-fase"] = numero_fase
    result["senato-url"] = resp.url

    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(resp.text, 'html.parser')

    # Extract title from boxTitolo div
    title_elem = soup.find('div', class_='boxTitolo')
    if title_elem:
        # Get only the first span to avoid concatenation
        span = title_elem.find('span')
        if span:
            title_text = span.get_text(strip=True)
            result["senato-titolo"] = title_text

    # Extract short title (titolo breve)
    title_breve = soup.find('strong', string=re.compile('Titolo breve'))
    if title_breve:
        em = title_breve.find_next('em')
        if em:
            result["senato-titolo-breve"] = em.get_text(strip=True)

    # Extract natura (nature of bill)
    natura_header = soup.find('h2', string=re.compile('Natura', re.IGNORECASE))
    if natura_header:
        natura_p = natura_header.find_next('p')
        if natura_p:
            # Get only the first span for clean text
            span = natura_p.find('span')
            if span:
                natura_text = span.get_text(strip=True)
            else:
                natura_text = natura_p.get_text(strip=True)

            # Keep only the first part before extra details
            natura_parts = re.split(r'(?:Contenente|Relazione|Include)', natura_text)
            natura_clean = natura_parts[0].strip()
            # Remove trailing punctuation
            natura_clean = re.sub(r'[,\.\s]+$', '', natura_clean)
            result["senato-natura"] = natura_clean

    # Extract iniziativa (initiative type)
    if 'Iniziativa Parlamentare' in resp.text:
        result["senato-iniziativa"] = "Parlamentare"
    elif 'Iniziativa Governativa' in resp.text:
        result["senato-iniziativa"] = "Governativa"

    # Extract TESEO classification
    teseo_header = soup.find('h2', string=re.compile('Classificazione TESEO', re.IGNORECASE))
    if teseo_header:
        teseo_p = teseo_header.find_next('p')
        if teseo_p:
            teseo_terms = []
            for span in teseo_p.find_all('span'):
                term = span.get_text(strip=True).strip(',').strip()
                if term:
                    teseo_terms.append(term)
            if teseo_terms:
                result["senato-teseo"] = teseo_terms

    # Build votazioni tab URL and fetch voting info
    # This is optional - if it fails, we still return the metadata collected so far
    votazioni_url = f"https://www.senato.it/leggi-e-documenti/disegni-di-legge/scheda-ddl?tab=votazioni&did={did}"
    result["senato-votazioni-url"] = votazioni_url

    try:
        vot_resp = session.get(votazioni_url, timeout=30)
        vot_resp.raise_for_status()
        vot_soup = BeautifulSoup(vot_resp.text, 'html.parser')

        # Find votazione finale link
        found_vote_link = False
        for li in vot_soup.find_all('li'):
            strong = li.find('strong')
            if strong and 'Votazione finale' in strong.get_text():
                # Extract link to vote detail
                vote_link = li.find('a', class_='schedaCamera')
                if vote_link and vote_link.get('href'):
                    href = vote_link['href']
                    if not href.startswith('http'):
                        href = 'https://www.senato.it' + href
                    result["senato-votazione-finale"] = href
                    found_vote_link = True
                break

        # Warn if voting page loaded but no final vote link found
        if not found_vote_link:
            result["senato-votazione-finale-warning"] = "Votazione finale link not found on voting page"
    except Exception as e:
        # If voting data is unavailable, record the failure reason
        result["senato-votazione-finale-warning"] = f"Voting page unavailable: {str(e)[:100]}"

    # Look for data presentazione (submission date)
    for pattern in [
        r'Data(?:\s+di)?\s+presentazione[:\s]+(\d{1,2}/\d{1,2}/\d{4})',
        r'Presentato il[:\s]+(\d{1,2}/\d{1,2}/\d{4})',
    ]:
        data_match = re.search(pattern, resp.text, re.IGNORECASE)
        if data_match:
            result["senato-data-presentazione"] = data_match.group(1)
            break

    # Look for documento links (PDFs, XML, etc.)
    doc_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if any(ext in href.lower() for ext in ['.pdf', '.xml', '.doc', '/stampe/', '/testi/']):
            # Handle protocol-relative URLs (//www.senato.it/...)
            if href.startswith('//'):
                href = 'https:' + href
            elif href.startswith('/'):
                # Relative URL
                href = 'https://www.senato.it' + href
            elif not href.startswith('http'):
                href = 'https://www.senato.it/' + href

            # Clean up any double slashes (except in http://)
            href = re.sub(r'([^:])//+', r'\1/', href)

            if href not in doc_links:
                doc_links.append(href)

    if doc_links:
        result["senato-documenti"] = doc_links

    return result
