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
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from functools import wraps

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS = {"Content-Type": "application/json"}
OUTPUT_DIR = Path(__file__).parent.parent / "data"
VAULT_DIR = Path(__file__).parent.parent / "content" / "leggi"
NORMATTIVA_SITE = "https://www.normattiva.it"

# Global list to track failed requests for manual review
REQUEST_FAILURES = []

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


@retry_request(max_retries=3, initial_delay=2)
def fetch_camera_metadata(session, camera_url: str) -> dict:
    """Fetch and parse metadata from a camera.it RDF endpoint.
    Returns dict with camera-atto, legislatura, natura, data-presentazione, iniziativa-dei-deputati.

    Raises:
        Exception: If HTTP request fails or RDF parsing fails
    """
    result = {}

    # Parse URL to extract legislatura and atto number
    # URL format: http://www.camera.it/uri-res/N2Ls?urn:camera-it:parlamento:scheda.progetto.legge:camera;19.legislatura;1621
    url_match = re.search(r'(\d+)\.legislatura;(\d+)', camera_url)
    if not url_match:
        raise ValueError(f"Could not parse legislatura/atto from camera URL: {camera_url}")

    legislatura = url_match.group(1)
    atto_num = url_match.group(2)
    result["legislatura"] = legislatura
    result["camera-atto"] = f"C. {atto_num}"

    rdf_url = f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{atto_num}"
    result["camera-atto-iri"] = rdf_url

    # Request RDF/XML format explicitly
    resp = session.get(rdf_url, headers={"Accept": "application/rdf+xml"}, timeout=30)
    resp.raise_for_status()
    rdf_text = resp.text

    # Parse RDF/XML
    root = ET.fromstring(rdf_text)

    # Namespaces used in the RDF
    ns = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'ocd': 'http://dati.camera.it/ocd/',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'foaf': 'http://xmlns.com/foaf/0.1/',
    }

    # Find any Description element (the main one)
    atto_elem = None
    for desc in root.findall('.//rdf:Description', ns):
        about = desc.get(f"{{{ns['rdf']}}}about", "")
        if f"ac{legislatura}_{atto_num}" in about:
            atto_elem = desc
            break

    if atto_elem is None:
        # Fallback: try first Description
        descriptions = root.findall('.//rdf:Description', ns)
        if descriptions:
            atto_elem = descriptions[0]

    if atto_elem is None:
        return result

    # Extract natura (dc:type)
    tipo_elem = atto_elem.find('dc:type', ns)
    if tipo_elem is not None and tipo_elem.text:
        result["camera-natura"] = tipo_elem.text.strip()

    # Extract iniziativa (ocd:iniziativa) - Governo or Parlamentare
    iniziativa_elem = atto_elem.find('ocd:iniziativa', ns)
    if iniziativa_elem is not None and iniziativa_elem.text:
        result["camera-iniziativa"] = iniziativa_elem.text.strip()

    # Extract presentation date (dc:date) - format YYYYMMDD
    date_elem = atto_elem.find('dc:date', ns)
    if date_elem is not None and date_elem.text:
        raw_date = date_elem.text.strip()
        # Convert YYYYMMDD to readable format
        if len(raw_date) == 8 and raw_date.isdigit():
            try:
                dt = datetime.strptime(raw_date, "%Y%m%d")
                months = ["", "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
                          "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre"]
                result["camera-data-presentazione"] = f"{dt.day} {months[dt.month]} {dt.year}"
            except ValueError:
                result["camera-data-presentazione"] = raw_date
        else:
            result["camera-data-presentazione"] = raw_date

    # Extract relazioni (related documents) links (dc:relation)
    relazioni_links = []
    for relation_elem in atto_elem.findall('dc:relation', ns):
        resource = relation_elem.get(f"{{{ns['rdf']}}}resource", "")
        if resource and resource.endswith('.pdf'):
            relazioni_links.append(resource)
    if relazioni_links:
        result["camera-relazioni"] = relazioni_links

    # Extract creator (first signer) - dc:creator contains name directly
    deputies = []
    for creator_elem in atto_elem.findall('dc:creator', ns):
        if creator_elem.text:
            name = creator_elem.text.strip()
            if not any(d["name"] == name for d in deputies):
                deputies.append({"name": name, "link": ""})

    # Get primo_firmatario URIs (handle both resource URIs and blank nodes)
    primo_uris = []
    for primo_elem in atto_elem.findall('ocd:primo_firmatario', ns):
        # Try direct resource URI first (parliamentary bills)
        resource = primo_elem.get(f"{{{ns['rdf']}}}resource", "")
        if resource:
            primo_uris.append(resource)
        else:
            # Try blank node (government bills)
            node_id = primo_elem.get(f"{{{ns['rdf']}}}nodeID", "")
            if node_id:
                persona_uri = resolve_blank_node(root, node_id, ns)
                if persona_uri:
                    primo_uris.append(persona_uri)

    # Fetch groups for all primo_firmatario
    for i, dep in enumerate(deputies):
        if i < len(primo_uris):
            group = fetch_parliamentary_group(session, primo_uris[i], ns, legislatura)
            if group:
                dep["group"] = group

    # Extract additional signers (dc:contributor contains names, ocd:altro_firmatario has URIs)
    contributors = []
    for contrib_elem in atto_elem.findall('dc:contributor', ns):
        if contrib_elem.text:
            contributors.append(contrib_elem.text.strip())

    altro_firmatari = []
    for altro_elem in atto_elem.findall('ocd:altro_firmatario', ns):
        resource = altro_elem.get(f"{{{ns['rdf']}}}resource", "")
        if resource:
            altro_firmatari.append(resource)
        else:
            # Try blank node
            node_id = altro_elem.get(f"{{{ns['rdf']}}}nodeID", "")
            if node_id:
                persona_uri = resolve_blank_node(root, node_id, ns)
                if persona_uri:
                    altro_firmatari.append(persona_uri)

    # Match contributors with their groups
    for i, name in enumerate(contributors):
        if not any(d["name"] == name for d in deputies):
            group = ""
            if i < len(altro_firmatari):
                group = fetch_parliamentary_group(session, altro_firmatari[i], ns, legislatura)
            deputies.append({"name": name, "group": group})

    if deputies:
        result["camera-firmatari"] = deputies

    # Extract relatori (rapporteurs)
    relatori_refs = []
    for rel_elem in atto_elem.findall('ocd:rif_relatore', ns):
        resource = rel_elem.get(f"{{{ns['rdf']}}}resource", "")
        if resource:
            relatori_refs.append(resource)

    if relatori_refs:
        relatori = fetch_relatori_names(session, relatori_refs, ns)
        if relatori:
            result["camera-relatori"] = relatori

    # Fetch HTML page for votazione-finale and potentially override with HTML-based firmatari
    html_resp = session.get(camera_url, timeout=30)
    html_resp.raise_for_status()
    html_text = html_resp.text

    # Extract final vote
    voto_match = re.search(r'href="([^"]*votazioni[^"]*schedaVotazione[^"]*)"', html_text)
    if voto_match:
        link = voto_match.group(1).replace("&amp;", "&")
        if not link.startswith("http"):
            link = "https://www.camera.it" + link
        result["camera-votazione-finale"] = link

    # Extract dossier links
    dossier_links = []
    dossier_pattern = r'href="([^"]*dossier[^"]*)"'
    for match in re.finditer(dossier_pattern, html_text, re.IGNORECASE):
        link = match.group(1).replace("&amp;", "&")
        if not link.startswith("http"):
            link = "https://www.camera.it" + link
        if link not in dossier_links:
            dossier_links.append(link)

    if dossier_links:
        result["camera-dossier"] = dossier_links

    # For government bills, parse HTML to get ministerial roles instead of groups
    if html_text and result.get("camera-iniziativa") == "Governo":
        html_firmatari = parse_html_firmatari(html_text, legislatura)
        if html_firmatari:
            result["camera-firmatari"] = html_firmatari

    return result


def parse_html_firmatari(html: str, legislatura: str) -> list:
    """Parse firmatari from HTML page for government bills."""
    firmatari = []

    # Look for <div class="iniziativa"> for government bills
    iniziativa_match = re.search(r'<div class="iniziativa">(.*?)</div>', html, re.DOTALL)
    if not iniziativa_match:
        return firmatari

    section = iniziativa_match.group(1)

    # Extract each person with their role
    # Pattern: <a href="...idPersona=123">NAME</a></span> (<em>ROLE</em>)
    pattern = r'<a\s+href="[^"]*idPersona=(\d+)"[^>]*>([^<]+)</a>\s*</span>\s*\(<em>([^<]+)</em>\)'

    for match in re.finditer(pattern, section):
        person_id = match.group(1)
        name = re.sub(r'\s+', ' ', match.group(2)).strip()
        role = match.group(3).strip()

        firmatari.append({
            "name": name,
            "role": role,
            "link": f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx?sezione=deputati&tipoDoc=schedaDeputato&idlegislatura={legislatura}&idPersona={person_id}"
        })

    return firmatari


@retry_request(max_retries=3, initial_delay=2)
def fetch_relatori_names(session, relatori_refs: list, ns: dict) -> list:
    """Fetch relatore names from their RDF URIs.

    Raises:
        Exception: If HTTP request fails or RDF parsing fails
    """
    relatori = []
    for ref in relatori_refs:
        resp = session.get(ref, headers={"Accept": "application/rdf+xml"}, timeout=10)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        # Find the Description with dc:creator (the relatore name)
        for desc in root.findall('.//rdf:Description', ns):
            creator = desc.find('dc:creator', ns)
            if creator is not None and creator.text:
                name = creator.text.strip()
                if name and name not in relatori:
                    relatori.append(name)
                break
    return relatori


def resolve_blank_node(root, node_id: str, ns: dict) -> str:
    """Resolve a blank node to get the persona/deputato URI."""
    for desc in root.findall('.//rdf:Description', ns):
        desc_node_id = desc.get(f"{{{ns['rdf']}}}nodeID", "")
        if desc_node_id == node_id:
            # Found the blank node, look for ocd:rif_persona
            rif_persona = desc.find('ocd:rif_persona', ns)
            if rif_persona is not None:
                resource = rif_persona.get(f"{{{ns['rdf']}}}resource", "")
                if resource:
                    return resource
    return ""


@retry_request(max_retries=3, initial_delay=2)
def fetch_parliamentary_group(session, person_uri: str, ns: dict, legislatura: str = "19") -> str:
    """Fetch parliamentary group abbreviation from person/deputato RDF.

    Raises:
        Exception: If HTTP request fails or RDF parsing fails
    """
    # If persona.rdf URI, convert to deputato.rdf URI
    # persona.rdf/p50204 → deputato.rdf/d50204_19
    if 'persona.rdf' in person_uri:
        person_match = re.search(r'/p(\d+)', person_uri)
        if person_match:
            person_id = person_match.group(1)
            person_uri = f"http://dati.camera.it/ocd/deputato.rdf/d{person_id}_{legislatura}"

    resp = session.get(person_uri, headers={"Accept": "application/rdf+xml"}, timeout=10)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)

    # Look for gruppo parlamentare reference
    gruppo_uri = None
    for desc in root.findall('.//rdf:Description', ns):
        gruppo_elem = desc.find('ocd:rif_gruppoParlamentare', ns)
        if gruppo_elem is not None:
            gruppo_uri = gruppo_elem.get(f"{{{ns['rdf']}}}resource", "")
            if gruppo_uri:
                break

    if not gruppo_uri:
        return ""

    # Fetch the group RDF to get the abbreviation
    gruppo_resp = session.get(gruppo_uri, headers={"Accept": "application/rdf+xml"}, timeout=10)
    gruppo_resp.raise_for_status()
    gruppo_root = ET.fromstring(gruppo_resp.text)

    # Find the main Description for this group
    for desc in gruppo_root.findall('.//rdf:Description', ns):
        about = desc.get(f"{{{ns['rdf']}}}about", "")
        if about == gruppo_uri:
            # Try ocd:sigla first
            sigla = desc.find('ocd:sigla', ns)
            if sigla is not None and sigla.text:
                return sigla.text.strip()

            # Parse abbreviation from rdfs:label
            # Format: "FULL NAME (ABBREVIATION) (DATE"
            label = desc.find('rdfs:label', ns)
            if label is not None and label.text:
                label_text = label.text.strip()
                # Extract abbreviation from parentheses
                match = re.search(r'\(([A-Z\-]+)\)\s*\(', label_text)
                if match:
                    return match.group(1)
                # Fallback: return full label
                return label_text

    return ""


def build_scheda_link(resource_uri: str, legislatura: str) -> str:
    """Build scheda deputato link from resource URI."""
    # Extract person ID from URI like:
    # - http://dati.camera.it/ocd/deputato.rdf/d50204_19 (deputato)
    # - http://dati.camera.it/ocd/persona.rdf/p50204 (persona)

    # Try deputato format: d{personId}_{legislatura}
    match = re.search(r'/d(\d+)_\d+', resource_uri)
    if match:
        person_id = match.group(1)
        return f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx?sezione=deputati&tipoDoc=schedaDeputato&idlegislatura={legislatura}&idPersona={person_id}"

    # Try persona format: p{personId}
    match = re.search(r'/p(\d+)', resource_uri)
    if match:
        person_id = match.group(1)
        return f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx?sezione=deputati&tipoDoc=schedaDeputato&idlegislatura={legislatura}&idPersona={person_id}"

    return resource_uri


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
    votazioni_url = f"https://www.senato.it/leggi-e-documenti/disegni-di-legge/scheda-ddl?tab=votazioni&did={did}"
    result["senato-votazioni-url"] = votazioni_url

    vot_resp = session.get(votazioni_url, timeout=30)
    vot_resp.raise_for_status()
    vot_soup = BeautifulSoup(vot_resp.text, 'html.parser')

    # Find votazione finale link
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
            break

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


@retry_request(max_retries=3, initial_delay=2)
def fetch_article_html(session, data_gu: str, codice: str, id_articolo: int, data_vigenza: str) -> str:
    """
    Fetch HTML for a SPECIFIC article from Normattiva API.

    IMPORTANT: Must call this for EACH article individually.
    The API returns different HTML for each idArticolo value.

    Args:
        session: requests.Session with established cookies
        data_gu: Data pubblicazione GU (YYYY-MM-DD format)
        codice: Codice redazionale
        id_articolo: Article number (REQUIRED - must fetch each article separately)
        data_vigenza: Data di vigenza from markdown (YYYY-MM-DD format)

    Returns:
        str: HTML of the specific article, or empty string if failed
    """
    url = f"{BASE_URL}/atto/dettaglio-atto"

    payload = {
        "dataGU": data_gu,
        "codiceRedazionale": codice,
        "idArticolo": id_articolo,
        "versione": 0,
        "dataVigenza": data_vigenza,
    }

    try:
        resp = session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        result = resp.json()

        if result.get("success") and "data" in result:
            atto = result["data"].get("atto", {})
            return atto.get("articoloHtml", "")

        return ""

    except Exception as e:
        return ""


@retry_request(max_retries=3, initial_delay=2)
def fetch_approfondimenti(session, uri):
    """Load the N2Ls page, find active approfondimento endpoints, fetch and parse links.
    Returns dict: {column_name: "link1; link2; ...", "gu_link": "..."} for all APPROFONDIMENTO_COLUMNS.

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
        bool: True if a branch exists with pattern legge/{codice}-*, False otherwise
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
            # Check if any branch matches legge/{codice}-*
            pattern = f"origin/legge/{codice}-"
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
                lines.append(f"{col_name}:")
                for link in content.split("\n"):
                    if link.strip():
                        lines.append(f"  - {link.strip()}")

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
        if atto.get("senato-documenti"):
            lines.append("senato-documenti:")
            for doc_link in atto.get("senato-documenti", []):
                lines.append(f"  - {doc_link}")

        lines.append("---")

        # Write frontmatter
        with filepath.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines))

            # Add article HTML content to the body
            articoli = atto.get("articoli", [])
            if articoli:
                f.write("\n\n")  # Add spacing between frontmatter and body
                for art in articoli:
                    f.write(art['html'])
                    f.write("\n")

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
    print("\n[Checking for existing PR branches]")
    pre_pr_count = len(atti)
    atti = [atto for atto in atti if not check_pr_branch_exists(atto.get("codiceRedazionale", ""))]
    pr_filtered_count = pre_pr_count - len(atti)

    if pr_filtered_count > 0:
        print(f"  Skipped {pr_filtered_count} norms with existing PR branches")
    print(f"  Processing {len(atti)} remaining norms\n")

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

    # Fetch article HTML for each atto
    print("[Fetching article HTML]")
    for i, atto in enumerate(atti):
        data_gu = atto.get("dataGU", "")
        codice = atto.get("codiceRedazionale", "")
        data_vigenza = atto.get("data_vigenza", "")

        if not data_gu or not codice or not data_vigenza:
            print(f"  [{i+1}/{len(atti)}] {codice}... skipping (missing data)")
            atto["articoli"] = []
            continue

        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        # Try fetching up to 30 articles (stop when we get empty response)
        articoli = []
        for article_num in range(1, 31):
            html = fetch_article_html(session, data_gu, codice, article_num, data_vigenza)

            if not html or len(html) < 100:
                # No more articles found
                break

            articoli.append({
                "numero": article_num,
                "html": html
            })

            # Small delay to avoid rate limiting
            time.sleep(0.3)

        atto["articoli"] = articoli
        print(f"{len(articoli)} articles ({sum(len(a['html']) for a in articoli):,} chars)")

    # Save after article HTML fetching (most time-consuming and important step)
    print("\n[Saving after article HTML fetch]")
    save_markdown(atti, VAULT_DIR)
    print("  ✅ Markdown files saved with article HTML\n")

    # Fetch camera.it metadata from lavori_preparatori
    print("[Fetching camera.it metadata]")
    for i, atto in enumerate(atti):
        lavori = atto.get("lavori_preparatori", "")
        camera_links = [l for l in lavori.split("\n") if "camera.it" in l]
        if camera_links:
            print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}...", end=" ", flush=True)
            camera_meta = fetch_camera_metadata(session, camera_links[0])
            atto.update(camera_meta)
            print(f"legislatura {camera_meta.get('legislatura', '?')}, {camera_meta.get('camera-atto', '?')}")
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
        senato_links = [l for l in lavori.split("\n") if "senato.it" in l and "ddl" in l]
        if senato_links:
            print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}...", end=" ", flush=True)
            try:
                senato_meta = fetch_senato_metadata(session, senato_links[0])
                atto.update(senato_meta)
                print(f"DDL {senato_meta.get('senato-numero-fase', '?')}, did {senato_meta.get('senato-did', '?')}")
            except Exception as e:
                print(f"error ({str(e)[:50]}...)")
        else:
            print(f"  [{i+1}/{len(atti)}] {atto.get('codiceRedazionale', '')}... no senato.it link")

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
