#!/usr/bin/env python3
"""
camera.py — Extract metadata from Camera.it (Italian Chamber of Deputies)

Provides functions to:
- Fetch and parse bill metadata from RDF endpoints
- Extract parliamentary groups, signers, and rapporteurs
- Parse HTML pages for additional metadata
- Extract Assembly debate information (Esame in Assemblea)

Usage:
    from camera import fetch_camera_metadata, fetch_esame_assemblea
"""

import re
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup


def fetch_camera_metadata(session, camera_url: str, interventi_dir: str = None) -> dict:
    """Fetch and parse metadata from a camera.it RDF endpoint.

    Args:
        session: requests.Session for HTTP requests
        camera_url: Camera.it bill URL
        interventi_dir: Optional directory path to save interventi data and markdown files

    Returns dict with camera-atto, legislatura, natura, data-presentazione,
    iniziativa-dei-deputati, firmatari, relatori, votazione-finale, and dossier.

    If interventi_dir is provided, also fetches Assembly debates and generates markdown files.

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
                from datetime import datetime
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

    # Store HTML content for reuse (to avoid duplicate fetches)
    result["_html_content"] = html_text

    # If interventi directory is provided, fetch and save Assembly debates
    if interventi_dir:
        import json
        from pathlib import Path
        from utils.parse_esame_assemblea import parse_esame_assemblea, enrich_with_stenografico_text, generate_markdown_files

        try:
            # Create interventi directory if it doesn't exist
            interventi_path = Path(interventi_dir)
            interventi_path.mkdir(parents=True, exist_ok=True)

            # Parse Esame in Assemblea section from the already-fetched HTML
            esame_data = parse_esame_assemblea(html_text)

            if esame_data and esame_data.get('sessions'):
                # Enrich with stenographic text from XML
                esame_data = enrich_with_stenografico_text(esame_data, fetch_xml=True)

                # Save JSON
                esame_json_path = interventi_path / "esame_assemblea.json"
                with esame_json_path.open('w', encoding='utf-8') as f:
                    json.dump(esame_data, f, ensure_ascii=False, indent=2)

                # Generate markdown files
                generate_markdown_files(esame_data, str(interventi_path))

                # Store summary in result
                total_sessions = len(esame_data.get('sessions', []))
                total_interventions = sum(
                    len(p.get('interventions', []))
                    for s in esame_data.get('sessions', [])
                    for p in s.get('phases', [])
                )
                result["_interventi_summary"] = {
                    "sessions": total_sessions,
                    "interventions": total_interventions
                }
        except Exception as e:
            # Store error but don't fail the whole metadata fetch
            result["_interventi_error"] = str(e)[:200]

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


def fetch_esame_assemblea(session, camera_url: str, fetch_text: bool = True, html_content: str = None) -> dict:
    """Fetch and parse 'Esame in Assemblea' (Assembly examination) from camera.it.

    This extracts the complete debate structure including sessions, phases, speakers,
    and optionally the full stenographic text of each intervention.

    Args:
        session: requests.Session for HTTP requests
        camera_url: Camera.it bill URL (e.g., http://www.camera.it/uri-res/N2Ls?urn:...)
        fetch_text: Whether to fetch XML stenographic text for interventions
        html_content: Optional pre-fetched HTML content (to avoid duplicate fetches)

    Returns:
        dict with 'sessions' containing parsed debate data, or empty dict if not found
    """
    from utils.parse_esame_assemblea import parse_esame_assemblea, enrich_with_stenografico_text

    try:
        # Use provided HTML content or fetch it
        if html_content is None:
            resp = session.get(camera_url, timeout=30)
            resp.raise_for_status()
            html_content = resp.text

        # Parse the Esame in Assemblea section
        data = parse_esame_assemblea(html_content)

        # Enrich with stenographic text if requested
        if fetch_text and data.get('sessions'):
            data = enrich_with_stenografico_text(data, fetch_xml=True)

        return data

    except Exception as e:
        print(f"    ⚠ Could not fetch Esame in Assemblea: {str(e)[:100]}")
        return {}
