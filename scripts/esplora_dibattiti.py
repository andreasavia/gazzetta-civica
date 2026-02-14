#!/usr/bin/env python3
"""
esplora_dibattiti.py — Explore interventi/dibattiti for a given Camera.it atto.

Usage:
  python esplora_dibattiti.py http://dati.camera.it/ocd/attocamera.rdf/ac19_1621
  python esplora_dibattiti.py ac19_1621
  python esplora_dibattiti.py 19 1621

This script fetches and analyzes:
  - Basic atto metadata (type, title, presentation date, signers)
  - Interventi (interventions/speeches) linked to the atto
  - Dibattiti (debates) with full discussion details
  - Speaker information (name, group, role)
  - Voting records for the atto
"""

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# Output directory for saving results
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "dibattiti"

# Common namespaces for RDF parsing
NAMESPACES = {
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'ocd': 'http://dati.camera.it/ocd/',
    'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    'foaf': 'http://xmlns.com/foaf/0.1/',
    'owl': 'http://www.w3.org/2002/07/owl#',
}


def parse_atto_identifier(input_str: str) -> tuple[str, str]:
    """Parse atto identifier from various input formats.
    
    Returns:
        Tuple of (legislatura, atto_num)
    
    Examples:
        - http://dati.camera.it/ocd/attocamera.rdf/ac19_1621 → ("19", "1621")
        - ac19_1621 → ("19", "1621")
        - 19 1621 → ("19", "1621")
    """
    # Try full URL format
    url_match = re.search(r'ac(\d+)_(\d+)', input_str)
    if url_match:
        return url_match.group(1), url_match.group(2)
    
    # Try space-separated format
    parts = input_str.strip().split()
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        return parts[0], parts[1]
    
    raise ValueError(f"Could not parse atto identifier from: {input_str}")


def fetch_rdf(session: requests.Session, url: str) -> ET.Element:
    """Fetch and parse RDF/XML from a URL.
    
    Raises:
        Exception: If HTTP request fails or RDF parsing fails
    """
    resp = session.get(url, headers={"Accept": "application/rdf+xml"}, timeout=30)
    resp.raise_for_status()
    return ET.fromstring(resp.text)


def extract_text(element: Optional[ET.Element]) -> str:
    """Extract text from an XML element, returning empty string if None."""
    if element is not None and element.text:
        return element.text.strip()
    return ""


def fetch_atto_metadata(session: requests.Session, legislatura: str, atto_num: str) -> Dict:
    """Fetch basic metadata for an atto.
    
    Returns dict with:
        - atto_iri: Full IRI of the atto
        - legislatura: Legislature number
        - numero: Atto number
        - tipo: Type of atto (dc:type)
        - titolo: Title (dc:title)
        - data_presentazione: Presentation date
        - iniziativa: Government or Parliamentary initiative
        - firmatari: List of signers with names and groups
    """
    atto_iri = f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{atto_num}"
    print(f"Fetching atto metadata from: {atto_iri}")
    
    root = fetch_rdf(session, atto_iri)
    
    # Find the main Description element for this atto
    atto_elem = None
    for desc in root.findall('.//rdf:Description', NAMESPACES):
        about = desc.get(f"{{{NAMESPACES['rdf']}}}about", "")
        if f"ac{legislatura}_{atto_num}" in about:
            atto_elem = desc
            break
    
    if atto_elem is None:
        # Fallback to first Description
        descriptions = root.findall('.//rdf:Description', NAMESPACES)
        if descriptions:
            atto_elem = descriptions[0]
        else:
            raise ValueError(f"No Description elements found in RDF for {atto_iri}")
    
    result = {
        "atto_iri": atto_iri,
        "legislatura": legislatura,
        "numero": atto_num,
    }
    
    # Extract basic metadata
    result["tipo"] = extract_text(atto_elem.find('dc:type', NAMESPACES))
    result["titolo"] = extract_text(atto_elem.find('dc:title', NAMESPACES))
    result["iniziativa"] = extract_text(atto_elem.find('ocd:iniziativa', NAMESPACES))
    
    # Extract presentation date (format YYYYMMDD)
    date_elem = atto_elem.find('dc:date', NAMESPACES)
    if date_elem is not None and date_elem.text:
        raw_date = date_elem.text.strip()
        if len(raw_date) == 8 and raw_date.isdigit():
            try:
                dt = datetime.strptime(raw_date, "%Y%m%d")
                result["data_presentazione"] = dt.strftime("%Y-%m-%d")
                result["data_presentazione_formatted"] = dt.strftime("%d/%m/%Y")
            except ValueError:
                result["data_presentazione"] = raw_date
        else:
            result["data_presentazione"] = raw_date
    
    # Extract firmatari (signers) - collect all creators and contributors
    firmatari = []
    for creator_elem in atto_elem.findall('dc:creator', NAMESPACES):
        name = extract_text(creator_elem)
        if name:
            firmatari.append({"name": name, "role": "primo firmatario"})
    
    for contrib_elem in atto_elem.findall('dc:contributor', NAMESPACES):
        name = extract_text(contrib_elem)
        if name and not any(f["name"] == name for f in firmatari):
            firmatari.append({"name": name, "role": "firmatario"})
    
    result["firmatari"] = firmatari
    
    return result


def fetch_dibattiti(session: requests.Session, legislatura: str, atto_num: str) -> List[Dict]:
    """Fetch all dibattiti (debates) linked to this atto from RDF.
    
    Returns list of dicts with:
        - dibattito_iri: IRI of the debate
        - data: Date of debate
        - seduta: Session number
        - tipo: Type of debate
        - interventi: List of interventions in this debate
    """
    # Get the atto RDF which contains rif_dibattito references
    atto_iri = f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{atto_num}"
    root = fetch_rdf(session, atto_iri)
    
    dibattiti_iris = []
    
    # Find all rif_dibattito references
    for desc in root.findall('.//rdf:Description', NAMESPACES):
        for dib_elem in desc.findall('ocd:rif_dibattito', NAMESPACES):
            dibattito_uri = dib_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "")
            if dibattito_uri and dibattito_uri not in dibattiti_iris:
                dibattiti_iris.append(dibattito_uri)
    
    print(f"Found {len(dibattiti_iris)} dibattito references in RDF")
    
    # Fetch details for each dibattito
    dibattiti = []
    for i, dib_uri in enumerate(dibattiti_iris, 1):
        try:
            print(f"  [{i}/{len(dibattiti_iris)}] Fetching {dib_uri.split('/')[-1]}...", end=" ", flush=True)
            dib_data = fetch_dibattito_details(session, dib_uri)
            dibattiti.append(dib_data)
            print(f"✓ {len(dib_data.get('interventi', []))} interventi")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    return dibattiti


def fetch_dibattito_details(session: requests.Session, dibattito_uri: str) -> Dict:
    """Fetch details for a single dibattito including its discussioni and web links."""
    root = fetch_rdf(session, dibattito_uri)
    
    result = {"dibattito_iri": dibattito_uri}
    
    # Parse dibattito metadata
    for desc in root.findall('.//rdf:Description', NAMESPACES):
        about = desc.get(f"{{{NAMESPACES['rdf']}}}about", "")
        if dibattito_uri in about or not about:
            # Extract basic metadata
            result["data"] = extract_text(desc.find('dc:date', NAMESPACES))
            result["tipo"] = extract_text(desc.find('dc:type', NAMESPACES))
            result["titolo"] = extract_text(desc.find('dc:title', NAMESPACES))
            
            # Extract seduta reference
            seduta_elem = desc.find('ocd:rif_seduta', NAMESPACES)
            if seduta_elem is not None:
                seduta_uri = seduta_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "")
                if seduta_uri:
                    result["seduta_uri"] = seduta_uri
                    # Extract seduta number from URI
                    seduta_match = re.search(r'/s(\d+)', seduta_uri)
                    if seduta_match:
                        result["seduta"] = seduta_match.group(1)
            
            # Extract all interventi in this dibattito
            interventi = []
            for interv_elem in desc.findall('ocd:rif_intervento', NAMESPACES):
                intervento_uri = interv_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "")
                if intervento_uri:
                    interventi.append({"intervento_iri": intervento_uri})
            
            result["interventi"] = interventi
            result["num_interventi"] = len(interventi)
            
            # Extract all discussioni references
            discussioni_iris = []
            for disc_elem in desc.findall('ocd:rif_discussione', NAMESPACES):
                discussione_uri = disc_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "")
                if discussione_uri:
                    discussioni_iris.append(discussione_uri)
            
            # Fetch web page links from each discussione
            discussioni = []
            for disc_uri in discussioni_iris:
                try:
                    disc_data = fetch_discussione_link(session, disc_uri)
                    if disc_data.get('web_link'):
                        discussioni.append(disc_data)
                except Exception as e:
                    # Silently continue if a discussione fetch fails
                    pass
            
            result["discussioni"] = discussioni
            result["num_discussioni"] = len(discussioni)
            
            break
    
    return result


def fetch_discussione_link(session: requests.Session, discussione_uri: str) -> Dict:
    """Fetch the web page link from a discussione RDF."""
    root = fetch_rdf(session, discussione_uri)
    
    result = {"discussione_iri": discussione_uri}
    
    for desc in root.findall('.//rdf:Description', NAMESPACES):
        # Extract dc:relation which contains the web page link
        relation_elem = desc.find('dc:relation', NAMESPACES)
        if relation_elem is not None:
            # The link can be in resource attribute or as text content
            link = relation_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "") or extract_text(relation_elem)
            if link:
                result["web_link"] = link
                break
    
    return result


def fetch_interventi(session: requests.Session, legislatura: str, atto_num: str) -> List[Dict]:
    """Fetch all interventi (interventions) linked to this atto.
    
    DEPRECATED: Use fetch_dibattiti instead, which provides better structure.
    
    Returns list of dicts with:
        - intervento_iri: IRI of the intervention
        - data: Date of intervention
        - seduta: Session number
        - oratore: Speaker name
        - gruppo: Parliamentary group
        - tipo_intervento: Type of intervention
        - testo_url: URL to full text (if available)
    """
    # Query the SPARQL endpoint or parse linked data
    # For now, we'll look for ocd:rif_intervento in the atto RDF
    atto_iri = f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{atto_num}"
    root = fetch_rdf(session, atto_iri)
    
    interventi = []
    
    # Find all rif_intervento references
    for desc in root.findall('.//rdf:Description', NAMESPACES):
        for interv_elem in desc.findall('ocd:rif_intervento', NAMESPACES):
            intervento_uri = interv_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "")
            if intervento_uri:
                # Fetch details for this intervento
                try:
                    interv_data = fetch_intervento_details(session, intervento_uri)
                    interventi.append(interv_data)
                except Exception as e:
                    print(f"  Warning: Could not fetch intervento {intervento_uri}: {e}")
    
    return interventi


def fetch_intervento_details(session: requests.Session, intervento_uri: str) -> Dict:
    """Fetch details for a single intervento."""
    root = fetch_rdf(session, intervento_uri)
    
    result = {"intervento_iri": intervento_uri}
    
    # Parse intervento metadata
    for desc in root.findall('.//rdf:Description', NAMESPACES):
        about = desc.get(f"{{{NAMESPACES['rdf']}}}about", "")
        if intervento_uri in about or not about:
            # Extract metadata
            result["data"] = extract_text(desc.find('dc:date', NAMESPACES))
            result["oratore"] = extract_text(desc.find('dc:creator', NAMESPACES))
            result["tipo"] = extract_text(desc.find('dc:type', NAMESPACES))
            
            # Extract seduta reference
            seduta_elem = desc.find('ocd:rif_seduta', NAMESPACES)
            if seduta_elem is not None:
                seduta_uri = seduta_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "")
                if seduta_uri:
                    result["seduta_uri"] = seduta_uri
                    # Extract seduta number from URI
                    seduta_match = re.search(r'/s(\d+)', seduta_uri)
                    if seduta_match:
                        result["seduta"] = seduta_match.group(1)
            
            # Extract deputato reference for group info
            dep_elem = desc.find('ocd:rif_deputato', NAMESPACES)
            if dep_elem is not None:
                dep_uri = dep_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "")
                if dep_uri:
                    result["deputato_uri"] = dep_uri
            
            break
    
    return result


def fetch_votazioni(session: requests.Session, legislatura: str, atto_num: str) -> List[Dict]:
    """Fetch votazioni (votes) related to this atto.
    
    Returns list of dicts with:
        - votazione_iri: IRI of the vote
        - data: Date of vote
        - seduta: Session number
        - esito: Result (approvato/respinto)
        - presenti: Number present
        - votanti: Number voting
        - favorevoli: Votes in favor
        - contrari: Votes against
        - astenuti: Abstentions
    """
    # Fetch atto to find votazione references
    atto_iri = f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{atto_num}"
    root = fetch_rdf(session, atto_iri)
    
    votazioni = []
    
    # Find all rif_votazione references
    for desc in root.findall('.//rdf:Description', NAMESPACES):
        for vot_elem in desc.findall('ocd:rif_votazione', NAMESPACES):
            votazione_uri = vot_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "")
            if votazione_uri:
                try:
                    vot_data = fetch_votazione_details(session, votazione_uri)
                    votazioni.append(vot_data)
                except Exception as e:
                    print(f"  Warning: Could not fetch votazione {votazione_uri}: {e}")
    
    return votazioni


def fetch_votazione_details(session: requests.Session, votazione_uri: str) -> Dict:
    """Fetch details for a single votazione."""
    root = fetch_rdf(session, votazione_uri)
    
    result = {"votazione_iri": votazione_uri}
    
    for desc in root.findall('.//rdf:Description', NAMESPACES):
        about = desc.get(f"{{{NAMESPACES['rdf']}}}about", "")
        if votazione_uri in about or not about:
            result["data"] = extract_text(desc.find('dc:date', NAMESPACES))
            result["esito"] = extract_text(desc.find('ocd:esito', NAMESPACES))
            result["presenti"] = extract_text(desc.find('ocd:presenti', NAMESPACES))
            result["votanti"] = extract_text(desc.find('ocd:votanti', NAMESPACES))
            result["favorevoli"] = extract_text(desc.find('ocd:favorevoli', NAMESPACES))
            result["contrari"] = extract_text(desc.find('ocd:contrari', NAMESPACES))
            result["astenuti"] = extract_text(desc.find('ocd:astenuti', NAMESPACES))
            
            # Extract seduta reference
            seduta_elem = desc.find('ocd:rif_seduta', NAMESPACES)
            if seduta_elem is not None:
                seduta_uri = seduta_elem.get(f"{{{NAMESPACES['rdf']}}}resource", "")
                if seduta_uri:
                    result["seduta_uri"] = seduta_uri
                    seduta_match = re.search(r'/s(\d+)', seduta_uri)
                    if seduta_match:
                        result["seduta"] = seduta_match.group(1)
            
            break
    
    return result


def query_sparql_interventi(session: requests.Session, legislatura: str, atto_num: str) -> List[Dict]:
    """Query Camera.it SPARQL endpoint for interventi related to this atto.
    
    Returns list of interventi with detailed information.
    """
    sparql_endpoint = "http://dati.camera.it/sparql"
    
    # SPARQL query to find all interventi related to this atto
    query = f"""
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT DISTINCT ?intervento ?data ?oratore ?tipo ?seduta WHERE {{
        ?intervento ocd:rif_atto <http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{atto_num}> .
        OPTIONAL {{ ?intervento dc:date ?data . }}
        OPTIONAL {{ ?intervento dc:creator ?oratore . }}
        OPTIONAL {{ ?intervento dc:type ?tipo . }}
        OPTIONAL {{ ?intervento ocd:rif_seduta ?seduta_uri . }}
        BIND(REPLACE(STR(?seduta_uri), "^.*/s(\\\\d+).*$", "$1") AS ?seduta)
    }}
    ORDER BY ?data
    """
    
    print(f"Querying SPARQL endpoint for interventi...")
    
    try:
        resp = session.post(
            sparql_endpoint,
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=60
        )
        resp.raise_for_status()
        results = resp.json()
        
        interventi = []
        for binding in results.get("results", {}).get("bindings", []):
            intervento = {
                "intervento_iri": binding.get("intervento", {}).get("value", ""),
                "data": binding.get("data", {}).get("value", ""),
                "oratore": binding.get("oratore", {}).get("value", ""),
                "tipo": binding.get("tipo", {}).get("value", ""),
                "seduta": binding.get("seduta", {}).get("value", ""),
            }
            interventi.append(intervento)
        
        return interventi
        
    except Exception as e:
        print(f"  Warning: SPARQL query failed: {e}")
        return []


def query_sparql_votazioni(session: requests.Session, legislatura: str, atto_num: str) -> List[Dict]:
    """Query Camera.it SPARQL endpoint for votazioni related to this atto.
    
    Returns list of votazioni with detailed information.
    """
    sparql_endpoint = "http://dati.camera.it/sparql"
    
    # SPARQL query to find all votazioni related to this atto
    query = f"""
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    
    SELECT DISTINCT ?votazione ?data ?esito ?presenti ?votanti ?favorevoli ?contrari ?astenuti ?seduta WHERE {{
        ?votazione ocd:rif_atto <http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{atto_num}> .
        OPTIONAL {{ ?votazione dc:date ?data . }}
        OPTIONAL {{ ?votazione ocd:esito ?esito . }}
        OPTIONAL {{ ?votazione ocd:presenti ?presenti . }}
        OPTIONAL {{ ?votazione ocd:votanti ?votanti . }}
        OPTIONAL {{ ?votazione ocd:favorevoli ?favorevoli . }}
        OPTIONAL {{ ?votazione ocd:contrari ?contrari . }}
        OPTIONAL {{ ?votazione ocd:astenuti ?astenuti . }}
        OPTIONAL {{ ?votazione ocd:rif_seduta ?seduta_uri . }}
        BIND(REPLACE(STR(?seduta_uri), "^.*/s(\\\\d+).*$", "$1") AS ?seduta)
    }}
    ORDER BY ?data
    """
    
    print(f"Querying SPARQL endpoint for votazioni...")
    
    try:
        resp = session.post(
            sparql_endpoint,
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=60
        )
        resp.raise_for_status()
        results = resp.json()
        
        votazioni = []
        for binding in results.get("results", {}).get("bindings", []):
            votazione = {
                "votazione_iri": binding.get("votazione", {}).get("value", ""),
                "data": binding.get("data", {}).get("value", ""),
                "esito": binding.get("esito", {}).get("value", ""),
                "presenti": binding.get("presenti", {}).get("value", ""),
                "votanti": binding.get("votanti", {}).get("value", ""),
                "favorevoli": binding.get("favorevoli", {}).get("value", ""),
                "contrari": binding.get("contrari", {}).get("value", ""),
                "astenuti": binding.get("astenuti", {}).get("value", ""),
                "seduta": binding.get("seduta", {}).get("value", ""),
            }
            votazioni.append(votazione)
        
        return votazioni
        
    except Exception as e:
        print(f"  Warning: SPARQL query failed: {e}")
        return []


def fetch_dibattiti_from_web(session: requests.Session, legislatura: str, atto_num: str) -> List[Dict]:
    """Scrape dibattiti information from Camera.it web interface.
    
    This complements the RDF data with additional information from the HTML pages.
    """
    # Construct the web URL for this atto
    web_url = f"https://www.camera.it/leg{legislatura}/126?tab=2&leg={legislatura}&idDocumento={atto_num}"
    
    print(f"Fetching dibattiti from web: {web_url}")
    
    try:
        resp = session.get(web_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  Warning: Could not fetch web page: {e}")
        return []
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    dibattiti = []
    
    # Look for "Resoconto" (transcript) links
    for link in soup.find_all('a', href=True):
        href = link['href']
        if 'resoconti' in href.lower() or 'resoconto' in href.lower():
            dibattito = {
                "url": href if href.startswith('http') else f"https://www.camera.it{href}",
                "text": link.get_text(strip=True),
            }
            # Extract date if available from link text
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', dibattito["text"])
            if date_match:
                dibattito["data"] = date_match.group(1)
            
            if dibattito not in dibattiti:
                dibattiti.append(dibattito)
    
    return dibattiti


def print_summary(metadata: Dict, dibattiti: List[Dict], votazioni: List[Dict]):
    """Print a human-readable summary of the atto and its debates."""
    print("\n" + "=" * 80)
    print("ATTO METADATA")
    print("=" * 80)
    print(f"IRI:                {metadata['atto_iri']}")
    print(f"Legislatura:        {metadata['legislatura']}")
    print(f"Numero:             {metadata['numero']}")
    print(f"Tipo:               {metadata.get('tipo', 'N/A')}")
    print(f"Iniziativa:         {metadata.get('iniziativa', 'N/A')}")
    print(f"Data presentazione: {metadata.get('data_presentazione_formatted', metadata.get('data_presentazione', 'N/A'))}")
    print(f"\nTitolo:")
    print(f"  {metadata.get('titolo', 'N/A')}")
    
    if metadata.get('firmatari'):
        print(f"\nFirmatari ({len(metadata['firmatari'])})")
        for i, f in enumerate(metadata['firmatari'][:10], 1):
            print(f"  {i}. {f['name']} ({f.get('role', 'firmatario')})")
        if len(metadata['firmatari']) > 10:
            print(f"  ... and {len(metadata['firmatari']) - 10} more")
    
    print("\n" + "=" * 80)
    print("DIBATTITI (DEBATES)")
    print("=" * 80)
    if dibattiti:
        total_interventi = sum(d.get('num_interventi', 0) for d in dibattiti)
        print(f"Total dibattiti found: {len(dibattiti)}")
        print(f"Total interventi across all dibattiti: {total_interventi}\n")
        
        for i, dib in enumerate(dibattiti[:20], 1):
            print(f"{i}. {dib.get('data', 'N/A')} - Seduta {dib.get('seduta', 'N/A')}")
            if dib.get('tipo'):
                print(f"   Tipo: {dib.get('tipo')}")
            if dib.get('titolo'):
                print(f"   Titolo: {dib.get('titolo')}")
            print(f"   Interventi: {dib.get('num_interventi', 0)}")
            print(f"   IRI: {dib['dibattito_iri']}")
            print()
        if len(dibattiti) > 20:
            print(f"... and {len(dibattiti) - 20} more dibattiti")
    else:
        print("No dibattiti found in RDF data.")
    
    print("\n" + "=" * 80)
    print("VOTAZIONI (VOTES)")
    print("=" * 80)
    if votazioni:
        print(f"Total votazioni found: {len(votazioni)}\n")
        for i, vot in enumerate(votazioni, 1):
            print(f"{i}. {vot.get('data', 'N/A')} - Seduta {vot.get('seduta', 'N/A')}")
            print(f"   Esito: {vot.get('esito', 'N/A')}")
            if vot.get('favorevoli'):
                print(f"   Favorevoli: {vot.get('favorevoli')}, Contrari: {vot.get('contrari')}, Astenuti: {vot.get('astenuti')}")
                print(f"   Presenti: {vot.get('presenti')}, Votanti: {vot.get('votanti')}")
            print(f"   IRI: {vot['votazione_iri']}")
            print()
    else:
        print("No votazioni found in RDF data.")


def save_json(data: Dict, filepath: Path):
    """Save data to JSON file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open('w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nSaved JSON to: {filepath}")


def save_links_markdown(metadata: Dict, dibattiti: List[Dict], filepath: Path):
    """Save all discussioni web links to a markdown file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    lines = []
    lines.append(f"# Dibattiti per {metadata.get('camera-atto', metadata.get('numero'))}")
    lines.append(f"")
    lines.append(f"**Atto**: {metadata.get('atto_iri')}")
    lines.append(f"**Legislatura**: {metadata.get('legislatura')}")
    lines.append(f"**Titolo**: {metadata.get('titolo', 'N/A')}")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")
    
    total_discussioni = sum(len(d.get('discussioni', [])) for d in dibattiti)
    lines.append(f"## Summary")
    lines.append(f"")
    lines.append(f"- **Total dibattiti**: {len(dibattiti)}")
    lines.append(f"- **Total discussioni with links**: {total_discussioni}")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")
    
    # Group dibattiti by type/commission
    for i, dib in enumerate(dibattiti, 1):
        titolo = dib.get('titolo', 'N/A')
        data = dib.get('data', 'N/A')
        discussioni = dib.get('discussioni', [])
        
        if not discussioni:
            continue
        
        lines.append(f"## {i}. {titolo}")
        lines.append(f"")
        lines.append(f"- **Data**: {data}")
        lines.append(f"- **Dibattito IRI**: {dib.get('dibattito_iri')}")
        lines.append(f"- **Discussioni**: {len(discussioni)}")
        lines.append(f"")
        
        if discussioni:
            lines.append(f"### Web Links")
            lines.append(f"")
            for j, disc in enumerate(discussioni, 1):
                web_link = disc.get('web_link', '')
                if web_link:
                    # Try to extract a readable title from the URL
                    # Format: sezione=bollettini&...&anno=2024&mese=10&giorno=09
                    date_match = re.search(r'anno=(\d+)&mese=(\d+)&giorno=(\d+)', web_link)
                    if date_match:
                        anno, mese, giorno = date_match.groups()
                        title = f"Bollettino {giorno}/{mese}/{anno}"
                    else:
                        title = f"Discussione {j}"
                    
                    lines.append(f"{j}. [{title}]({web_link})")
            lines.append(f"")
        
        lines.append(f"")
    
    with filepath.open('w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"Saved markdown links to: {filepath}")



def main():
    parser = argparse.ArgumentParser(
        description="Explore interventi/dibattiti for a Camera.it atto.",
        epilog="Example: python esplora_dibattiti.py ac19_1621"
    )
    parser.add_argument(
        "atto",
        nargs='+',
        help="Atto identifier (e.g., 'http://dati.camera.it/ocd/attocamera.rdf/ac19_1621' or 'ac19_1621' or '19 1621')"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output JSON file path (default: data/dibattiti/ac{leg}_{num}.json)"
    )
    
    args = parser.parse_args()
    
    # Parse atto identifier
    atto_input = ' '.join(args.atto)
    try:
        legislatura, atto_num = parse_atto_identifier(atto_input)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Exploring atto: Legislatura {legislatura}, Numero {atto_num}")
    print()
    
    # Create session
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    
    # Fetch data
    try:
        metadata = fetch_atto_metadata(session, legislatura, atto_num)
        print(f"✓ Fetched atto metadata\n")
        
        # Fetch dibattiti (debates) - this is the main method
        print("[Fetching dibattiti from RDF]")
        dibattiti = fetch_dibattiti(session, legislatura, atto_num)
        print()
        
        # Fetch votazioni
        print("[Fetching votazioni]")
        votazioni = query_sparql_votazioni(session, legislatura, atto_num)
        if not votazioni:
            # Fallback to RDF parsing if SPARQL fails
            votazioni = fetch_votazioni(session, legislatura, atto_num)
        print(f"✓ Found {len(votazioni)} votazioni")
        print()
        
        # Try to fetch additional info from web
        dibattiti_web = fetch_dibattiti_from_web(session, legislatura, atto_num)
        if dibattiti_web:
            print(f"✓ Found {len(dibattiti_web)} additional dibattiti from web")
        
    except Exception as e:
        print(f"Error fetching data: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Print summary
    print_summary(metadata, dibattiti, votazioni)
    
    # Print web dibattiti if found
    if dibattiti_web:
        print("\n" + "=" * 80)
        print("ADDITIONAL DIBATTITI FROM WEB")
        print("=" * 80)
        for i, dib in enumerate(dibattiti_web, 1):
            print(f"{i}. {dib.get('text', 'N/A')}")
            print(f"   Data: {dib.get('data', 'N/A')}")
            print(f"   URL: {dib['url']}")
            print()
    
    # Save to JSON
    output_data = {
        "metadata": metadata,
        "dibattiti": dibattiti,
        "votazioni": votazioni,
        "dibattiti_web": dibattiti_web,
        "fetched_at": datetime.now().isoformat(),
    }
    
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = OUTPUT_DIR / f"ac{legislatura}_{atto_num}.json"
    
    save_json(output_data, output_path)
    
    # Also save discussioni links to markdown file
    markdown_path = output_path.with_suffix('.md')
    save_links_markdown(metadata, dibattiti, markdown_path)
    
    print("\n" + "=" * 80)
    print("DONE")
    print("=" * 80)


if __name__ == "__main__":
    main()
