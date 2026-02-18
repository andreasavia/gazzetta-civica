#!/usr/bin/env python3
"""
Fetch and extract a specific intervento (speech) from Camera.it stenographic transcripts.

This script takes a URL with an anchor pointing to a specific intervento and extracts
the text of that speech, saving it as a markdown file.

Usage:
    python scripts/fetch_intervento.py <url> [-o output.md]

Example:
    python scripts/fetch_intervento.py "https://www.camera.it/leg19/410?idSeduta=0461&tipo=stenografico#sed0461.stenografico.tit00110.int00010"
"""

import sys
import argparse
import requests
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import xml.etree.ElementTree as ET
import re



def try_fetch_xml(legislatura: str, seduta_id: str) -> tuple[bool, str]:
    """
    Try to fetch the stenographic transcript in XML format.

    Args:
        legislatura: Legislature number (e.g., "19")
        seduta_id: Session ID (e.g., "0461")

    Returns:
        Tuple of (success: bool, content: str)
    """
    xml_url = (
        f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx"
        f"?sezione=assemblea&tipoDoc=formato_xml&tipologia=stenografico"
        f"&idNumero={seduta_id}&idLegislatura={legislatura}"
    )

    print(f"Trying XML format: {xml_url}")

    try:
        response = requests.get(xml_url, timeout=30)
        response.raise_for_status()

        # Check if we got actual XML or an error page
        if 'Documento non disponibile' in response.text or '<html' in response.text[:100]:
            print("  XML format not available")
            return False, ""

        print("  ✓ XML format available")
        return True, response.text
    except Exception as e:
        print(f"  XML fetch failed: {e}")
        return False, ""


def parse_xml_stenografico(xml_content: str, anchor: str) -> dict:
    """
    Parse XML stenographic content and extract speeches.

    XML Structure:
    <seduta legislatura="19" numero="461" anno="2025" mese="04" giorno="02">
      <metadati>
        <dataEstesa>mercoledì 2 aprile 2025</dataEstesa>
      </metadati>
      <resoconto tipo="stenografico">
        <intervento id="tit00110.int00010">
          <testoXHTML>
            <nominativo id="..." cognomeNome="SURNAME Name">SPEAKER NAME</nominativo>
          </testoXHTML>
          <interventoVirtuale>Speech text here</interventoVirtuale>
        </intervento>
      </resoconto>
    </seduta>

    Args:
        xml_content: Raw XML content
        anchor: Anchor ID (e.g., "sed0461.stenografico.tit00110.int00010")

    Returns:
        dict with parsed data
    """
    # Parse XML
    root = ET.fromstring(xml_content)

    # Extract date from dataEstesa
    date_elem = root.find('.//dataEstesa')
    date = date_elem.text if date_elem is not None else 'Unknown date'

    # Extract date components from root attributes for unique filename
    anno = root.get('anno', '')
    mese = root.get('mese', '')
    giorno = root.get('giorno', '')
    date_iso = f"{anno}-{mese.zfill(2)}-{giorno.zfill(2)}" if anno and mese and giorno else 'unknown'

    # Extract section ID from anchor
    # anchor format: "sed0461.stenografico.tit00110.int00010"
    # intervento id format: "tit00110.int00010"
    section_id = None
    if anchor:
        parts = anchor.split('.')
        # Find the part starting with "tit"
        for i, part in enumerate(parts):
            if part.startswith('tit'):
                # Take from "tit" onwards
                section_id = '.'.join(parts[i:])
                break

    print(f"  Looking for section ID: {section_id}")

    # Find all intervento elements
    all_interventi = root.findall('.//intervento')
    print(f"  Found {len(all_interventi)} total interventi in XML")

    speeches = []

    # If we have a section ID, find interventions starting with that prefix
    # Otherwise, get all interventions
    target_interventi = []

    if section_id:
        # If the ID contains a dot, it's a specific intervention (e.g., tit00110.int00010)
        # In this case, we want an exact match.
        if '.' in section_id:
            for intervento in all_interventi:
                if intervento.get('id') == section_id:
                    target_interventi.append(intervento)
        else:
            # It's a title-level ID (e.g., tit00110), match all starting with it
            for intervento in all_interventi:
                intervento_id = intervento.get('id', '')
                if intervento_id.startswith(section_id):
                    target_interventi.append(intervento)
    else:
        target_interventi = all_interventi

    print(f"  Extracting {len(target_interventi)} matching interventi")

    # Extract speeches from target interventions
    for intervento in target_interventi:
        intervento_id = intervento.get('id', '')

        # Extract speaker name
        nominativo = intervento.find('.//nominativo')
        speaker_name = ''
        cognome_nome = ''

        if nominativo is not None:
            speaker_name = nominativo.text or ''
            cognome_nome = nominativo.get('cognomeNome', '')

        # Extract party/group from interventoVirtuale text patterns
        # Party is usually in parentheses after speaker name in the text
        party = ''

        # Extract all text content
        texts = []

        # Get text from interventoVirtuale elements
        for iv in intervento.findall('.//interventoVirtuale'):
            if iv.text:
                texts.append(iv.text.strip())

        # Get text directly from testoXHTML if available
        testo_xhtml = intervento.find('.//testoXHTML')
        if testo_xhtml is not None:
            # Get all text including text from emphasis and other tags
            for elem in testo_xhtml.iter():
                if elem.text and elem.tag != 'nominativo':
                    text = elem.text.strip()
                    if text and text not in texts:
                        texts.append(text)
                if elem.tail:
                    tail = elem.tail.strip()
                    if tail and tail not in texts:
                        texts.append(tail)

        full_text = ' '.join(texts)

        # Try to extract party from text (pattern: "SPEAKER(PARTY).")
        party_match = re.search(r'\(([^)]+)\)\.', full_text[:200])
        if party_match:
            party = party_match.group(1)

        # Use cognomeNome if we have it, otherwise use speaker_name
        display_name = cognome_nome if cognome_nome else speaker_name

        if display_name and full_text:
            # Format name nicely (convert from "SURNAME Name" to "Name Surname")
            if cognome_nome:
                parts = cognome_nome.split()
                if len(parts) >= 2:
                    # Last part is surname, rest is name
                    display_name = ' '.join(parts[::-1])  # Reverse order

            speeches.append({
                'id': intervento_id,
                'speaker': display_name.title(),
                'party': party,
                'text': full_text
            })

    section_title = f"Interventi (da XML)" if section_id else "Seduta completa"

    return {
        'speeches': speeches,
        'section_title': section_title,
        'date': date,
        'date_iso': date_iso
    }


def fetch_intervento(url: str) -> dict:
    """
    Fetch a specific intervento section from a Camera.it stenographic transcript.

    Args:
        url: Full URL with anchor pointing to the intervento section

    Returns:
        dict with keys: section_title, speeches (list), anchor_id, seduta_id, date, url
    """
    # Parse URL to get anchor
    parsed = urlparse(url)
    anchor = parsed.fragment

    # Extract seduta ID and legislatura from query params
    params = parse_qs(parsed.query)
    seduta_id = params.get('idSeduta', ['unknown'])[0]

    # Try to get legislatura from query params first, then from URL path
    legislatura = params.get('idLegislatura', [None])[0]

    if not legislatura:
        # Try to determine legislatura from URL path (e.g., /leg19/)
        legislatura = "19"  # default
        path_parts = parsed.path.split('/')
        for part in path_parts:
            if part.startswith('leg'):
                legislatura = part.replace('leg', '')
                break

    # Fetch XML format
    xml_success, xml_content = try_fetch_xml(legislatura, seduta_id)

    if not xml_success or not xml_content:
        raise ValueError(f"XML format not available for seduta {seduta_id}")

    # Parse XML
    xml_data = parse_xml_stenografico(xml_content, anchor)

    if not xml_data['speeches']:
        raise ValueError(f"No speeches found for anchor {anchor}")

    xml_data['anchor_id'] = anchor
    xml_data['seduta_id'] = seduta_id
    xml_data['url'] = url

    return xml_data


def build_camera_person_url(deputato_iri: str) -> str | None:
    """
    Build a camera.it person URL from a deputato IRI.

    IRI format: http://dati.camera.it/ocd/deputato.rdf/d307388_19
    Output:     https://www.camera.it/deputati/elenco/19-307388
    """
    if not deputato_iri:
        return None
    try:
        last_part = deputato_iri.rstrip('/').split('/')[-1]  # d307388_19
        if not last_part.startswith('d'):
            return None
        inner = last_part[1:]  # 307388_19
        person_id, legislatura = inner.rsplit('_', 1)
        return f"https://www.camera.it/deputati/elenco/{legislatura}-{person_id}"
    except Exception:
        return None


_deputato_label_cache: dict[str, tuple[str, str, str, str] | None] = {}


def lookup_deputato_by_label(label: str, legislatura: str) -> tuple[str, str, str, str] | None:
    """
    Look up a deputato via SPARQL using the name from an intervento label.

    Label format: "intervento di Nome COGNOME"
    Returns (deputato_iri, camera_url, nome_upper, cognome) or None if not found.
    """
    if not label:
        return None

    cache_key = f"{label}|{legislatura}"
    if cache_key in _deputato_label_cache:
        return _deputato_label_cache[cache_key]

    # Strip "intervento di " prefix and parse nome/cognome
    name_part = label.removeprefix("intervento di ").strip()
    tokens = name_part.split()
    if not tokens:
        _deputato_label_cache[cache_key] = None
        return None

    # Cognome = first all-uppercase token onward; nome = tokens before it
    cognome_start = len(tokens)
    for i, token in enumerate(tokens):
        letters = [c for c in token if c.isalpha()]
        if letters and all(c.isupper() for c in letters):
            cognome_start = i
            break

    nome = " ".join(tokens[:cognome_start])
    cognome = " ".join(tokens[cognome_start:])

    if not cognome:
        _deputato_label_cache[cache_key] = None
        return None

    query = f"""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?deputato WHERE {{
        ?deputato rdf:type ocd:deputato .
        ?deputato foaf:firstName ?fn .
        ?deputato foaf:surname ?sn .
        FILTER(CONTAINS(STR(?deputato), "_{legislatura}"))
        FILTER(LCASE(?fn) = "{nome.lower()}" && LCASE(?sn) = "{cognome.lower()}")
    }}
    LIMIT 1
    """

    try:
        response = requests.get(
            "http://dati.camera.it/sparql",
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=15
        )
        response.raise_for_status()
        bindings = response.json().get("results", {}).get("bindings", [])
        if bindings:
            deputato_iri = bindings[0]["deputato"]["value"]
            url = build_camera_person_url(deputato_iri)
            result = (deputato_iri, url, nome.upper(), cognome)
            _deputato_label_cache[cache_key] = result
            return result
    except Exception:
        pass

    _deputato_label_cache[cache_key] = None
    return None


def format_as_markdown(data: dict) -> str:
    """Format the intervento section data as markdown with individual speeches."""
    md = f"# {data['section_title']} - Seduta {data['seduta_id']}\n\n"
    md += f"**Data:** {data['date']}\n\n"
    md += f"**Seduta:** {data['seduta_id']}\n\n"
    md += f"**Numero di interventi:** {len(data['speeches'])}\n\n"
    md += "---\n\n"

    # Add each speech
    for i, speech in enumerate(data['speeches'], 1):
        md += f"## Intervento {i}: {speech['speaker']}"

        if speech['party']:
            md += f" ({speech['party']})"

        md += "\n\n"
        md += f"{speech['text']}\n\n"
        md += "---\n\n"

    md += f"**Fonte:** {data['url']}\n"

    return md


def generate_intervento_filename(data: dict, counter: int = None) -> str:
    """
    Generate a unique filename for the intervento.

    Format: {counter:03d}.md or {date}-sed{seduta_id}.md
    Example: 001.md, 2025-04-02-sed0461.md, etc.
    """
    if counter is not None:
        # Use simple counter-based naming for batch processing
        filename = f"{counter:03d}.md"
    else:
        # Use seduta and date for single file processing
        seduta_id = data['seduta_id']
        date_iso = data.get('date_iso', 'unknown')
        filename = f"{date_iso}-sed{seduta_id}.md"

    return filename


def main():
    parser = argparse.ArgumentParser(
        description="Extract a specific intervento from Camera.it stenographic transcripts"
    )
    parser.add_argument(
        "url",
        help="Full URL with anchor pointing to the intervento"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output markdown file (default: intervento_{anchor_id}.md)"
    )

    args = parser.parse_args()

    try:
        # Fetch the intervento
        print(f"Extracting intervento from: {args.url}")
        data = fetch_intervento(args.url)

        print(f"\nExtracted:")
        print(f"  Section: {data['section_title']}")
        print(f"  Seduta: {data['seduta_id']}")
        print(f"  Anchor: {data['anchor_id']}")
        print(f"  Number of speeches: {len(data['speeches'])}")

        if data['speeches']:
            total_chars = sum(len(s['text']) for s in data['speeches'])
            print(f"  Total text length: {total_chars} characters")
            print(f"\n  Speakers:")
            for speech in data['speeches'][:10]:  # Show first 10
                party = f" ({speech['party']})" if speech['party'] else ""
                print(f"    - {speech['speaker']}{party}")

        # Format as markdown
        markdown = format_as_markdown(data)

        # Determine output path
        if args.output:
            output_str = args.output

            # If output path ends with '/', it's a directory
            if output_str.endswith('/'):
                output_dir = Path(output_str)
                filename = generate_intervento_filename(data)
                output_path = output_dir / filename
            else:
                # Check if it's an existing directory
                output_path = Path(output_str)
                if output_path.exists() and output_path.is_dir():
                    filename = generate_intervento_filename(data)
                    output_path = output_path / filename
                # Otherwise treat as full file path
        else:
            # Generate unique filename in current directory
            filename = generate_intervento_filename(data)
            output_path = Path(filename)

        # Create parent directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save to file
        output_path.write_text(markdown, encoding='utf-8')
        print(f"\n✓ Saved to: {output_path}")

    except Exception as e:
        import traceback
        print(f"\n✗ Error: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
