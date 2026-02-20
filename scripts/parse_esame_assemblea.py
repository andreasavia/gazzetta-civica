#!/usr/bin/env python3
"""
Parse "Esame in Assemblea" section from Camera.it HTML pages.

This script extracts structured information about Assembly debates,
including sessions, phases, speakers, and their interventions.

Usage:
    python scripts/parse_esame_assemblea.py <html_file> [-o output.json]
"""

import sys
import json
import argparse
import re
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
import requests
import xml.etree.ElementTree as ET


def fetch_xml_stenografico(seduta_id, legislatura='19'):
    """
    Fetch the XML stenographic transcript for a session.

    Args:
        seduta_id: Session ID (e.g., "0608")
        legislatura: Legislature number (default: "19")

    Returns:
        Tuple of (success: bool, xml_content: str)
    """
    xml_url = (
        f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx"
        f"?sezione=assemblea&tipoDoc=formato_xml&tipologia=stenografico"
        f"&idNumero={seduta_id}&idLegislatura={legislatura}"
    )

    print(f"  Fetching XML for seduta {seduta_id}...", end=" ", flush=True)

    try:
        response = requests.get(xml_url, timeout=30)
        response.raise_for_status()

        # Check if we got actual XML or an error page
        if 'Documento non disponibile' in response.text or '<html' in response.text[:100]:
            print("not available")
            return False, ""

        print("✓")
        return True, response.text
    except Exception as e:
        print(f"error: {e}")
        return False, ""


def extract_text_from_xml_intervento(xml_root, anchor):
    """
    Extract the text content for a specific anchor from XML.

    Args:
        xml_root: Parsed XML root element
        anchor: Anchor ID (e.g., "sed0608.stenografico.tit00080.sub00010.int00080")

    Returns:
        dict with extracted text and metadata, or None if not found
    """
    # Extract the intervento ID from the anchor
    # Format: sed0608.stenografico.tit00080.sub00010.int00080
    # We want: tit00080.sub00010.int00080
    parts = anchor.split('.')

    # Find the part starting with "tit"
    intervento_id = None
    for i, part in enumerate(parts):
        if part.startswith('tit'):
            # Take from "tit" onwards
            intervento_id = '.'.join(parts[i:])
            break

    if not intervento_id:
        return None

    # Find the intervento element with this ID
    intervento_elem = xml_root.find(f".//intervento[@id='{intervento_id}']")

    if intervento_elem is None:
        return None

    result = {
        'intervento_id': intervento_id,
        'text': '',
        'speaker_xml': ''
    }

    # Extract speaker from nominativo element
    nominativo = intervento_elem.find('.//nominativo')
    if nominativo is not None:
        result['speaker_xml'] = nominativo.text or ''

    # Extract all text content from interventoVirtuale elements
    texts = []
    for iv in intervento_elem.findall('.//interventoVirtuale'):
        if iv.text:
            texts.append(iv.text.strip())

    # Also get text from testoXHTML if available
    testo_xhtml = intervento_elem.find('.//testoXHTML')
    if testo_xhtml is not None:
        for elem in testo_xhtml.iter():
            if elem.text and elem.tag != 'nominativo':
                text = elem.text.strip()
                if text and text not in texts:
                    texts.append(text)
            if elem.tail:
                tail = elem.tail.strip()
                if tail and tail not in texts:
                    texts.append(tail)

    result['text'] = ' '.join(texts)

    return result if result['text'] else None


def enrich_with_stenografico_text(data, fetch_xml=True):
    """
    Enrich the parsed data with stenographic text from XML.

    Args:
        data: Parsed data dictionary
        fetch_xml: Whether to fetch XML (set to False for testing)

    Returns:
        Updated data dictionary with text content added
    """
    if not fetch_xml:
        return data

    print("\nFetching stenographic XML transcripts...")

    # Collect all unique seduta IDs and their legislatura
    seduta_info = {}
    for session in data['sessions']:
        for phase in session.get('phases', []):
            for intervention in phase.get('interventions', []):
                for page_ref in intervention.get('page_references', []):
                    seduta_id = page_ref.get('seduta_id')
                    legislatura = intervention.get('legislatura', '19')
                    if seduta_id:
                        seduta_info[seduta_id] = legislatura

    # Fetch and cache XML for each seduta
    xml_cache = {}
    for seduta_id, legislatura in seduta_info.items():
        success, xml_content = fetch_xml_stenografico(seduta_id, legislatura)
        if success and xml_content:
            try:
                xml_root = ET.fromstring(xml_content)
                xml_cache[seduta_id] = xml_root
            except Exception as e:
                print(f"  Error parsing XML for seduta {seduta_id}: {e}")

    # Now enrich each intervention with its text
    print("\nExtracting intervention texts...")
    extracted_count = 0
    total_count = 0

    for session in data['sessions']:
        for phase in session.get('phases', []):
            for intervention in phase.get('interventions', []):
                for page_ref in intervention.get('page_references', []):
                    total_count += 1
                    seduta_id = page_ref.get('seduta_id')
                    anchor = page_ref.get('anchor')

                    if seduta_id in xml_cache and anchor:
                        xml_root = xml_cache[seduta_id]
                        text_data = extract_text_from_xml_intervento(xml_root, anchor)

                        if text_data:
                            # Add text to the page reference
                            page_ref['stenografico_text'] = text_data['text']
                            page_ref['intervento_id'] = text_data['intervento_id']
                            extracted_count += 1

    print(f"  Extracted text for {extracted_count}/{total_count} page references")

    return data


def extract_person_info(intervento_li):
    """
    Extract information about a person's intervention.

    Args:
        intervento_li: BeautifulSoup element with class "intervento"

    Returns:
        dict with person and intervention details
    """
    person_data = {}

    # Extract photo URL
    photo_img = intervento_li.find('img')
    if photo_img:
        person_data['photo_url'] = photo_img.get('src', '')
        person_data['photo_alt'] = photo_img.get('alt', '')

    # Extract person link and details
    person_link = intervento_li.find('a', href=lambda h: h and 'schedaDeputato' in h)
    if person_link:
        person_url = person_link.get('href', '')
        person_data['profile_url'] = person_url

        # Extract person ID from URL
        params = parse_qs(urlparse(person_url).query)
        person_data['person_id'] = params.get('idPersona', [''])[0]
        person_data['legislatura'] = params.get('idlegislatura', [''])[0]

    # Extract name and party from the text
    # Format: "NAME (PARTY) Role pag. LINK"
    text_span = intervento_li.find('span', recursive=False)
    if text_span:
        # Get the person link text for name
        name_link = text_span.find('a')
        if name_link:
            person_data['name'] = name_link.get_text(strip=True)

    # Extract party from the full li text (it's outside the span)
    # Get all text content from the li element
    li_text = intervento_li.get_text()
    party_match = re.search(r'\(([^)]+)\)', li_text)
    if party_match:
        person_data['party'] = party_match.group(1)

    # Extract role if present (text in <em> tags - also outside the span)
    role_em = intervento_li.find('em')
    if role_em:
        person_data['role'] = role_em.get_text(strip=True).strip(',').strip()

    # Extract page references and links
    page_links = []
    for page_link in intervento_li.find_all('a', class_='go-to-page'):
        page_data = {
            'page_number': page_link.get_text(strip=True),
            'url': page_link.get('href', ''),
            'title': page_link.get('title', '')
        }

        # Extract anchor from URL
        parsed_url = urlparse(page_data['url'])
        params = parse_qs(parsed_url.query)
        page_data['seduta_id'] = params.get('idSeduta', [''])[0]
        page_data['anchor'] = params.get('ancora', [''])[0]

        page_links.append(page_data)

    person_data['page_references'] = page_links

    return person_data


def parse_fase(fase_li):
    """
    Parse a phase (fase) of the Assembly examination.

    Args:
        fase_li: BeautifulSoup <li> element containing a phase

    Returns:
        dict with phase details
    """
    fase_data = {}

    # Extract phase title
    fase_span = fase_li.find('span', class_='titoloFase', recursive=False)
    if fase_span:
        # Get title text (excluding page numbers)
        title_text = fase_span.get_text()
        # Remove page references from title
        title_clean = re.sub(r',\s*pag\..*$', '', title_text).strip()
        fase_data['title'] = title_clean

        # Extract page references from the span
        page_links = []
        for page_link in fase_span.find_all('a', class_='go-to-page'):
            page_data = {
                'page_number': page_link.get_text(strip=True),
                'url': page_link.get('href', '')
            }

            # Extract parameters from URL
            parsed_url = urlparse(page_data['url'])
            params = parse_qs(parsed_url.query)
            page_data['seduta_id'] = params.get('idSeduta', [''])[0]
            page_data['anchor'] = params.get('ancora', [''])[0]

            page_links.append(page_data)

        fase_data['page_references'] = page_links

    # Extract interventions
    interventions = []
    interventi_ul = fase_li.find('ul', recursive=False)
    if interventi_ul:
        for intervento_li in interventi_ul.find_all('li', class_='intervento', recursive=False):
            person_info = extract_person_info(intervento_li)
            if person_info:
                interventions.append(person_info)

    fase_data['interventions'] = interventions

    return fase_data


def parse_seduta(seduta_li):
    """
    Parse a single seduta (session) from the Assembly examination.

    Args:
        seduta_li: BeautifulSoup <li> element containing a seduta

    Returns:
        dict with seduta details
    """
    seduta_data = {}

    # Extract seduta header (session number and date)
    seduta_span = seduta_li.find('span', class_='sedutaAssemblea')
    if seduta_span:
        seduta_text = seduta_span.get_text(strip=True)

        # Parse session number and date
        # Format: "Seduta n. XXX del DD mese YYYY"
        match = re.search(r'Seduta n\.\s*(\d+)\s+del\s+(.+)', seduta_text)
        if match:
            seduta_data['session_number'] = match.group(1)
            seduta_data['date'] = match.group(2)

    # Extract main title
    title_span = seduta_li.find('span', class_='titolo', recursive=False)
    if title_span:
        # Get title text (excluding page numbers)
        title_text = title_span.get_text()
        # Remove page references from title
        title_clean = re.sub(r',\s*pag\..*$', '', title_text).strip()
        seduta_data['title'] = title_clean

        # Extract page references
        page_links = []
        for page_link in title_span.find_all('a', class_='go-to-page'):
            page_data = {
                'page_number': page_link.get_text(strip=True),
                'url': page_link.get('href', '')
            }

            # Extract parameters from URL
            parsed_url = urlparse(page_data['url'])
            params = parse_qs(parsed_url.query)
            page_data['seduta_id'] = params.get('idSeduta', [''])[0]
            page_data['anchor'] = params.get('ancora', [''])[0]

            page_links.append(page_data)

        seduta_data['page_references'] = page_links

    # Find nested list for phases
    phases = []
    nested_ul = seduta_li.find('ul', recursive=False)
    if nested_ul:
        # Direct children li elements that are NOT interventions
        for child_li in nested_ul.find_all('li', recursive=False):
            # Check if this is a fase (has titoloFase) or a direct intervention list
            if child_li.find('span', class_='titoloFase'):
                fase_data = parse_fase(child_li)
                if fase_data:
                    phases.append(fase_data)
            # Handle direct interventions (interventions not under a fase)
            elif child_li.find('li', class_='intervento'):
                # This is a phase without explicit titoloFase
                phase_interventions = []
                for intervento_li in child_li.find_all('li', class_='intervento', recursive=False):
                    person_info = extract_person_info(intervento_li)
                    if person_info:
                        phase_interventions.append(person_info)

                if phase_interventions:
                    phases.append({
                        'title': 'Direct interventions',
                        'interventions': phase_interventions
                    })

    seduta_data['phases'] = phases

    return seduta_data


def parse_esame_assemblea(html_content):
    """
    Parse the "Esame in Assemblea" section from Camera.it HTML.

    Args:
        html_content: HTML content as string

    Returns:
        dict with structured data about Assembly examination
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the "Esame in Assemblea" section
    esame_section = soup.find('div', id='discussione')

    if not esame_section:
        raise ValueError("Could not find 'Esame in Assemblea' section (div#discussione)")

    # Extract all sedute
    sedute = []
    # The ul is inside a card-body div
    card_body = esame_section.find('div', class_='card-body')
    seduta_list = card_body.find('ul', recursive=False) if card_body else None

    if seduta_list:
        for seduta_li in seduta_list.find_all('li', recursive=False):
            # Check if this li contains a seduta (has sedutaAssemblea span)
            if seduta_li.find('span', class_='sedutaAssemblea'):
                seduta_data = parse_seduta(seduta_li)
                if seduta_data:
                    sedute.append(seduta_data)

    return {
        'section': 'Esame in Assemblea',
        'sessions': sedute,
        'total_sessions': len(sedute)
    }


def generate_markdown_files(data, output_dir):
    """
    Generate markdown files for each seduta from the parsed data.

    Args:
        data: Parsed data dictionary
        output_dir: Directory to save markdown files
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for session in data['sessions']:
        session_num = session.get('session_number', 'unknown')
        date = session.get('date', '')
        title = session.get('title', '')

        # Convert date format if needed (from "DD mese YYYY" to "DD/MM/YYYY")
        date_formatted = date
        month_map = {
            'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
            'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
            'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
        }
        date_match = re.search(r'(\d+)\s+(\w+)\s+(\d{4})', date)
        if date_match:
            day = date_match.group(1).zfill(2)
            month_name = date_match.group(2).lower()
            year = date_match.group(3)
            month_num = month_map.get(month_name, '00')
            date_formatted = f"{day}/{month_num}/{year}"

        # Collect page references
        page_refs = []
        for page_ref in session.get('page_references', []):
            if page_ref.get('url'):
                page_refs.append(page_ref['url'])

        # Build markdown content
        md_content = "---\n"
        md_content += f"seduta_id: {session_num}\n"
        md_content += f"date: {date_formatted}\n"
        md_content += f"title: {title}\n"
        if page_refs:
            md_content += "page_references:\n"
            for ref in page_refs:
                md_content += f"  - {ref}\n"
        md_content += "---\n\n"

        # Add phases and interventions
        for phase in session.get('phases', []):
            phase_title = phase.get('title', 'Untitled Phase')
            md_content += f"# {phase_title}\n\n"

            for intervention in phase.get('interventions', []):
                name = intervention.get('name', 'Unknown')
                party = intervention.get('party', '')
                role = intervention.get('role', '')

                # Build intervention header
                header_parts = [f'Intervento di "{name}"']
                if party:
                    header_parts.append(f'"{party}"')
                if role:
                    header_parts.append(f'("{role}")')

                md_content += f"## {' - '.join(header_parts)}\n\n"

                # Add stenographic text from page references
                for page_ref in intervention.get('page_references', []):
                    steno_text = page_ref.get('stenografico_text', '')
                    if steno_text:
                        md_content += f"{steno_text}\n\n"

                # If no stenographic text was found, add a note
                if not any(pr.get('stenografico_text') for pr in intervention.get('page_references', [])):
                    md_content += "*[Testo stenografico non disponibile]*\n\n"

        # Save to file
        filename = f"seduta_{session_num}.md"
        filepath = output_path / filename
        filepath.write_text(md_content, encoding='utf-8')
        print(f"  ✓ Generated: {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract 'Esame in Assemblea' section from Camera.it HTML"
    )
    parser.add_argument(
        'html_file',
        help='Path to the HTML file to parse'
    )
    parser.add_argument(
        '-o', '--output',
        help='Output JSON file (default: esame_assemblea.json)',
        default='esame_assemblea.json'
    )
    parser.add_argument(
        '--pretty',
        action='store_true',
        help='Pretty-print JSON output'
    )
    parser.add_argument(
        '--fetch-text',
        action='store_true',
        help='Fetch stenographic text from XML for each intervention'
    )
    parser.add_argument(
        '--generate-md',
        action='store_true',
        help='Generate markdown files for each seduta'
    )
    parser.add_argument(
        '--md-output-dir',
        help='Directory for markdown output (default: sedute_md)',
        default='sedute_md'
    )

    args = parser.parse_args()

    # Read HTML file
    html_path = Path(args.html_file)
    if not html_path.exists():
        print(f"Error: File not found: {html_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading HTML file: {html_path}")
    html_content = html_path.read_text(encoding='utf-8')

    # Parse the section
    print("Parsing 'Esame in Assemblea' section...")
    try:
        data = parse_esame_assemblea(html_content)

        # Enrich with stenographic text if requested
        if args.fetch_text:
            data = enrich_with_stenografico_text(data, fetch_xml=True)

        print(f"\nExtracted data:")
        print(f"  Total sessions: {data['total_sessions']}")

        # Count total phases and interventions
        total_phases = sum(len(s.get('phases', [])) for s in data['sessions'])
        total_interventions = sum(
            len(p.get('interventions', []))
            for s in data['sessions']
            for p in s.get('phases', [])
        )

        print(f"  Total phases: {total_phases}")
        print(f"  Total interventions: {total_interventions}")

        # Save to JSON
        output_path = Path(args.output)
        with output_path.open('w', encoding='utf-8') as f:
            if args.pretty:
                json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                json.dump(data, f, ensure_ascii=False)

        print(f"\n✓ Saved to: {output_path}")

        # Generate markdown files if requested
        if args.generate_md:
            print(f"\nGenerating markdown files...")
            generate_markdown_files(data, args.md_output_dir)
            print(f"✓ Markdown files saved to: {args.md_output_dir}/")

    except Exception as e:
        import traceback
        print(f"\n✗ Error: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
