#!/usr/bin/env python3
"""
Fetch all interventi from a dibattiti.json file, grouped by seduta.

This script reads a dibattiti.json file, groups interventi by seduta (session),
and saves one markdown file per seduta containing all interventi from that session.

Usage:
    python scripts/fetch_all_interventi.py path/to/dibattiti.json

Example:
    python scripts/fetch_all_interventi.py "content/leggi/2026/01/07/n. 1/dibattiti.json"
"""

import sys
import json
import argparse
from pathlib import Path
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
from fetch_intervento import try_fetch_xml, parse_xml_stenografico, format_as_markdown, build_camera_person_url, lookup_deputato_by_label


def extract_interventi_by_seduta(dibattiti_data: dict) -> dict:
    """
    Extract intervento URLs from dibattiti.json and group by seduta.

    Returns:
        Dict mapping seduta_id to list of URLs
    """
    sedute = defaultdict(list)

    for dibattito in dibattiti_data.get('dibattiti', []):
        for discussione in dibattito.get('discussioni', []):
            for intervento in discussione.get('interventi', []):
                url = intervento.get('testo')

                if url:
                    # Extract seduta ID and anchor from URL
                    parsed = urlparse(url)
                    params = parse_qs(parsed.query)
                    seduta_id = params.get('idSeduta', ['unknown'])[0]

                    # Extract the intervention ID part from the anchor fragment
                    anchor = parsed.fragment
                    if anchor:
                        parts = anchor.split('.')
                        for j, part in enumerate(parts):
                            if part.startswith('tit'):
                                anchor = '.'.join(parts[j:])
                                break

                    sedute[seduta_id].append({
                        'url': url,
                        'anchor': anchor,
                        'deputato': intervento.get('deputato')
                    })

    return sedute


def main():
    parser = argparse.ArgumentParser(
        description="Extract all interventi from a dibattiti.json file, grouped by seduta"
    )
    parser.add_argument(
        "dibattiti_json",
        help="Path to dibattiti.json file"
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for interventi (default: same directory as dibattiti.json, in 'interventi' subfolder)"
    )

    args = parser.parse_args()

    # Read dibattiti.json
    dibattiti_path = Path(args.dibattiti_json)

    if not dibattiti_path.exists():
        print(f"✗ Error: {dibattiti_path} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Reading: {dibattiti_path}")

    with open(dibattiti_path, 'r', encoding='utf-8') as f:
        dibattiti_data = json.load(f)

    # Build seduta_id → discussione mapping for frontmatter enrichment
    seduta_to_discussione = {}
    for _dib in dibattiti_data.get('dibattiti', []):
        for _disc in _dib.get('discussioni', []):
            for _int in _disc.get('interventi', []):
                _testo = _int.get('testo', '')
                if _testo:
                    _sid = parse_qs(urlparse(_testo).query).get('idSeduta', [''])[0]
                    if _sid:
                        seduta_to_discussione[_sid] = _disc
                        break

    # Extract interventi grouped by seduta
    sedute = extract_interventi_by_seduta(dibattiti_data)

    total_interventi = sum(len(urls) for urls in sedute.values())
    print(f"Found {total_interventi} interventi across {len(sedute)} sedute")

    if not sedute:
        print("No interventi found in dibattiti.json")
        return

    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # If dibattiti.json is already in an 'interventi' folder, use that
        # Otherwise, create an 'interventi' subfolder in the same directory
        if dibattiti_path.parent.name == "interventi":
            output_dir = dibattiti_path.parent
        else:
            output_dir = dibattiti_path.parent / "interventi"

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")

    # Process each seduta
    successful = 0
    failed = 0
    speeches_by_anchor = {}  # anchor → speech text, accumulated across all sedute

    for i, (seduta_id, items) in enumerate(sorted(sedute.items()), 1):
        print(f"\n[{i}/{len(sedute)}] Processing seduta {seduta_id} ({len(items)} interventi)")

        try:
            # Parse first URL to get legislatura
            first_url = items[0]['url']
            parsed = urlparse(first_url)
            params = parse_qs(parsed.query)
            legislatura = params.get('idLegislatura', ['19'])[0]

            # Fetch XML for this seduta (only once!)
            print(f"  Fetching XML for seduta {seduta_id}...")
            xml_success, xml_content = try_fetch_xml(legislatura, seduta_id)

            if not xml_success or not xml_content:
                print(f"  ✗ XML not available for seduta {seduta_id}")
                failed += 1
                continue

            # Parse XML to get ALL interventi from this seduta
            # Use empty anchor to get all interventi
            xml_data = parse_xml_stenografico(xml_content, anchor="")

            if not xml_data['speeches']:
                print(f"  ✗ No speeches found in seduta {seduta_id}")
                failed += 1
                continue

            # Filter speeches based on the anchors found in dibattiti.json
            json_anchors = {item['anchor'] for item in items if item['anchor']}
            if json_anchors:
                original_count = len(xml_data['speeches'])
                xml_data['speeches'] = [
                    s for s in xml_data['speeches']
                    if any(s['id'].startswith(a) for a in json_anchors)
                ]
                print(f"  Found {len(xml_data['speeches'])} matching interventions (out of {original_count} in XML)")

            if not xml_data['speeches']:
                print(f"  ✗ None of the requested interventions found in XML for seduta {seduta_id}")
                failed += 1
                continue

            # Add metadata
            xml_data['seduta_id'] = seduta_id
            xml_data['anchor_id'] = ", ".join(sorted(json_anchors)) if json_anchors else f"sed{seduta_id}.complete"
            xml_data['url'] = items[0]['url']  # Use first URL as reference

            # Add frontmatter metadata from dibattiti.json
            disc = seduta_to_discussione.get(seduta_id, {})
            xml_data['frontmatter'] = {
                'tipo': 'INTERVENTI',
                'camera-atto-iri': dibattiti_data['metadata']['atto_iri'],
                'seduta': seduta_id,
                'data-seduta': xml_data.get('date_iso', ''),
                'seduta-url': disc.get('seduta_url', ''),
                'argomento': disc.get('argomento', ''),
            }

            # Accumulate speech texts for JSON enrichment
            for speech in xml_data['speeches']:
                speeches_by_anchor[speech['id']] = speech['text']

            # Generate markdown
            markdown = format_as_markdown(xml_data)

            # Generate filename: {date}-sed{seduta_id}.md
            date_iso = xml_data.get('date_iso', 'unknown')
            filename = f"{date_iso}-sed{seduta_id}.md"
            output_path = output_dir / filename

            # Save to file
            output_path.write_text(markdown, encoding='utf-8')

            print(f"  ✓ Saved: {filename} ({len(xml_data['speeches'])} interventi)")
            successful += 1

        except Exception as e:
            print(f"  ✗ Failed: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            failed += 1

    # Enrich dibattiti.json with speech texts
    if speeches_by_anchor:
        enriched = 0
        for dibattito in dibattiti_data.get('dibattiti', []):
            for discussione in dibattito.get('discussioni', []):
                # Build seduta_url from first intervento's testo URL (drop &ancora= and fragment)
                seduta_url = None
                for item in discussione.get('interventi', []):
                    first_testo = item.get('testo', '')
                    if first_testo:
                        url_no_ancora = first_testo
                        if '&ancora=' in url_no_ancora:
                            url_no_ancora = url_no_ancora[:url_no_ancora.index('&ancora=')]
                        if '#' in url_no_ancora:
                            url_no_ancora = url_no_ancora[:url_no_ancora.index('#')]
                        seduta_url = url_no_ancora
                        break
                if seduta_url and 'seduta_url' not in discussione:
                    new_disc = {}
                    for k, v in discussione.items():
                        new_disc[k] = v
                        if k == 'dataSeduta':
                            new_disc['seduta_url'] = seduta_url
                    discussione.clear()
                    discussione.update(new_disc)
                for intervento in discussione.get('interventi', []):
                    testo_url = intervento.get('testo', '')
                    if not testo_url:
                        continue
                    anchor = urlparse(testo_url).fragment
                    if anchor:
                        parts = anchor.split('.')
                        for j, part in enumerate(parts):
                            if part.startswith('tit'):
                                anchor = '.'.join(parts[j:])
                                break
                    deputato_url = build_camera_person_url(intervento.get('deputato'))
                    needs_lookup = (not deputato_url) or (not intervento.get('nome')) or (not intervento.get('cognome'))
                    if needs_lookup and intervento.get('label'):
                        leg = parse_qs(urlparse(testo_url).query).get('idlegislatura', ['19'])[0]
                        found = lookup_deputato_by_label(intervento['label'], leg)
                        if found:
                            deputato_iri, deputato_url, nome, cognome = found
                            intervento['deputato'] = deputato_iri
                            intervento['nome'] = nome
                            intervento['cognome'] = cognome
                    if deputato_url:
                        intervento['deputato_url'] = deputato_url
                    if anchor in speeches_by_anchor:
                        intervento['testo_stenografico'] = speeches_by_anchor[anchor]
                        enriched += 1
        dibattiti_path.write_text(
            json.dumps(dibattiti_data, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )
        print(f"\n✓ Enriched dibattiti.json with {enriched} speech texts")

    # Summary
    print("\n" + "="*80)
    print(f"Summary:")
    print(f"  Total sedute: {len(sedute)}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total interventi: {total_interventi}")
    print(f"  Output: {output_dir}")
    print("="*80)


if __name__ == "__main__":
    main()
