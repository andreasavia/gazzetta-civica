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
from fetch_intervento import try_fetch_xml, parse_xml_stenografico, format_as_markdown


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
                    # Extract seduta ID from URL
                    parsed = urlparse(url)
                    params = parse_qs(parsed.query)
                    seduta_id = params.get('idSeduta', ['unknown'])[0]

                    sedute[seduta_id].append(url)

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

    for i, (seduta_id, urls) in enumerate(sorted(sedute.items()), 1):
        print(f"\n[{i}/{len(sedute)}] Processing seduta {seduta_id} ({len(urls)} interventi)")

        try:
            # Parse first URL to get legislatura
            first_url = urls[0]
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

            # Add metadata
            xml_data['seduta_id'] = seduta_id
            xml_data['anchor_id'] = f"sed{seduta_id}.complete"
            xml_data['url'] = f"Seduta {seduta_id} completa"

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
