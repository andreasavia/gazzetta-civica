#!/usr/bin/env python3
"""
Inspect the XML structure of stenographic transcripts from Camera.it.

This script downloads an XML stenographic transcript and displays its structure
to help understand how to parse it.

Usage:
    python scripts/inspect_xml_stenografico.py <legislatura> <seduta_id>

Example:
    python scripts/inspect_xml_stenografico.py 19 0461
"""

import sys
import argparse
import requests
import xml.etree.ElementTree as ET
from pathlib import Path


def fetch_xml_stenografico(legislatura: str, seduta_id: str) -> str:
    """Fetch XML stenographic transcript."""
    xml_url = (
        f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx"
        f"?sezione=assemblea&tipoDoc=formato_xml&tipologia=stenografico"
        f"&idNumero={seduta_id}&idLegislatura={legislatura}"
    )

    print(f"Fetching: {xml_url}")

    response = requests.get(xml_url, timeout=30)
    response.raise_for_status()

    if 'Documento non disponibile' in response.text or '<html' in response.text[:100]:
        raise ValueError("XML document not available")

    return response.text


def print_xml_structure(element, indent=0, max_children=5):
    """Print XML structure in a readable format."""
    prefix = "  " * indent

    # Print element name and attributes
    attrs = " ".join([f'{k}="{v}"' for k, v in element.attrib.items()])
    if attrs:
        print(f"{prefix}<{element.tag} {attrs}>")
    else:
        print(f"{prefix}<{element.tag}>")

    # Print text content if it exists and is not just whitespace
    if element.text and element.text.strip():
        text_preview = element.text.strip()[:100]
        if len(element.text.strip()) > 100:
            text_preview += "..."
        print(f"{prefix}  TEXT: {text_preview}")

    # Print children (limit to max_children to avoid too much output)
    children = list(element)
    if children:
        print(f"{prefix}  ({len(children)} children)")
        for i, child in enumerate(children[:max_children]):
            print_xml_structure(child, indent + 1, max_children)

        if len(children) > max_children:
            print(f"{prefix}  ... and {len(children) - max_children} more")


def find_elements_by_tag(root, tag_name):
    """Find all elements with a specific tag name."""
    return root.findall(f".//{tag_name}")


def main():
    parser = argparse.ArgumentParser(
        description="Inspect XML stenographic transcript structure"
    )
    parser.add_argument(
        "legislatura",
        help="Legislature number (e.g., 19)"
    )
    parser.add_argument(
        "seduta_id",
        help="Session ID (e.g., 0461)"
    )
    parser.add_argument(
        "--save",
        help="Save XML to file"
    )

    args = parser.parse_args()

    try:
        # Fetch XML
        xml_content = fetch_xml_stenografico(args.legislatura, args.seduta_id)

        print(f"✓ Fetched {len(xml_content)} bytes")

        # Save if requested
        if args.save:
            Path(args.save).write_text(xml_content, encoding='utf-8')
            print(f"✓ Saved to {args.save}")

        # Parse XML
        root = ET.fromstring(xml_content)

        print("\n" + "="*80)
        print("XML STRUCTURE OVERVIEW")
        print("="*80)
        print(f"\nRoot element: <{root.tag}>")
        print(f"Root attributes: {root.attrib}")

        # Print structure
        print("\n" + "="*80)
        print("ELEMENT TREE (showing first 5 children at each level)")
        print("="*80 + "\n")
        print_xml_structure(root, max_children=5)

        # Look for common element names that might contain speeches
        print("\n" + "="*80)
        print("SEARCHING FOR KEY ELEMENTS")
        print("="*80)

        search_tags = [
            'intervento', 'speech', 'oratore', 'speaker', 'deputato',
            'testo', 'text', 'seduta', 'session', 'titolo', 'title',
            'gruppo', 'party', 'data', 'date'
        ]

        for tag in search_tags:
            elements = find_elements_by_tag(root, tag)
            if elements:
                print(f"\n<{tag}>: Found {len(elements)} elements")
                if elements:
                    first = elements[0]
                    attrs = " ".join([f'{k}="{v}"' for k, v in first.attrib.items()])
                    print(f"  First element: <{tag} {attrs}>")
                    if first.text and first.text.strip():
                        preview = first.text.strip()[:100]
                        print(f"  Text preview: {preview}...")

        # Check all unique tag names
        print("\n" + "="*80)
        print("ALL UNIQUE TAG NAMES IN DOCUMENT")
        print("="*80)

        all_tags = set()
        for elem in root.iter():
            all_tags.add(elem.tag)

        for tag in sorted(all_tags):
            count = len(find_elements_by_tag(root, tag))
            print(f"  <{tag}>: {count} occurrences")

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
