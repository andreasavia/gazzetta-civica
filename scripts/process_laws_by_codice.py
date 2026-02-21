#!/usr/bin/env python3
"""
process_laws_by_codice.py — Process specific laws by their codice redazionale.

Usage:
  python process_laws_by_codice.py 26G00036 25G00196
  python process_laws_by_codice.py --from-search-results data/ricerca_semplice_*.json

This script processes individual laws through the full pipeline:
- Fetch normattiva permalink
- Fetch lavori preparatori (camera.it + senato.it)
- Fetch full text HTML
- Generate markdown files
- Create new_laws.json for PR creation
"""

import argparse
import json
import sys
from pathlib import Path

# Import from ricerca_normattiva
from ricerca_normattiva import (
    fetch_normattiva_permalink,
    fetch_full_text_via_export,
    save_markdown,
    save_json,
    OUTPUT_DIR,
    VAULT_DIR,
    REQUEST_FAILURES,
    MISSING_REFERENCES
)
from lib.camera import fetch_camera_metadata
from lib.senato import fetch_senato_metadata
import requests
from datetime import datetime


def process_laws(atti: list, session) -> list:
    """Process a list of atti through the full pipeline.

    Args:
        atti: List of atti with at least codiceRedazionale and dataGU
        session: Requests session

    Returns:
        list: Processed laws metadata for PR creation
    """
    print(f"\n[Processing {len(atti)} law(s)]")

    # Fetch normattiva permalink for each atto
    print("\n[Fetching Normattiva permalinks]")
    for i, atto in enumerate(atti):
        codice = atto.get("codiceRedazionale", "")
        data_gu = atto.get("dataGU", "")

        if not codice or not data_gu:
            print(f"  [{i+1}/{len(atti)}] Skipping - missing codice or dataGU")
            continue

        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        try:
            permalink_data = fetch_normattiva_permalink(session, data_gu, codice)
            atto.update(permalink_data)
            print("✓")
        except Exception as e:
            print(f"✗ {str(e)[:50]}")

    # Fetch Camera.it metadata
    print("\n[Fetching Camera.it metadata (lavori preparatori)]")
    for i, atto in enumerate(atti):
        codice = atto.get("codiceRedazionale", "")
        lavori_url = atto.get("lavori_preparatori")

        if not lavori_url:
            print(f"  [{i+1}/{len(atti)}] {codice}... no lavori-preparatori link")
            continue

        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        try:
            camera_meta = fetch_camera_metadata(session, lavori_url)
            atto.update(camera_meta)

            if camera_meta:
                print(f"AC {camera_meta.get('camera-numero-atto', '?')}")
            else:
                print("no data")
        except Exception as e:
            print(f"error ({str(e)[:50]})")

    # Fetch Senato.it metadata
    print("\n[Fetching Senato.it metadata (lavori preparatori)]")
    for i, atto in enumerate(atti):
        codice = atto.get("codiceRedazionale", "")
        senato_url = atto.get("senato-url")

        if not senato_url:
            print(f"  [{i+1}/{len(atti)}] {codice}... no senato.it link")
            continue

        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        try:
            senato_meta = fetch_senato_metadata(session, senato_url)
            atto.update(senato_meta)
            print(f"DDL {senato_meta.get('senato-numero-fase', '?')}, did {senato_meta.get('senato-did', '?')}")

            if senato_meta.get("senato-votazione-finale-warning"):
                print(f"    ⚠ {senato_meta.get('senato-votazione-finale-warning')}")
        except Exception as e:
            print(f"error ({str(e)[:50]})")

    # Fetch full text HTML
    print("\n[Fetching full text HTML via export endpoint]")
    for i, atto in enumerate(atti):
        codice = atto.get("codiceRedazionale", "")
        data_gu = atto.get("dataGU", "")

        if not codice or not data_gu:
            print(f"  [{i+1}/{len(atti)}] {codice}... skipping (missing data)")
            atto["full_text_html"] = ""
            continue

        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        try:
            full_html = fetch_full_text_via_export(session, data_gu, codice)
            atto["full_text_html"] = full_html

            if full_html:
                print(f"{len(full_html):,} chars")
            else:
                print("no content found")
        except Exception as e:
            print(f"error ({str(e)[:50]})")
            atto["full_text_html"] = ""

    # Save markdown files
    print("\n[Saving markdown files]")
    processed_laws = save_markdown(atti, VAULT_DIR)

    return processed_laws


def main():
    parser = argparse.ArgumentParser(
        description="Process specific laws by codice redazionale",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process specific laws by codice
  python process_laws_by_codice.py 26G00036 25G00196

  # Process laws from search results
  python process_laws_by_codice.py --from-search-results data/ricerca_semplice_20260220_120000.json
        """
    )

    parser.add_argument("codici", nargs="*", help="Codici redazionali to process")
    parser.add_argument("--from-search-results", type=Path, help="JSON file with search results")

    args = parser.parse_args()

    atti = []

    # Load from search results file
    if args.from_search_results:
        if not args.from_search_results.exists():
            print(f"Error: File not found: {args.from_search_results}", file=sys.stderr)
            sys.exit(1)

        with args.from_search_results.open("r", encoding="utf-8") as f:
            data = json.load(f)
            atti = data.get("listaAtti", [])

        print(f"Loaded {len(atti)} law(s) from {args.from_search_results}")

    # Load from codici arguments
    elif args.codici:
        print(f"Processing {len(args.codici)} law(s) by codice...")

        # We need to search for each codice to get the full atto data
        from ricerca_semplice import search_laws

        for codice in args.codici:
            print(f"\nSearching for {codice}...")
            results = search_laws(codice, max_results=5)

            # Find exact match by codice
            found = False
            for atto in results:
                if atto.get("codiceRedazionale") == codice:
                    atti.append(atto)
                    found = True
                    print(f"  ✓ Found: {atto.get('descrizioneAtto')}")
                    break

            if not found:
                print(f"  ✗ Not found: {codice}")

    else:
        print("Error: Must provide either codici or --from-search-results", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    if not atti:
        print("No laws to process")
        sys.exit(0)

    # Create session
    session = requests.Session()

    # Process laws
    processed_laws = process_laws(atti, session)

    # Save processed laws metadata for GitHub Actions
    if processed_laws:
        new_laws_file = OUTPUT_DIR / "new_laws.json"
        save_json({"new_laws": processed_laws}, new_laws_file)
        print(f"\n✓ Processed laws metadata: {new_laws_file} ({len(processed_laws)} laws)")

    # Save request failures
    if REQUEST_FAILURES:
        failures_file = OUTPUT_DIR / "request_failures.json"
        save_json({
            "failures": REQUEST_FAILURES,
            "count": len(REQUEST_FAILURES),
            "timestamp": datetime.now().isoformat()
        }, failures_file)
        print(f"\n⚠ Request failures: {failures_file} ({len(REQUEST_FAILURES)} failures)")

    # Save missing references
    if MISSING_REFERENCES:
        missing_refs_file = OUTPUT_DIR / "missing_references.json"
        save_json({
            "missing_references": MISSING_REFERENCES,
            "count": len(MISSING_REFERENCES),
            "timestamp": datetime.now().isoformat()
        }, missing_refs_file)
        print(f"\n⚠ Missing references: {missing_refs_file} ({len(MISSING_REFERENCES)} missing)")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
