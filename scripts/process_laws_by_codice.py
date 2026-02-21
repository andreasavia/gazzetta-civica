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
import requests
from pathlib import Path

# Import from library modules
from lib.processing_pipeline import process_laws_full_pipeline
from lib.normattiva_api import search_laws
from lib.persistence import save_json
from lib.state import save_failures_and_warnings

# Constants
OUTPUT_DIR = Path(__file__).parent.parent / "data"
VAULT_DIR = Path(__file__).parent.parent / "content" / "leggi"


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

    # Process laws through full pipeline
    processed_laws = process_laws_full_pipeline(atti, session, VAULT_DIR)

    # Save processed laws metadata for GitHub Actions
    if processed_laws:
        new_laws_file = OUTPUT_DIR / "new_laws.json"
        save_json({"new_laws": processed_laws}, new_laws_file)
        print(f"\n✓ Processed laws metadata: {new_laws_file} ({len(processed_laws)} laws)")

    # Save failures and warnings
    save_failures_and_warnings(OUTPUT_DIR)

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
