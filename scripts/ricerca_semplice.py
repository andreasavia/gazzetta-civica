#!/usr/bin/env python3
"""
ricerca_semplice.py — Search for laws using the Normattiva ricerca/semplice API.

Usage:
  python ricerca_semplice.py "decreto-legge 27 dicembre 2025 n. 196"
  python ricerca_semplice.py "legge 15 marzo 2024 n. 42"

This script searches for laws by keywords and can be used to:
- Find specific laws by title/number/date
- Discover laws related to a topic
- Get metadata for laws without full processing
"""

import argparse
from datetime import datetime
from pathlib import Path

# Import from library modules
from lib.normattiva_api import search_laws
from lib.persistence import save_json

OUTPUT_DIR = Path(__file__).parent.parent / "data"


def print_results(atti: list, verbose: bool = False):
    """Print search results in a readable format.

    Args:
        atti: List of atti from search results
        verbose: If True, print full details; if False, print summary
    """
    if not atti:
        print("\nNo results found")
        return

    print("\n" + "=" * 80)
    print(f"Results ({len(atti)})")
    print("=" * 80)

    for i, atto in enumerate(atti, 1):
        codice = atto.get("codiceRedazionale", "N/A")
        data_gu = atto.get("dataGU", "N/A")
        descrizione = atto.get("descrizioneAtto", "N/A")
        titolo = atto.get("titoloAtto", "").strip("[] ")

        print(f"\n[{i}] {codice}")
        print(f"    Data GU: {data_gu}")
        print(f"    Descrizione: {descrizione}")

        if verbose:
            print(f"    Titolo: {titolo[:100]}{'...' if len(titolo) > 100 else ''}")
            print(f"    Numero GU: {atto.get('numeroGU', 'N/A')}")
            print(f"    Denominazione: {atto.get('denominazioneAtto', 'N/A')}")
            if atto.get("numeroSupplemento") and atto.get("numeroSupplemento") != "0":
                print(f"    Supplemento: {atto.get('tipoSupplementoIt', '')}")

    print("\n" + "=" * 80)


def save_results(atti: list, output_file: Path = None):
    """Save search results to JSON file.

    Args:
        atti: List of atti to save
        output_file: Output file path (default: data/ricerca_semplice_<timestamp>.json)
    """
    if not atti:
        print("No results to save")
        return

    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUT_DIR / f"ricerca_semplice_{timestamp}.json"

    save_json({
        "listaAtti": atti,
        "count": len(atti),
        "timestamp": datetime.now().isoformat()
    }, output_file)

    print(f"\nResults saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Search for Italian laws using the Normattiva ricerca/semplice API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for a specific decreto-legge
  python ricerca_semplice.py "decreto-legge 27 dicembre 2025 n. 196"

  # Search by topic
  python ricerca_semplice.py "consultazioni elettorali"

  # Search and save results
  python ricerca_semplice.py "bilancio 2026" --save

  # Search with more results
  python ricerca_semplice.py "ambiente" --max-results 20

  # Verbose output
  python ricerca_semplice.py "legge di bilancio" -v
        """
    )

    parser.add_argument("search_text", help="Keywords to search for")
    parser.add_argument("--max-results", type=int, default=10, help="Maximum number of results (default: 10)")
    parser.add_argument("--order", choices=["recente", "vecchio"], default="recente", help="Sort order (default: recente)")
    parser.add_argument("--save", action="store_true", help="Save results to JSON file")
    parser.add_argument("--output", type=Path, help="Output file path (default: data/ricerca_semplice_<timestamp>.json)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Search for laws
    atti = search_laws(args.search_text, max_results=args.max_results, order=args.order)

    # Print results
    print_results(atti, verbose=args.verbose)

    # Save if requested
    if args.save:
        save_results(atti, output_file=args.output)


if __name__ == "__main__":
    main()
