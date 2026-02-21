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
import json
import requests
import sys
from datetime import datetime
from pathlib import Path

BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS = {"Content-Type": "application/json"}
OUTPUT_DIR = Path(__file__).parent.parent / "data"


def search_laws(search_text: str, max_results: int = 10, order: str = "recente") -> list:
    """Search for laws using the ricerca/semplice API.

    Args:
        search_text: Keywords to search for (in title or text)
        max_results: Maximum number of results to return (default: 10)
        order: Sort order - "recente" (newest first) or "vecchio" (oldest first)

    Returns:
        list: List of matching atti (laws) with metadata
    """
    search_url = f"{BASE_URL}/ricerca/semplice"
    payload = {
        "testoRicerca": search_text,
        "orderType": order,
        "paginazione": {
            "paginaCorrente": 1,
            "numeroElementiPerPagina": max_results
        }
    }

    print(f"Searching for: {search_text}")
    print(f"API: {search_url}")

    try:
        session = requests.Session()
        resp = session.post(search_url, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        lista_atti = data.get("listaAtti", [])
        print(f"\nFound {len(lista_atti)} result(s)")

        return lista_atti

    except requests.exceptions.RequestException as e:
        print(f"Error: Search failed - {str(e)}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return []


def find_exact_match(search_text: str, date: str = None, number: str = None) -> dict | None:
    """Search for a law and return the exact match by date and number.

    Args:
        search_text: Search keywords (e.g., "decreto-legge 27 dicembre 2025 n. 196")
        date: Expected date in YYYY-MM-DD format (optional, for exact matching)
        number: Expected number (optional, for exact matching)

    Returns:
        dict: Matching atto if found, None otherwise
    """
    results = search_laws(search_text, max_results=5)

    if not results:
        return None

    # If date and number provided, find exact match
    if date and number:
        for atto in results:
            if (atto.get("numeroProvvedimento") == number and
                atto.get("dataGU") == date):
                return atto
        print(f"No exact match found for date={date}, number={number}")
        return None

    # Otherwise return first result
    return results[0] if results else None


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

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as f:
        json.dump({"listaAtti": atti, "count": len(atti), "timestamp": datetime.now().isoformat()}, f, indent=2, ensure_ascii=False)

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
