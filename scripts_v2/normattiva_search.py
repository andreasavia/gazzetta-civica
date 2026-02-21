"""
normattiva_search.py — Fetch laws from the Normattiva API.

Responsibility: paginate the /ricerca/avanzata endpoint for a given year/month
and return a list of `Legge` objects ready for downstream enrichment.

Usage:
    python normattiva_search.py 2026 1
    python normattiva_search.py 2026 1 --per-pagina 50
    python normattiva_search.py 2026 1 --dump          # saves raw + Legge JSON to dump_2026_01.json
"""

from __future__ import annotations

import argparse
import calendar
import dataclasses
import json
import time
from functools import wraps
from pathlib import Path

import requests

from models import Legge

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL = "https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1"
HEADERS = {"Content-Type": "application/json"}


# ── Retry decorator ───────────────────────────────────────────────────────────

def retry_request(max_retries: int = 3, initial_delay: float = 2.0, backoff_factor: float = 2.0):
    """Decorator: retry HTTP calls with exponential backoff on network errors.

    Args:
        max_retries: How many times to retry before giving up.
        initial_delay: Seconds to wait before first retry.
        backoff_factor: Multiplier applied to delay on each subsequent retry.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exc: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, ConnectionError, TimeoutError) as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        print(f"  ⚠ Retry {attempt + 1}/{max_retries} after {delay}s: {str(exc)[:100]}")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        print(f"  ✗ Failed after {max_retries} retries: {str(exc)[:100]}")
                        raise

            raise RuntimeError("Unreachable") from last_exc

        return wrapper
    return decorator


# ── API call ──────────────────────────────────────────────────────────────────

@retry_request(max_retries=3, initial_delay=2)
def _ricerca_avanzata_page(data_inizio: str, data_fine: str, pagina: int, per_pagina: int) -> dict:
    """Fetch a single page from the /ricerca/avanzata endpoint.

    IMPORTANT: The date parameters filter by **Gazzetta Ufficiale publication
    date** (dataGU), NOT by the law's emanation date. A law enacted on
    Dec 30, 2025 but published on Jan 2, 2026 will be returned when filtering
    for January 2026.

    Args:
        data_inizio: Start of GU publication date range (YYYY-MM-DD).
        data_fine:   End of GU publication date range (YYYY-MM-DD).
        pagina:      1-based page number.
        per_pagina:  Results per page (max 100).

    Returns:
        Raw API response dict (contains 'listaAtti').
    """
    payload = {
        "dataInizioPubProvvedimento": data_inizio,
        "dataFinePubProvvedimento": data_fine,
        "paginazione": {
            "paginaCorrente": str(pagina),
            "numeroElementiPerPagina": str(per_pagina),
        },
    }
    resp = requests.post(
        f"{BASE_URL}/ricerca/avanzata",
        json=payload,
        headers=HEADERS,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_atti(anno: int, mese: int, per_pagina: int = 100) -> tuple[list[Legge], list[dict]]:
    """Fetch all laws published in the given month and return as Legge objects.

    Paginates through all result pages automatically.

    Args:
        anno:       Year (e.g. 2026).
        mese:       Month (1–12).
        per_pagina: Results per page — lower values help debug rate limits.

    Returns:
        tuple of:
          - list[Legge]: One Legge per law, populated with core API fields.
          - list[dict]:  Raw API dicts, for inspection / field discovery.
    """
    _, last_day = calendar.monthrange(anno, mese)
    data_inizio = f"{anno}-{mese:02d}-01"
    data_fine = f"{anno}-{mese:02d}-{last_day:02d}"

    print(f"  GU publication date range: {data_inizio} → {data_fine}")

    leggi: list[Legge] = []
    raw_atti: list[dict] = []
    pagina = 1

    while True:
        print(f"  Fetching page {pagina}...", end=" ", flush=True)
        result = _ricerca_avanzata_page(data_inizio, data_fine, pagina, per_pagina)
        batch = result.get("listaAtti", [])

        if not batch:
            print("no more results.")
            break

        raw_atti.extend(batch)
        leggi.extend(Legge.from_api(raw) for raw in batch)
        print(f"{len(batch)} results (total so far: {len(leggi)})")
        pagina += 1

    return leggi, raw_atti


# ── CLI / test entrypoint ─────────────────────────────────────────────────────

def main() -> list[Legge]:
    """CLI entrypoint for testing. Runs the search and prints a summary table.

    Returns:
        list[Legge]: All laws found for the given year/month.
    """
    parser = argparse.ArgumentParser(
        description="Fetch Italian laws from the Normattiva API for a given month.",
    )
    parser.add_argument("anno", type=int, help="Year (e.g. 2026)")
    parser.add_argument("mese", type=int, help="Month (1–12)")
    parser.add_argument("--per-pagina", type=int, default=100,
                        help="Results per page (default: 100)")
    parser.add_argument("--dump", action="store_true",
                        help="Save raw API response + serialised Legge objects to a JSON file")
    args = parser.parse_args()

    if not (1 <= args.mese <= 12):
        parser.error(f"mese must be 1–12, got: {args.mese}")

    print("=" * 60)
    print(f"Normattiva search: {args.anno}/{args.mese:02d}")
    print("=" * 60)

    leggi, raw_atti = fetch_atti(args.anno, args.mese, per_pagina=args.per_pagina)

    print(f"\n  Total found: {len(leggi)}\n")

    # Summary table
    col_w = (14, 12, 10, 50)
    header = (
        f"  {'codice':<{col_w[0]}} "
        f"{'data_gu':<{col_w[1]}} "
        f"{'numero':<{col_w[2]}} "
        f"{'descrizione':<{col_w[3]}}"
    )
    print(header)
    print("  " + "-" * (sum(col_w) + 3))
    for legge in leggi:
        n = legge.normattiva
        suppl = f" [{n.tipo_supplemento_it}]" if n.tipo_supplemento_it else ""
        print(
            f"  {n.codice_redazionale:<{col_w[0]}} "
            f"{n.data_gu:<{col_w[1]}} "
            f"{n.numero_provvedimento:<{col_w[2]}} "
            f"{(n.descrizione_atto + suppl):<{col_w[3]}}"
        )

    # Dump to JSON if requested
    if args.dump:
        dump_path = Path(__file__).parent / f"dump_{args.anno}_{args.mese:02d}.json"
        dump_data = {
            "meta": {
                "anno": args.anno,
                "mese": args.mese,
                "total": len(leggi),
            },
            # Raw API keys exactly as returned — shows every field available
            "raw_api": raw_atti,
            # Serialised Legge objects — shows what we're actually capturing
            "leggi": [dataclasses.asdict(l) for l in leggi],
        }
        with dump_path.open("w", encoding="utf-8") as f:
            json.dump(dump_data, f, indent=2, ensure_ascii=False)
        print(f"\n  💾 Dump saved → {dump_path}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)

    return leggi


if __name__ == "__main__":
    main()
