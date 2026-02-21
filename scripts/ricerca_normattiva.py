#!/usr/bin/env python3
"""
ricerca_normattiva.py — Search Italian norms by year/month via ricerca/avanzata.

Usage:
  python ricerca_normattiva.py 2026 1

API Behavior:
  - The date parameters filter by **Gazzetta Ufficiale publication date** (dataGU)
  - Laws are stored by **emanation date** (data-emanazione) in content/leggi/
"""

import argparse
import calendar
import subprocess
import yaml
import requests
from pathlib import Path
from datetime import datetime

# Import from library modules
from lib.normattiva_api import BASE_URL, HEADERS
from lib.persistence import save_json, save_csv
from lib.state import save_failures_and_warnings

# Constants
OUTPUT_DIR = Path(__file__).parent.parent / "data"
VAULT_DIR = Path(__file__).parent.parent / "content" / "leggi"
MANUAL_OVERRIDES_FILE = Path(__file__).parent / "normattiva_overrides.yaml"

# Approfondimenti columns to extract
APPROFONDIMENTO_COLUMNS = [
    "camera-url", "camera-numero-atto", "camera-tipo-iniziativa",
    "camera-data-presentazione", "camera-firmatari", "camera-relatori",
    "camera-relazioni", "camera-votazione-finale", "camera-dossier",
    "senato-url", "senato-did", "senato-legislatura", "senato-numero-fase",
    "senato-titolo", "senato-titolo-breve", "senato-natura",
    "senato-iniziativa", "senato-data-presentazione", "senato-teseo",
    "senato-votazioni-url", "senato-votazione-finale", "senato-documenti"
]


def load_manual_overrides():
    """Load manual overrides from YAML file."""
    if not MANUAL_OVERRIDES_FILE.exists():
        return {}
    try:
        with MANUAL_OVERRIDES_FILE.open('r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"  ⚠ Warning: Could not load manual overrides: {str(e)[:100]}")
        return {}


def apply_manual_overrides(atto: dict, overrides: dict) -> dict:
    """Apply manual overrides to an atto."""
    codice = atto.get("codiceRedazionale", "")
    if not codice or codice not in overrides:
        return atto

    atto_overrides = overrides[codice]
    print(f"    Applying {len(atto_overrides)} manual override(s) for {codice}")

    for field, value in atto_overrides.items():
        internal_field = field.replace("-", "_")
        atto[internal_field] = value
        print(f"      • {field}: {str(value)[:60]}{'...' if len(str(value)) > 60 else ''}")

    return atto


def ricerca_avanzata(data_inizio: str, data_fine: str, pagina: int = 1, per_pagina: int = 100) -> dict:
    """Call the Normattiva ricerca/avanzata API for a date range."""
    url = f"{BASE_URL}/ricerca/avanzata"
    payload = {
        "dataInizioPubProvvedimento": data_inizio,
        "dataFinePubProvvedimento": data_fine,
        "paginazione": {
            "paginaCorrente": pagina,
            "numeroElementiPerPagina": per_pagina
        }
    }

    session = requests.Session()
    resp = session.post(url, json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def extract_links(html):
    """Extract links from HTML content."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        links.append(f"{text} ({href})")
    return links


def extract_text_content(html):
    """Extract text content from HTML."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text(separator="\n", strip=True)


def fetch_approfondimenti(session, uri):
    """Fetch approfondimenti (additional context) for a law."""
    if not uri:
        return {}

    from bs4 import BeautifulSoup

    url = f"https://www.normattiva.it{uri}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        approfondimenti = {}
        app_div = soup.find("div", id="approfondimenti")
        if not app_div:
            return {}

        for panel in app_div.find_all("div", class_="card"):
            header = panel.find("div", class_="card-header")
            if not header:
                continue

            title = header.get_text(strip=True).lower()
            body = panel.find("div", class_="card-body")
            if not body:
                continue

            content_html = str(body)

            # Check if content is URL list or text
            links = extract_links(content_html)
            is_url_list = all(line.strip().startswith("http") or not line.strip() for line in content_html.split("\n"))

            if "lavori preparatori" in title:
                if links:
                    approfondimenti["lavori_preparatori"] = links[0].split(" (")[1].rstrip(")")
            elif "note" in title:
                if is_url_list:
                    approfondimenti["note"] = links
                else:
                    approfondimenti["note"] = extract_text_content(content_html)
            elif "avvisi" in title:
                approfondimenti["avvisi"] = extract_text_content(content_html)
            elif "relazioni" in title:
                if is_url_list:
                    approfondimenti["relazioni"] = links
                else:
                    approfondimenti["relazioni"] = extract_text_content(content_html)

        return approfondimenti

    except Exception:
        return {}


def check_norm_exists(atto: dict, vault_dir: Path) -> bool:
    """Check if a norm already exists in the vault."""
    from lib.normattiva_api import fetch_normattiva_permalink

    try:
        session = requests.Session()
        permalink_data = fetch_normattiva_permalink(
            session,
            atto.get("dataGU", ""),
            atto.get("codiceRedazionale", "")
        )

        data_emanazione = permalink_data.get("data_emanazione", "")
        numero = permalink_data.get("numero", "")

        if not data_emanazione or not numero:
            return False

        year, month, day = data_emanazione.split("-")
        norm_dir = vault_dir / year / month / day / f"n. {numero}"
        return norm_dir.exists()

    except Exception:
        return False


def check_pr_branch_exists(codice: str) -> bool:
    """Check if a PR branch already exists for this codice."""
    try:
        result = subprocess.run(
            ["git", "branch", "-r"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return f"origin/legge/{codice}" in result.stdout
    except (subprocess.SubprocessError, subprocess.TimeoutExpired, FileNotFoundError):
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Search norms on Normattiva by year and month.",
        epilog="Output is saved to data/ and content/leggi/",
    )
    parser.add_argument("anno", type=int, help="Year (e.g. 2026)")
    parser.add_argument("mese", type=int, help="Month (1-12)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip git operations (branch checks)")
    args = parser.parse_args()

    # Validate
    if not (1 <= args.mese <= 12):
        parser.error(f"mese must be 1-12, got: {args.mese}")

    # Calculate date range
    _, last_day = calendar.monthrange(args.anno, args.mese)
    data_inizio = f"{args.anno}-{args.mese:02d}-01"
    data_fine = f"{args.anno}-{args.mese:02d}-{last_day:02d}"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_range = f"{args.anno}_{args.mese:02d}"

    print("=" * 60)
    print(f"Ricerca normattiva: {args.anno}/{args.mese:02d}")
    print(f"GU publication date range: {data_inizio} to {data_fine}")
    print("=" * 60 + "\n")

    # Paginate all results
    atti = []
    pagina = 1
    while True:
        print(f"  Pagina {pagina}...")
        results = ricerca_avanzata(data_inizio, data_fine, pagina=pagina)
        batch = results.get("listaAtti", [])
        if not batch:
            break
        atti.extend(batch)
        print(f"    {len(batch)} risultati")
        pagina += 1

    print(f"\n  Totale norme fetched: {len(atti)}")

    # Filter existing norms
    print("\n[Checking for existing norms]")
    original_count = len(atti)
    atti = [atto for atto in atti if not check_norm_exists(atto, VAULT_DIR)]
    filtered_count = original_count - len(atti)

    if filtered_count > 0:
        print(f"  Skipped {filtered_count} existing norms")
    print(f"  Processing {len(atti)} new norms")

    # Filter PR branches
    if not args.dry_run:
        print("\n[Checking for existing PR branches]")
        try:
            print("  Fetching remote branches...")
            subprocess.run(["git", "fetch", "origin"], capture_output=True, timeout=30, check=True)
        except Exception as e:
            print(f"  ⚠ Warning: Could not fetch remote: {str(e)[:100]}")

        pre_pr_count = len(atti)
        atti = [atto for atto in atti if not check_pr_branch_exists(atto.get("codiceRedazionale", ""))]
        pr_filtered = pre_pr_count - len(atti)

        if pr_filtered > 0:
            print(f"  Skipped {pr_filtered} norms with existing PR branches")
        print(f"  Processing {len(atti)} remaining norms\n")
    else:
        print("\n[Dry run: Skipping PR branch checks]\n")

    if len(atti) == 0:
        print("  No new norms to process. Exiting.")
        print("\n" + "=" * 60)
        print("Done!")
        print("=" * 60)
        return

    # Fetch normattiva permalinks
    print("[Fetching normattiva permalinks]")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    from lib.normattiva_api import fetch_normattiva_permalink
    for i, atto in enumerate(atti):
        data_gu = atto.get("dataGU", "")
        codice = atto.get("codiceRedazionale", "")
        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        try:
            permalink_data = fetch_normattiva_permalink(session, data_gu, codice)
            atto.update(permalink_data)
            print("✓")
        except Exception as e:
            print(f"✗ {str(e)[:50]}")

    # Fetch approfondimenti
    print("\n[Fetching approfondimenti]")
    for i, atto in enumerate(atti):
        uri = atto.get("normattiva_uri", atto.get("urn", ""))
        codice = atto.get("codiceRedazionale", "")
        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        try:
            approfondimenti = fetch_approfondimenti(session, uri)
            if approfondimenti:
                atto.update(approfondimenti)
                populated = [k for k in APPROFONDIMENTO_COLUMNS if k in approfondimenti]
                print(f"{len(populated)} fields")
            else:
                print("none")
        except Exception as e:
            print(f"error ({str(e)[:30]})")

    # Apply manual overrides
    print("\n[Applying manual overrides]")
    manual_overrides = load_manual_overrides()
    if manual_overrides:
        print(f"  Loaded {len(manual_overrides)} override(s)")
        for i, atto in enumerate(atti):
            atti[i] = apply_manual_overrides(atto, manual_overrides)
    else:
        print(f"  No manual overrides found")

    # Fetch Camera.it and Senato.it metadata
    print("\n[Fetching Camera.it metadata]")
    from lib.camera import fetch_camera_metadata
    for i, atto in enumerate(atti):
        lavori_url = atto.get("lavori_preparatori")
        if not lavori_url:
            continue

        codice = atto.get("codiceRedazionale", "")
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

    print("\n[Fetching Senato.it metadata]")
    from lib.senato import fetch_senato_metadata
    for i, atto in enumerate(atti):
        senato_url = atto.get("senato-url")
        if not senato_url:
            continue

        codice = atto.get("codiceRedazionale", "")
        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        try:
            senato_meta = fetch_senato_metadata(session, senato_url)
            atto.update(senato_meta)
            print(f"DDL {senato_meta.get('senato-numero-fase', '?')}")
            if senato_meta.get("senato-votazione-finale-warning"):
                print(f"    ⚠ {senato_meta['senato-votazione-finale-warning']}")
        except Exception as e:
            print(f"error ({str(e)[:50]})")

    # Fetch full text HTML
    print("\n[Fetching full text HTML]")
    from lib.normattiva_api import fetch_full_text_via_export
    for i, atto in enumerate(atti):
        data_gu = atto.get("dataGU", "")
        codice = atto.get("codiceRedazionale", "")

        if not data_gu or not codice:
            print(f"  [{i+1}/{len(atti)}] {codice}... skipping")
            atto["full_text_html"] = ""
            continue

        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        try:
            full_html = fetch_full_text_via_export(session, data_gu, codice)
            atto["full_text_html"] = full_html
            if full_html:
                print(f"{len(full_html):,} chars")
            else:
                print("no content")
        except Exception as e:
            print(f"error ({str(e)[:50]})")
            atto["full_text_html"] = ""

    # Auto-fetch missing referenced laws
    from lib.law_references import detect_and_fetch_missing_references
    atti = detect_and_fetch_missing_references(atti, session, VAULT_DIR)

    # Save markdown files
    print("\n[Saving markdown files]")
    from lib.persistence import save_markdown
    processed_laws = save_markdown(atti, VAULT_DIR)

    # Save outputs
    print("\n[Saving outputs]")
    save_json({"listaAtti": atti}, OUTPUT_DIR / f"ricerca_{safe_range}_raw_{timestamp}.json")
    save_csv(atti, OUTPUT_DIR / f"ricerca_{safe_range}_{timestamp}.csv")

    if processed_laws:
        save_json({"new_laws": processed_laws}, OUTPUT_DIR / "new_laws.json")
        print(f"  Processed laws: {len(processed_laws)}")

    # Save failures and warnings
    save_failures_and_warnings(OUTPUT_DIR)

    # Collect interventi failures
    print("\n[Checking interventi results]")
    interventi_failures = [
        {"codice": atto.get("codiceRedazionale"), "error": atto.get("_interventi_error")}
        for atto in atti
        if "_interventi_error" in atto
    ]
    if interventi_failures:
        save_json({
            "failures": interventi_failures,
            "count": len(interventi_failures),
            "timestamp": datetime.now().isoformat()
        }, OUTPUT_DIR / "interventi_failures.json")
        print(f"  ⚠ Interventi failures: {len(interventi_failures)}")

    # Preview
    if atti:
        print(f"\n  Prime 10 norme:")
        print(f"  {'codice':<14} {'dataGU':<12} {'descrizione':<45}")
        print(f"  {'-'*14} {'-'*12} {'-'*45}")
        for atto in atti[:10]:
            print(f"  {atto.get('codiceRedazionale', ''):<14} "
                  f"{atto.get('dataGU', ''):<12} "
                  f"{atto.get('descrizioneAtto', ''):<45}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
