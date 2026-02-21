"""
Law reference detection and auto-fetching

This module provides functions for:
- Detecting law references in titles (conversions, modifications)
- Finding referenced laws in the vault
- Auto-fetching missing referenced laws
"""

import re
from pathlib import Path
from datetime import datetime


def parse_italian_date(day: str, month_name: str, year: str) -> str:
    """Convert Italian date to YYYY-MM-DD format.

    Args:
        day: Day of month (1-31)
        month_name: Italian month name (gennaio, febbraio, etc.)
        year: Four-digit year

    Returns:
        str: Date in YYYY-MM-DD format
    """
    months = {
        'gennaio': 1, 'febbraio': 2, 'marzo': 3, 'aprile': 4,
        'maggio': 5, 'giugno': 6, 'luglio': 7, 'agosto': 8,
        'settembre': 9, 'ottobre': 10, 'novembre': 11, 'dicembre': 12
    }
    month_num = months.get(month_name.lower(), 1)
    return f"{year}-{month_num:02d}-{int(day):02d}"


def extract_law_references(title_atto: str) -> list:
    """Extract references to other laws from the title.

    Detects patterns like:
    - "Conversione in legge del decreto-legge DD MONTH YYYY, n. NNN"
    - "Modifiche alla legge DD MONTH YYYY, n. NNN"

    Args:
        title_atto: The title of the law (titoloAtto field)

    Returns:
        list: List of dicts with: type, act_type, date, number
    """
    references = []

    # Pattern 1: Conversione in legge del decreto-legge
    conversion_pattern = r"Conversione\s+in\s+legge.*?del\s+(decreto-legge|decreto\s+legislativo)\s+(\d{1,2})\s+(\w+)\s+(\d{4}),?\s*n\.\s*(\d+)"

    # Pattern 2: Modifiche/Integrazioni alla legge
    modification_pattern = r"(Modifiche|Integrazioni).*?(legge|decreto-legge|decreto\s+legislativo)\s+(\d{1,2})\s+(\w+)\s+(\d{4}),?\s*n\.\s*(\d+)"

    for match in re.finditer(conversion_pattern, title_atto, re.IGNORECASE):
        act_type, day, month_name, year, number = match.groups()
        references.append({
            'type': 'converts',
            'act_type': act_type.lower().replace(' ', '_'),
            'date': parse_italian_date(day, month_name, year),
            'number': number
        })

    for match in re.finditer(modification_pattern, title_atto, re.IGNORECASE):
        mod_type, act_type, day, month_name, year, number = match.groups()
        references.append({
            'type': 'modifies' if mod_type.lower() == 'modifiche' else 'integrates',
            'act_type': act_type.lower().replace(' ', '_'),
            'date': parse_italian_date(day, month_name, year),
            'number': number
        })

    return references


def find_referenced_law(ref: dict, vault_dir: Path) -> str | None:
    """Find the markdown file for a referenced law.

    Args:
        ref: Reference dict with date, number, act_type
        vault_dir: Base directory for laws (content/leggi/)

    Returns:
        str: Relative file path from project root if found, None otherwise
    """
    # Parse date components
    date_obj = datetime.strptime(ref['date'], '%Y-%m-%d')
    year, month, day = date_obj.year, date_obj.month, date_obj.day
    number = ref['number']

    # Construct expected path
    search_path = vault_dir / str(year) / f"{month:02d}" / f"{day:02d}" / f"n. {number}"

    if search_path.exists():
        # Find the markdown file in that directory
        md_files = list(search_path.glob("*.md"))
        if md_files:
            # Return relative path from project root (content/leggi/...)
            relative_path = md_files[0].relative_to(vault_dir.parent.parent)
            return str(relative_path)

    return None


def search_and_fetch_missing_law(ref: dict, session) -> dict | None:
    """Search for a missing law using the ricerca/semplice API and fetch its metadata.

    Args:
        ref: Reference dict with date, number, act_type
        session: Requests session (unused but kept for API compatibility)

    Returns:
        dict: Atto dictionary if found and fetched successfully, None otherwise
    """
    from lib.normattiva_api import search_laws

    # Construct search query from reference
    date_obj = datetime.strptime(ref['date'], '%Y-%m-%d')

    # Convert act_type back to Italian format
    act_type_map = {
        'decreto-legge': 'decreto-legge',
        'decreto_legge': 'decreto-legge',
        'decreto-legislativo': 'decreto legislativo',
        'decreto_legislativo': 'decreto legislativo',
        'legge': 'legge'
    }
    act_type_italian = act_type_map.get(ref['act_type'], ref['act_type'])

    # Format date in Italian
    month_names = ['', 'gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno',
                   'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre']
    month_name = month_names[date_obj.month]

    search_text = f"{act_type_italian} {date_obj.day} {month_name} {date_obj.year} n. {ref['number']}"

    print(f"    Searching for: {search_text}")

    try:
        # Use ricerca_semplice module
        lista_atti = search_laws(search_text, max_results=5)

        if not lista_atti:
            print(f"    ✗ Not found in search results")
            return None

        # Find exact match by number and date
        for atto in lista_atti:
            if (atto.get("numeroProvvedimento") == ref['number'] and
                atto.get("dataGU") == ref['date']):
                print(f"    ✓ Found: {atto.get('codiceRedazionale')}")
                # Convert to our internal format
                return {
                    "codiceRedazionale": atto.get("codiceRedazionale"),
                    "dataGU": atto.get("dataGU"),
                    "descrizioneAtto": atto.get("descrizioneAtto"),
                    "titoloAtto": atto.get("titoloAtto", "").strip("[] "),
                    "numeroProvvedimento": atto.get("numeroProvvedimento"),
                    "denominazioneAtto": atto.get("denominazioneAtto"),
                    "_auto_fetched": True  # Mark as auto-fetched to prevent recursive fetching
                }

        print(f"    ✗ No exact match in {len(lista_atti)} results")
        return None

    except Exception as e:
        print(f"    ✗ Search failed: {str(e)[:50]}")
        return None


def detect_and_fetch_missing_references(atti: list, session, vault_dir: Path) -> list:
    """Detect missing references across all atti and auto-fetch them.

    Args:
        atti: List of atti to check for missing references
        session: Requests session for API calls
        vault_dir: Base directory for laws (content/leggi/)

    Returns:
        list: Extended atti list with auto-fetched laws added
    """
    from lib.normattiva_api import fetch_normattiva_permalink, fetch_full_text_via_export
    from lib.camera import fetch_camera_metadata
    from lib.senato import fetch_senato_metadata

    print("\n[Checking for missing law references]")
    missing_to_fetch = []

    for atto in atti:
        # Skip auto-fetched laws to prevent infinite recursion
        if atto.get("_auto_fetched"):
            continue

        references = extract_law_references(atto.get("titoloAtto", ""))
        if references:
            for ref in references:
                # Check if law exists in vault
                file_path = find_referenced_law(ref, vault_dir)
                if not file_path:
                    # Check if already in our fetch list
                    already_listed = any(
                        m['date'] == ref['date'] and m['number'] == ref['number']
                        for m in missing_to_fetch
                    )
                    if not already_listed:
                        missing_to_fetch.append(ref)

    if not missing_to_fetch:
        print(f"  ✓ All referenced laws found in vault")
        return atti

    print(f"  Found {len(missing_to_fetch)} missing reference(s), attempting to fetch...")

    newly_fetched = []
    for ref in missing_to_fetch:
        print(f"  • {ref['act_type']} {ref['date']} n. {ref['number']}")
        fetched_atto = search_and_fetch_missing_law(ref, session)
        if fetched_atto:
            newly_fetched.append(fetched_atto)

    if not newly_fetched:
        return atti

    print(f"\n  ✓ Successfully fetched {len(newly_fetched)} missing law(s)")
    print(f"  Processing newly fetched laws...")

    # Process newly fetched laws through the same pipeline
    for i, atto in enumerate(newly_fetched):
        codice = atto.get("codiceRedazionale", "")
        data_gu = atto.get("dataGU", "")

        print(f"\n  [{i+1}/{len(newly_fetched)}] {codice}")

        # Fetch permalink
        try:
            permalink_data = fetch_normattiva_permalink(session, data_gu, codice)
            atto.update(permalink_data)
        except Exception as e:
            print(f"    ⚠ Permalink fetch failed: {str(e)[:50]}")

        # Fetch lavori preparatori
        lavori_url = atto.get("lavori_preparatori")
        if lavori_url:
            print(f"    Fetching Camera metadata...")
            try:
                camera_meta = fetch_camera_metadata(session, lavori_url)
                atto.update(camera_meta)
            except Exception as e:
                print(f"    ⚠ Camera fetch failed: {str(e)[:50]}")

            if atto.get("senato-url"):
                print(f"    Fetching Senato metadata...")
                try:
                    senato_meta = fetch_senato_metadata(session, atto.get("senato-url"))
                    atto.update(senato_meta)
                except Exception as e:
                    print(f"    ⚠ Senato fetch failed: {str(e)[:50]}")

        # Fetch full text HTML
        print(f"    Fetching full text HTML...")
        try:
            full_html = fetch_full_text_via_export(session, data_gu, codice)
            atto["full_text_html"] = full_html
            if full_html:
                print(f"    ✓ {len(full_html):,} chars")
        except Exception as e:
            print(f"    ⚠ Full text fetch failed: {str(e)[:50]}")
            atto["full_text_html"] = ""

    # Add newly fetched laws to main list
    atti.extend(newly_fetched)
    print(f"\n  Total laws to process: {len(atti)} (original + auto-fetched)")

    return atti
