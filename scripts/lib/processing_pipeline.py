"""
Law processing pipeline - orchestrates the full fetch sequence

This module provides the main processing pipeline that:
1. Filters out laws that already exist (in vault or have open PRs)
2. Fetches Normattiva permalinks and metadata
3. Fetches Camera.it lavori preparatori
4. Fetches Senato.it lavori preparatori
5. Fetches full text HTML
6. Auto-fetches missing referenced laws
7. Saves markdown files

This pipeline is reusable across all scripts (ricerca_normattiva, process_laws_by_codice, etc.)
"""

import subprocess
from pathlib import Path


def check_norm_exists(atto: dict, session, vault_dir: Path) -> bool:
    """Check if a norm already exists in the vault.

    Args:
        atto: Law dict with dataGU and codiceRedazionale
        session: Requests session
        vault_dir: Base directory for laws (content/leggi/)

    Returns:
        bool: True if the law already exists, False otherwise
    """
    from lib.normattiva_api import fetch_normattiva_permalink

    try:
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
    """Check if a PR branch already exists for this codice.

    Args:
        codice: Codice redazionale (e.g., "26G00036")

    Returns:
        bool: True if a PR branch exists, False otherwise
    """
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


def filter_existing_laws(atti: list, session, vault_dir: Path) -> list:
    """Filter out laws that already exist in the vault or have open PRs.

    Args:
        atti: List of atti to filter
        session: Requests session
        vault_dir: Base directory for laws (content/leggi/)

    Returns:
        list: Filtered atti (only new laws)
    """
    print("\n[Checking for existing laws]")
    new_atti = []

    for i, atto in enumerate(atti):
        codice = atto.get("codiceRedazionale", "")

        if not codice:
            print(f"  [{i+1}/{len(atti)}] Skipping - no codice")
            continue

        print(f"  [{i+1}/{len(atti)}] {codice}...", end=" ", flush=True)

        # Check if PR branch exists
        if check_pr_branch_exists(codice):
            print("⏭ PR branch exists")
            continue

        # Check if law exists in vault
        if check_norm_exists(atto, session, vault_dir):
            print("⏭ Law exists in vault")
            continue

        print("✓ New law")
        new_atti.append(atto)

    if len(new_atti) < len(atti):
        print(f"\n  Filtered: {len(atti)} → {len(new_atti)} new law(s)")

    return new_atti


def process_laws_full_pipeline(atti: list, session, vault_dir: Path, skip_existing_check: bool = False) -> list:
    """Process laws through the complete pipeline.

    Args:
        atti: List of atti with at least codiceRedazionale and dataGU
        session: Requests session for API calls
        vault_dir: Base directory for markdown files (content/leggi/)
        skip_existing_check: If True, skip checking for existing laws (default: False)

    Returns:
        list: Processed laws metadata for PR creation
    """
    from lib.normattiva_api import fetch_normattiva_permalink, fetch_full_text_via_export
    from lib.camera import fetch_camera_metadata
    from lib.senato import fetch_senato_metadata
    from lib.law_references import detect_and_fetch_missing_references
    from lib.persistence import save_markdown

    print(f"\n[Processing {len(atti)} law(s) through full pipeline]")

    # Step 0: Filter out existing laws (unless skip_existing_check is True)
    if not skip_existing_check:
        atti = filter_existing_laws(atti, session, vault_dir)

        if not atti:
            print("\n  No new laws to process")
            return []

    # Step 1: Fetch Normattiva permalinks
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

    # Step 2: Fetch Camera.it metadata (lavori preparatori)
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

    # Step 3: Fetch Senato.it metadata (lavori preparatori)
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

            # Check for voting link warning
            if senato_meta.get("senato-votazione-finale-warning"):
                print(f"    ⚠ {senato_meta.get('senato-votazione-finale-warning')}")
        except Exception as e:
            print(f"error ({str(e)[:50]})")

    # Step 4: Fetch full text HTML via export endpoint
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

    # Step 5: Auto-fetch missing referenced laws
    atti = detect_and_fetch_missing_references(atti, session, vault_dir)

    # Step 6: Save markdown files
    print("\n[Saving markdown files]")
    processed_laws = save_markdown(atti, vault_dir)

    return processed_laws
