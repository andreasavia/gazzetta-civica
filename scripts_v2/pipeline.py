"""
pipeline.py — Orchestrates the full Gazzetta Civica data pipeline.

Flow:
    1. Parse args (anno, mese, --dry-run)
    2. Search Normattiva API → list[Legge]
    3. Filter: skip if vault folder already exists
    4. Filter: skip if PR branch already exists on origin
    5. Apply manual overrides from normattiva_overrides.yaml
    6. Normattiva enrich (permalink + approfondimenti + full text)
    7. Camera enrich (RDF + HTML + interventi)
    8. Senato enrich (HTML scraping)
    9. Write markdown to vault
   10. Write data/new_laws.json (consumed by GH Actions PR-creation workflow)
   11. Print summary

Usage:
    python pipeline.py 2026 1
    python pipeline.py 2026 1 --dry-run   # skips PR creation (no git ops)
    python pipeline.py 2026 1 --limit 3   # process at most 3 new laws (for testing)
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import subprocess
from datetime import datetime
from pathlib import Path

import yaml  # PyYAML — already in requirements.txt

from models import Legge
from normattiva_search import fetch_atti
from normattiva_enrich import enrich_all as normattiva_enrich_all
from camera_enrich import enrich_all as camera_enrich_all
from senato_enrich import enrich_all as senato_enrich_all
from markdown_writer import write_all

# ── Paths ─────────────────────────────────────────────────────────────────────

_SCRIPT_DIR   = Path(__file__).parent
_REPO_ROOT    = _SCRIPT_DIR.parent
_VAULT_DIR    = _REPO_ROOT / "content" / "leggi"
_DATA_DIR     = _REPO_ROOT / "data"
_OVERRIDES    = _SCRIPT_DIR / "normattiva_overrides.yaml"
_NEW_LAWS_OUT = _DATA_DIR / "new_laws.json"


# ── Filter helpers ────────────────────────────────────────────────────────────

def check_norm_exists(legge: Legge) -> bool:
    """Return True if this law's vault directory already has a .md file.

    Organised by emanation date: vault/YYYY/MM/DD/n.{numero}/*.md
    """
    n = legge.normattiva
    try:
        dt = datetime.strptime(n.data_emanazione, "%Y-%m-%d")
    except ValueError:
        return False
    norm_dir = (
        _VAULT_DIR
        / str(dt.year)
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
        / f"n. {n.numero_provvedimento}"
    )
    return norm_dir.exists() and any(norm_dir.glob("*.md"))


def _git_fetch() -> None:
    """Fetch remote branches so branch-existence checks are up to date."""
    try:
        subprocess.run(
            ["git", "fetch", "origin", "--prune"],
            cwd=_REPO_ROOT,
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        print(f"  ⚠ git fetch failed (branch checks may be stale): {exc}")


def check_pr_branch_exists(codice: str) -> bool:
    """Return True if origin/legge/{codice} already exists on the remote."""
    try:
        result = subprocess.run(
            ["git", "branch", "-r"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        return f"origin/legge/{codice}" in result.stdout
    except subprocess.CalledProcessError:
        return False


# ── Override applicator ───────────────────────────────────────────────────────

def _apply_overrides(legge: Legge, overrides: dict) -> None:
    """Apply manual field overrides from normattiva_overrides.yaml.

    YAML keys use dash-separated names. We try to map them to model fields:
      - Bare keys (e.g. "lavori-preparatori")  → NormattivaData attr
      - Keys starting with "camera-"            → CameraData attr
      - Keys starting with "senato-"            → SenatoData attr

    The YAML may contain string "lavori-preparatori" as a multi-line block
    (for text fallback) or as a list (for URL lists). We handle both.
    """
    n, c, s = legge.normattiva, legge.camera, legge.senato

    for yaml_key, value in overrides.items():
        if yaml_key.startswith("camera-"):
            field_name = yaml_key[len("camera-"):].replace("-", "_")
            if hasattr(c, field_name):
                setattr(c, field_name, value)

        elif yaml_key.startswith("senato-"):
            field_name = yaml_key[len("senato-"):].replace("-", "_")
            if hasattr(s, field_name):
                setattr(s, field_name, value)

        else:
            field_name = yaml_key.replace("-", "_")
            # Special case: if the normattiva field is list[str] but the YAML
            # value is a string block, split by newlines and filter empty lines.
            current = getattr(n, field_name, _SENTINEL)
            if current is _SENTINEL:
                continue
            if isinstance(current, list) and isinstance(value, str):
                value = [l.strip() for l in value.splitlines() if l.strip()]
            if hasattr(n, field_name):
                setattr(n, field_name, value)

_SENTINEL = object()


def load_overrides() -> dict:
    """Load normattiva_overrides.yaml. Returns empty dict if file not found."""
    if not _OVERRIDES.exists():
        return {}
    with _OVERRIDES.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ── Output writer ─────────────────────────────────────────────────────────────

def _build_law_entry(
    legge: Legge,
    filepath: Path,
    missing_refs: list[dict],
) -> dict:
    """Build a single entry for new_laws.json."""
    n = legge.normattiva
    try:
        dt = datetime.strptime(n.data_emanazione, "%Y-%m-%d")
        year_short = str(dt.year)[-2:]
        months_it = [
            "", "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
            "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre",
        ]
        date_it = f"{dt.day} {months_it[dt.month]} {dt.year}"
        tipo_simple = (
            n.denominazione_atto.title()
            .replace("Del ", "del ")
            .replace("Dei ", "dei ")
        )
        titolo_alt = f"{tipo_simple} n. {n.numero_provvedimento}/{year_short} del {date_it}"
    except ValueError:
        titolo_alt = n.descrizione_atto

    try:
        rel_path = filepath.relative_to(_REPO_ROOT)
    except ValueError:
        rel_path = filepath

    return {
        "codice":            n.codice_redazionale,
        "descrizione":       n.descrizione_atto,
        "tipo":              n.denominazione_atto,
        "numero":            n.numero_provvedimento,
        "data_emanazione":   n.data_emanazione,
        "titolo_alternativo": titolo_alt,
        "filepath":          str(rel_path),
        "directory":         str(rel_path.parent),
        "failures":          legge.failures,
        "missing_refs":      missing_refs,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Gazzetta Civica pipeline: search, enrich, write, report."
    )
    parser.add_argument("anno",  type=int, help="Year (e.g. 2026)")
    parser.add_argument("mese",  type=int, help="Month (1-12)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run everything but skip git operations (no PR creation).",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap the number of new laws to process (useful for testing).",
    )
    args = parser.parse_args()

    print("=" * 60)
    print(f"Pipeline: {args.anno}/{args.mese:02d}{'  [DRY RUN]' if args.dry_run else ''}")
    print("=" * 60)

    # ── 1. Git fetch (so branch checks are current) ───────────────────────────
    if not args.dry_run:
        print("\n[git fetch]")
        _git_fetch()

    # ── 2. Search ─────────────────────────────────────────────────────────────
    print(f"\n[1/7] Normattiva search {args.anno}/{args.mese:02d}...")
    leggi, _ = fetch_atti(args.anno, args.mese)
    print(f"  Found {len(leggi)} laws total")

    # ── 3. Filter: vault exists ───────────────────────────────────────────────
    print("\n[2/7] Filtering: vault exists...")
    new_leggi = [l for l in leggi if not check_norm_exists(l)]
    skipped_vault = len(leggi) - len(new_leggi)
    print(f"  {skipped_vault} already in vault, {len(new_leggi)} new")
    leggi = new_leggi

    # ── 4. Filter: PR branch exists ───────────────────────────────────────────
    print("\n[3/7] Filtering: PR branch exists...")
    before = len(leggi)
    leggi = [l for l in leggi if not check_pr_branch_exists(l.normattiva.codice_redazionale)]
    skipped_pr = before - len(leggi)
    print(f"  {skipped_pr} already have a PR branch, {len(leggi)} remaining")

    if not leggi:
        print("\nNothing new to process. Exiting.")
        return 0

    # Apply the --limit cap if requested
    if args.limit and len(leggi) > args.limit:
        print(f"  Limiting to {args.limit} laws (--limit)")
        leggi = leggi[: args.limit]

    print(f"\nProcessing {len(leggi)} laws:")
    for l in leggi:
        n = l.normattiva
        print(f"  • {n.codice_redazionale}  {n.descrizione_atto}")

    # ── 5. Apply manual overrides ─────────────────────────────────────────────
    print("\n[4/7] Applying manual overrides...")
    all_overrides = load_overrides()
    override_count = 0
    for legge in leggi:
        codice = legge.normattiva.codice_redazionale
        if codice in all_overrides:
            _apply_overrides(legge, all_overrides[codice])
            override_count += 1
            print(f"  Applied overrides for {codice}")
    if override_count == 0:
        print("  No overrides found")

    # ── 6. Normattiva enrich ──────────────────────────────────────────────────
    print(f"\n[5/7] Normattiva enrich ({len(leggi)} laws)...")
    normattiva_enrich_all(leggi)

    # ── 7. Camera enrich ──────────────────────────────────────────────────────
    print(f"\n[6/7] Camera enrich ({len(leggi)} laws)...")
    camera_enrich_all(leggi, _VAULT_DIR)

    # ── 8. Senato enrich ──────────────────────────────────────────────────────
    print(f"\n[7/7] Senato enrich ({len(leggi)} laws)...")
    senato_enrich_all(leggi)

    # ── 9. Write markdown ─────────────────────────────────────────────────────
    print(f"\n[Writing markdown → {_VAULT_DIR}]")
    law_metadata, all_missing_refs = write_all(leggi, _VAULT_DIR)

    # ── 10. Write new_laws.json ───────────────────────────────────────────────
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Build per-law missing_refs lookup (keyed by codice)
    missing_by_codice: dict[str, list[dict]] = {}
    for ref in all_missing_refs:
        missing_by_codice.setdefault(ref["codice"], []).append(ref)

    new_laws: list[dict] = []
    for legge, meta in zip(leggi, law_metadata):
        codice = legge.normattiva.codice_redazionale
        filepath = _REPO_ROOT / meta["filepath"]
        entry = _build_law_entry(
            legge,
            filepath,
            missing_by_codice.get(codice, []),
        )
        new_laws.append(entry)

    with _NEW_LAWS_OUT.open("w", encoding="utf-8") as f:
        json.dump({"new_laws": new_laws}, f, ensure_ascii=False, indent=2)
    print(f"  💾 {_NEW_LAWS_OUT} ({len(new_laws)} entries)")

    # ── 11. Summary ───────────────────────────────────────────────────────────
    total_failures = sum(len(l.failures) for l in leggi)
    total_missing  = len(all_missing_refs)

    print("\n" + "=" * 60)
    print(f"Done!  {len(leggi)} laws processed")
    if total_failures:
        print(f"  ⚠ {total_failures} fetch failures (see new_laws.json)")
    if total_missing:
        print(f"  ⚠ {total_missing} unresolved cross-references")
    if args.dry_run:
        print("  [DRY RUN] PR creation skipped — run the GH Actions workflow to create PRs")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
