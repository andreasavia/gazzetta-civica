"""
markdown_writer.py — Write enriched Legge objects to vault Markdown files.

Responsibility: translate a fully-enriched Legge object into the canonical
YAML frontmatter + HTML body Markdown file format used by the Gazzetta Civica
vault.

File layout (organised by **emanation date**, not GU date):
    content/leggi/{year}/{MM}/{DD}/n. {numero}/{descrizione}.md

Key design decisions vs. old save_markdown():
  - All fields are read from the typed Legge / NormattivaData / CameraData /
    SenatoData objects, never from a raw dict (no .get() with string keys).
  - Approfondimenti are list[str], so the old string-split / URL-detection
    logic is replaced by a simple list serialiser.
  - full_text_html contains only the #printThis content (extracted at fetch
    time), so no BeautifulSoup re-parse is needed here.
  - extract_law_references / find_referenced_law are ported inline.
  - MISSING_REFERENCES tracking returns a list instead of using a module-level
    global, so callers can decide what to do with it.

Usage:
    from markdown_writer import write_legge, write_all
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from models import Legge
from utils import _MONTHS_IT


def find_referenced_law(ref: dict, vault_dir: Path) -> str | None:
    """Find the vault Markdown file for a referenced law.

    Returns:
        Relative path string from project root if found, None otherwise.
    """
    try:
        dt = datetime.strptime(ref["date"], "%Y-%m-%d")
    except ValueError:
        return None

    search_path = (
        vault_dir
        / str(dt.year)
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
        / f"n. {ref['number']}"
    )
    if search_path.exists():
        md_files = list(search_path.glob("*.md"))
        if md_files:
            return str(md_files[0].relative_to(vault_dir.parent.parent))
    return None


# ── Frontmatter helpers ───────────────────────────────────────────────────────

def _yaml_str(value: str) -> str:
    """Wrap a string value in double-quotes, escaping any internal quotes."""
    return '"' + value.replace('"', '\\"') + '"'


def _append_url_list(lines: list[str], key: str, urls: list[str]) -> None:
    """Append a YAML list of URLs, skipping empty values."""
    non_empty = [u for u in urls if u]
    if not non_empty:
        return
    lines.append(f"{key}:")
    for url in non_empty:
        lines.append(f"  - {url}")


def _append_quoted_list(lines: list[str], key: str, items: list[str]) -> None:
    """Append a YAML list of quoted strings."""
    if not items:
        return
    lines.append(f"{key}:")
    for item in items:
        lines.append(f"  - {_yaml_str(item)}")


# ── Main writer ───────────────────────────────────────────────────────────────

def write_legge(
    legge: Legge,
    vault_dir: Path,
) -> tuple[Path, list[dict]]:
    """Write a single Legge to its vault Markdown file.

    Args:
        legge:     Fully enriched Legge object.
        vault_dir: Root vault directory (e.g. content/leggi/).

    Returns:
        Tuple of:
          - filepath: Path to the written .md file.
          - missing_refs: list of dicts describing law references that could
            not be resolved to an existing vault file (for PR warnings).
    """
    n = legge.normattiva
    c = legge.camera
    s = legge.senato
    missing_refs: list[dict] = []

    # ── Resolve file path (by emanation date) ─────────────────────────────────
    try:
        dt = datetime.strptime(n.data_emanazione, "%Y-%m-%d")
        year, month, day = str(dt.year), f"{dt.month:02d}", f"{dt.day:02d}"
    except ValueError:
        year, month, day = "unknown", "00", "00"
        dt = None

    norm_dir = vault_dir / year / month / day / f"n. {n.numero_provvedimento}"
    norm_dir.mkdir(parents=True, exist_ok=True)

    safe_name = re.sub(r'[<>:"/\\|?*]', "_", n.descrizione_atto)
    filepath = norm_dir / f"{safe_name}.md"

    # ── Build frontmatter lines ───────────────────────────────────────────────
    lines: list[str] = ["---"]

    # Core identity
    lines.append(f"codice-redazionale: {n.codice_redazionale}")
    lines.append(f"tipo: {n.denominazione_atto}")
    lines.append(f"numero-atto: {n.numero_provvedimento}")
    lines.append(f"data-emanazione: {n.data_emanazione}")
    lines.append(f"data-gu: {n.data_gu}")
    lines.append(f"numero-gu: {n.numero_gu}")

    if n.data_vigenza:
        lines.append(f"data-vigenza: {n.data_vigenza}")

    # Normattiva URN + direct link
    if n.uri:
        lines.append(f"normattiva-urn: {n.uri}")
    if n.data_gu and n.codice_redazionale:
        link = (
            f"https://www.normattiva.it/atto/caricaDettaglioAtto"
            f"?atto.dataPubblicazioneGazzetta={n.data_gu}"
            f"&atto.codiceRedazionale={n.codice_redazionale}"
        )
        if n.data_vigenza:
            try:
                vig_fmt = datetime.strptime(n.data_vigenza, "%Y-%m-%d").strftime("%d/%m/%Y")
                link += f"&tipoDettaglio=singolavigenza&dataVigenza={vig_fmt}"
            except ValueError:
                pass
        lines.append(f"normattiva-link: {link}")

    if n.gu_link:
        lines.append(f"gu-link: {n.gu_link}")

    # Titles
    titolo_clean = n.titolo_atto.strip().strip("[]").strip()
    lines.append(f"titolo-atto: {_yaml_str(titolo_clean)}")
    lines.append(f"descrizione-atto: {_yaml_str(n.descrizione_atto)}")

    # Alternative title: "Legge n. 9/26 del 26 gennaio 2026"
    if dt:
        year_short = str(dt.year)[-2:]
        date_it = f"{dt.day} {_MONTHS_IT[dt.month]} {dt.year}"
        tipo_simple = (
            n.denominazione_atto.title()
            .replace("Del ", "del ")
            .replace("Dei ", "dei ")
        )
        titolo_alt = f"{tipo_simple} n. {n.numero_provvedimento}/{year_short} del {date_it}"
        lines.append(f"titolo-alternativo: {_yaml_str(titolo_alt)}")

    # ── Approfondimenti (now proper list[str]) ────────────────────────────────
    _APPRO_FIELDS = [
        ("atti-aggiornati",          n.atti_aggiornati),
        ("atti-correlati",           n.atti_correlati),
        ("aggiornamenti-atto",       n.aggiornamenti_atto),
        ("note-atto",                n.note_atto),
        ("relazioni",                n.relazioni),
        ("aggiornamenti-titolo",     n.aggiornamenti_titolo),
        ("aggiornamenti-struttura",  n.aggiornamenti_struttura),
        ("atti-parlamentari",        n.atti_parlamentari),
        ("atti-attuativi",           n.atti_attuativi),
    ]
    for key, url_list in _APPRO_FIELDS:
        _append_url_list(lines, key, url_list)

    # lavori-preparatori: URL list + optional text block
    if n.lavori_preparatori:
        _append_url_list(lines, "lavori-preparatori", n.lavori_preparatori)
    if n.lavori_preparatori_testo:
        lines.append("lavori-preparatori-testo: |")
        for text_line in n.lavori_preparatori_testo.split("\n"):
            lines.append(f"  {text_line}")

    # ── Camera metadata ───────────────────────────────────────────────────────
    if c.legislatura:
        lines.append(f"camera-legislatura: {c.legislatura}")
    if c.atto:
        lines.append(f"camera-atto: {c.atto}")
    if c.atto_iri:
        lines.append(f"camera-atto-iri: {c.atto_iri}")
    if c.natura:
        lines.append(f"camera-natura: {_yaml_str(c.natura)}")
    if c.iniziativa:
        lines.append(f"camera-iniziativa: {_yaml_str(c.iniziativa)}")
    if c.data_presentazione:
        lines.append(f"camera-data-presentazione: {_yaml_str(c.data_presentazione)}")
    _append_url_list(lines, "camera-relazioni", c.relazioni)
    if c.firmatari:
        lines.append("camera-firmatari:")
        for dep in c.firmatari:
            name = dep.get("name", "")
            role = dep.get("role") or dep.get("group") or ""
            entry = f"{name} - {role}" if role else name
            lines.append(f"  - {_yaml_str(entry)}")
    _append_quoted_list(lines, "camera-relatori", c.relatori)
    if c.votazione_finale:
        lines.append(f"camera-votazione-finale: {c.votazione_finale}")
    _append_url_list(lines, "camera-dossier", c.dossier)

    # ── Senato metadata ───────────────────────────────────────────────────────
    if s.did:
        lines.append(f"senato-did: {s.did}")
    if s.legislatura:
        lines.append(f"senato-legislatura: {s.legislatura}")
    if s.numero_fase:
        lines.append(f"senato-numero-fase: {s.numero_fase}")
    if s.url:
        lines.append(f"senato-url: {s.url}")
    if s.titolo:
        lines.append(f"senato-titolo: {_yaml_str(s.titolo)}")
    if s.titolo_breve:
        lines.append(f"senato-titolo-breve: {_yaml_str(s.titolo_breve)}")
    if s.natura:
        lines.append(f"senato-natura: {_yaml_str(s.natura)}")
    if s.iniziativa:
        lines.append(f"senato-iniziativa: {_yaml_str(s.iniziativa)}")
    if s.data_presentazione:
        lines.append(f"senato-data-presentazione: {_yaml_str(s.data_presentazione)}")
    _append_quoted_list(lines, "senato-teseo", s.teseo)
    if s.votazioni_url:
        lines.append(f"senato-votazioni-url: {s.votazioni_url}")
    if s.votazione_finale:
        lines.append(f"senato-votazione-finale: {s.votazione_finale}")
    _append_url_list(lines, "senato-documenti", s.documenti)

    # ── Law cross-references (reads from model, no re-parsing needed) ───────────
    if n.riferimenti:
        refs_by_type: dict[str, list[str]] = {}
        for ref in n.riferimenti:
            ref_type = ref["type"]
            fp = find_referenced_law(ref, vault_dir)
            if fp:
                refs_by_type.setdefault(ref_type, []).append(fp)
            else:
                missing_refs.append({
                    "codice":          n.codice_redazionale,
                    "title":           n.titolo_atto,
                    "reference_type":  ref_type,
                    "reference_date":  ref["date"],
                    "reference_number": ref["number"],
                    "reference_act_type": ref["act_type"],
                })
        for ref_type, paths in refs_by_type.items():
            lines.append(f"{ref_type}:")
            for path in paths:
                lines.append(f"  - {_yaml_str(path)}")

    lines.append("---")

    # ── Write file ────────────────────────────────────────────────────────────
    with filepath.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        # Body: full_text_html already contains only #printThis contents
        # (extracted at fetch time by normattiva_enrich._fetch_full_text)
        if n.full_text_html:
            f.write("\n\n")
            f.write(n.full_text_html)

    return filepath, missing_refs


def write_all(
    leggi: list[Legge],
    vault_dir: Path,
) -> tuple[list[dict], list[dict]]:
    """Write all Legge objects to vault Markdown files.

    Args:
        leggi:     List of fully enriched Legge objects.
        vault_dir: Root vault directory (e.g. content/leggi/).

    Returns:
        Tuple of:
          - law_metadata: list of dicts with per-law summary info.
          - all_missing_refs: aggregated list of unresolved cross-references.
    """
    vault_dir.mkdir(parents=True, exist_ok=True)
    law_metadata: list[dict] = []
    all_missing_refs: list[dict] = []

    for legge in leggi:
        n = legge.normattiva
        filepath, missing_refs = write_legge(legge, vault_dir)
        all_missing_refs.extend(missing_refs)

        # Compute relative path from project root
        try:
            rel_path = filepath.relative_to(vault_dir.parent.parent)
        except ValueError:
            rel_path = filepath

        law_metadata.append({
            "codice":      n.codice_redazionale,
            "descrizione": n.descrizione_atto,
            "tipo":        n.denominazione_atto,
            "numero":      n.numero_provvedimento,
            "data_emanazione": n.data_emanazione,
            "filepath":    str(rel_path),
            "directory":   str(rel_path.parent),
        })

    print(f"  Vault: {vault_dir}/ ({len(leggi)} norms)")
    return law_metadata, all_missing_refs
