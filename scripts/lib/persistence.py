"""
Data persistence - save to JSON, CSV, markdown

This module provides functions for saving law data in various formats:
- JSON for API output
- CSV for spreadsheet analysis
- Markdown for Obsidian vault
"""

import csv
import json
from pathlib import Path
from bs4 import BeautifulSoup


def save_json(data, path: Path) -> None:
    """Save data to JSON file with pretty printing.

    Args:
        data: Data to save (dict or list)
        path: Path to output file
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_csv(atti: list, path: Path) -> None:
    """Save atti to CSV file.

    Args:
        atti: List of atti dictionaries
        path: Path to output CSV file
    """
    if not atti:
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    # Collect all unique keys
    all_keys = set()
    for atto in atti:
        all_keys.update(atto.keys())

    # Sort keys for consistent column order
    sorted_keys = sorted(all_keys)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sorted_keys)
        writer.writeheader()
        writer.writerows(atti)


def save_markdown(atti: list, vault_dir: Path) -> list:
    """Generate markdown files for laws with YAML frontmatter.

    Args:
        atti: List of atti with metadata and full_text_html
        vault_dir: Base directory for markdown files (content/leggi/)

    Returns:
        list: Processed laws metadata for PR creation
    """
    from lib.law_references import extract_law_references, find_referenced_law

    law_metadata = []

    for atto in atti:
        # Extract basic metadata
        codice = atto.get("codiceRedazionale", "")
        descrizione = atto.get("descrizioneAtto", "")
        tipo = atto.get("tipo", atto.get("denominazioneAtto", ""))
        numero_provv = atto.get("numero", atto.get("numeroProvvedimento", ""))

        # Handle dataEmanazione from API (ISO format) or data_emanazione (YYYY-MM-DD)
        data_emanazione = atto.get("data_emanazione", "")
        if not data_emanazione:
            # Try API field dataEmanazione (ISO 8601 format: "2024-12-31T00:00:00Z")
            data_em_iso = atto.get("dataEmanazione", "")
            if data_em_iso:
                # Extract just the date part (YYYY-MM-DD)
                data_emanazione = data_em_iso.split("T")[0]

        if not data_emanazione or not numero_provv:
            print(f"  Skipping {codice} - missing data_emanazione or numero")
            continue

        # Parse date components
        year, month, day = data_emanazione.split("-")

        # Create directory structure
        norm_dir = vault_dir / year / month / day / f"n. {numero_provv}"
        norm_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename
        filename = f"{tipo} {day} {_month_name(int(month))} {year}, n. {numero_provv}.md"
        filepath = norm_dir / filename

        # Build YAML frontmatter
        lines = ["---"]
        lines.append(f"codice-redazionale: {codice}")
        lines.append(f"tipo: {tipo}")
        lines.append(f"numero: \"{numero_provv}\"")
        lines.append(f"data-emanazione: {data_emanazione}")

        if atto.get("dataGU"):
            lines.append(f"data-gu: {atto.get('dataGU')}")
        if atto.get("numeroGU"):
            lines.append(f"numero-gu: \"{atto.get('numeroGU')}\"")

        # Add permalink
        if atto.get("permalink"):
            lines.append(f"permalink: {atto.get('permalink')}")

        # Add titolo
        titolo_atto = atto.get("titoloAtto", "")
        if titolo_atto:
            lines.append(f"titolo-atto: \"{titolo_atto}\"")

        # Add lavori preparatori
        if atto.get("lavori_preparatori"):
            lines.append(f"lavori-preparatori: {atto.get('lavori_preparatori')}")

        # Add Camera metadata
        _add_camera_metadata(atto, lines)

        # Add Senato metadata
        _add_senato_metadata(atto, lines)

        # Extract law references from title (conversions, modifications, etc.)
        references = extract_law_references(atto.get("titoloAtto", ""))
        if references:
            # Group references by type
            refs_by_type = {}
            for ref in references:
                ref_type = ref['type']
                file_path = find_referenced_law(ref, vault_dir)
                if file_path:
                    if ref_type not in refs_by_type:
                        refs_by_type[ref_type] = []
                    refs_by_type[ref_type].append(file_path)
                else:
                    # Track missing reference for PR warning
                    from lib.state import MISSING_REFERENCES
                    MISSING_REFERENCES.append({
                        "codice": codice,
                        "title": atto.get("titoloAtto", ""),
                        "reference_type": ref_type,
                        "reference_date": ref['date'],
                        "reference_number": ref['number'],
                        "reference_act_type": ref['act_type']
                    })

            # Write file paths as list (always as list for consistency)
            for ref_type, file_paths in refs_by_type.items():
                lines.append(f"{ref_type}:")
                for path in file_paths:
                    lines.append(f"  - \"{path}\"")

        lines.append("---")

        # Write frontmatter and body
        with filepath.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines))

            # Add full text HTML to the body
            full_text_html = atto.get("full_text_html", "")
            if full_text_html:
                f.write("\n\n")

                # Parse HTML and extract the printable content div
                soup = BeautifulSoup(full_text_html, 'html.parser')
                print_div = soup.find('div', id='printThis')

                if print_div:
                    f.write(print_div.decode_contents())
                else:
                    f.write(full_text_html)

        # Track processed laws metadata
        relative_path = filepath.relative_to(vault_dir.parent.parent)
        law_metadata.append({
            "codice": codice,
            "descrizione": descrizione,
            "tipo": tipo,
            "numero": numero_provv,
            "data_emanazione": data_emanazione,
            "titolo_alternativo": atto.get("titolo_alternativo", descrizione),
            "filepath": str(relative_path),
            "directory": str(norm_dir.relative_to(vault_dir.parent.parent)),
        })

    print(f"  Vault: {vault_dir}/ ({len(atti)} norms)")
    return law_metadata


def _month_name(month: int) -> str:
    """Convert month number to Italian name."""
    months = {
        1: "gennaio", 2: "febbraio", 3: "marzo", 4: "aprile",
        5: "maggio", 6: "giugno", 7: "luglio", 8: "agosto",
        9: "settembre", 10: "ottobre", 11: "novembre", 12: "dicembre"
    }
    return months.get(month, "")


def _add_camera_metadata(atto: dict, lines: list):
    """Add Camera.it metadata to frontmatter lines."""
    if atto.get("camera-url"):
        lines.append(f"camera-url: {atto.get('camera-url')}")
    if atto.get("camera-numero-atto"):
        lines.append(f"camera-numero-atto: {atto.get('camera-numero-atto')}")
    if atto.get("camera-legislatura"):
        lines.append(f"camera-legislatura: {atto.get('camera-legislatura')}")
    if atto.get("camera-tipo-iniziativa"):
        lines.append(f"camera-tipo-iniziativa: \"{atto.get('camera-tipo-iniziativa')}\"")
    if atto.get("camera-data-presentazione"):
        lines.append(f"camera-data-presentazione: \"{atto.get('camera-data-presentazione')}\"")
    if atto.get("camera-relazioni"):
        lines.append("camera-relazioni:")
        for relazione in atto.get("camera-relazioni", []):
            lines.append(f"  - {relazione}")
    if atto.get("camera-firmatari"):
        lines.append("camera-firmatari:")
        for dep in atto.get("camera-firmatari", []):
            if dep.get('role'):
                lines.append(f"  - \"{dep['name']} - {dep['role']}\"")
            elif dep.get('group'):
                lines.append(f"  - \"{dep['name']} - {dep['group']}\"")
            else:
                lines.append(f"  - \"{dep['name']}\"")
    if atto.get("camera-relatori"):
        lines.append("camera-relatori:")
        for rel in atto.get("camera-relatori", []):
            lines.append(f"  - \"{rel}\"")
    if atto.get("camera-votazione-finale"):
        lines.append(f"camera-votazione-finale: {atto.get('camera-votazione-finale')}")
    if atto.get("camera-dossier"):
        lines.append("camera-dossier:")
        for dossier_link in atto.get("camera-dossier", []):
            lines.append(f"  - {dossier_link}")


def _add_senato_metadata(atto: dict, lines: list):
    """Add Senato.it metadata to frontmatter lines."""
    if atto.get("senato-did"):
        lines.append(f"senato-did: {atto.get('senato-did')}")
    if atto.get("senato-legislatura"):
        lines.append(f"senato-legislatura: {atto.get('senato-legislatura')}")
    if atto.get("senato-numero-fase"):
        lines.append(f"senato-numero-fase: {atto.get('senato-numero-fase')}")
    if atto.get("senato-url"):
        lines.append(f"senato-url: {atto.get('senato-url')}")
    if atto.get("senato-titolo"):
        lines.append(f"senato-titolo: \"{atto.get('senato-titolo')}\"")
    if atto.get("senato-titolo-breve"):
        lines.append(f"senato-titolo-breve: \"{atto.get('senato-titolo-breve')}\"")
    if atto.get("senato-natura"):
        lines.append(f"senato-natura: \"{atto.get('senato-natura')}\"")
    if atto.get("senato-iniziativa"):
        lines.append(f"senato-iniziativa: \"{atto.get('senato-iniziativa')}\"")
    if atto.get("senato-data-presentazione"):
        lines.append(f"senato-data-presentazione: \"{atto.get('senato-data-presentazione')}\"")
    if atto.get("senato-teseo"):
        lines.append("senato-teseo:")
        for term in atto.get("senato-teseo", []):
            lines.append(f"  - \"{term}\"")
    if atto.get("senato-votazioni-url"):
        lines.append(f"senato-votazioni-url: {atto.get('senato-votazioni-url')}")
    if atto.get("senato-votazione-finale"):
        lines.append(f"senato-votazione-finale: {atto.get('senato-votazione-finale')}")
    if atto.get("senato-documenti"):
        lines.append("senato-documenti:")
        for doc_link in atto.get("senato-documenti", []):
            lines.append(f"  - {doc_link}")
