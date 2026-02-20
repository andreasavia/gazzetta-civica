#!/usr/bin/env python3
"""
Find the folder containing a markdown file with a specific camera-atto-iri.

This script searches through content/leggi to find the markdown file
that has the specified camera-atto-iri in its frontmatter, and returns
the folder path.

Usage:
    python scripts/find_atto_folder.py <atto_iri>

Example:
    python scripts/find_atto_folder.py http://dati.camera.it/ocd/attocamera.rdf/ac19_1621
"""

import sys
import re
from pathlib import Path


def extract_frontmatter(content: str) -> dict:
    """Extract YAML frontmatter from markdown content."""
    # Match frontmatter between --- delimiters
    match = re.match(r'^---\s*\n(.*?)\n---\s*', content, re.DOTALL)
    if not match:
        return {}

    frontmatter_text = match.group(1)
    frontmatter = {}

    # Simple YAML parser for our use case (single-line values)
    for line in frontmatter_text.split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            frontmatter[key] = value

    return frontmatter


def find_atto_folder(atto_iri: str, base_path: str = "content/leggi") -> str:
    """
    Find the folder containing the markdown file with the specified camera-atto-iri.

    Args:
        atto_iri: The IRI to search for (e.g., http://dati.camera.it/ocd/attocamera.rdf/ac19_1621)
        base_path: Base directory to search in

    Returns:
        The folder path containing the matching markdown file, or empty string if not found
    """
    base = Path(base_path)

    if not base.exists():
        print(f"Error: Base path {base_path} does not exist", file=sys.stderr)
        return ""

    # Search all .md files recursively
    for md_file in base.rglob("*.md"):
        try:
            content = md_file.read_text(encoding='utf-8')
            frontmatter = extract_frontmatter(content)

            # Check if this file has the matching camera-atto-iri
            if frontmatter.get('camera-atto-iri') == atto_iri:
                # Return the parent directory path
                folder = str(md_file.parent)
                return folder

        except Exception as e:
            print(f"Warning: Could not process {md_file}: {e}", file=sys.stderr)
            continue

    return ""


def main():
    if len(sys.argv) < 2:
        print("Usage: python find_atto_folder.py <atto_iri>", file=sys.stderr)
        print("Example: python find_atto_folder.py http://dati.camera.it/ocd/attocamera.rdf/ac19_1621", file=sys.stderr)
        sys.exit(1)

    atto_iri = sys.argv[1]

    # Normalize IRI format
    if not atto_iri.startswith("http://"):
        if atto_iri.startswith("ac"):
            # Format: ac19_1621
            atto_iri = f"http://dati.camera.it/ocd/attocamera.rdf/{atto_iri}"
        else:
            # Assume format: "19 1621"
            parts = atto_iri.split()
            if len(parts) == 2:
                legislatura, numero = parts
                atto_iri = f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{numero}"

    folder = find_atto_folder(atto_iri)

    if folder:
        print(folder)
        sys.exit(0)
    else:
        print(f"Error: No folder found for atto IRI: {atto_iri}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
