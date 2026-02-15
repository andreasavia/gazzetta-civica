#!/usr/bin/env python3
"""
validate_leggi.py — Validate legislative markdown files for required fields.

Ensures that all .md files in content/leggi/ have the required YAML frontmatter
fields based on their tipo (LEGGE, DECRETO, DECRETO-LEGGE, etc.).
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List

# Required fields for all legislative acts
COMMON_REQUIRED_FIELDS = {
    "codice-redazionale",
    "tipo",
    "numero-atto",
    "data-emanazione",
    "data-gu",
    "numero-gu",
    "data-vigenza",
    "normattiva-urn",
    "normattiva-link",
    "gu-link",
    "titolo-atto",
    "descrizione-atto",
    "titolo-alternativo",
}

# Additional required fields for specific types
# Most types only require common fields; add additional requirements here if needed
TYPE_SPECIFIC_FIELDS = {
    "LEGGE": set(),  # LEGGE uses only common fields
    "DECRETO-LEGGE": set(),  # DECRETO-LEGGE uses only common fields
    "DECRETO LEGISLATIVO": set(),  # DECRETO LEGISLATIVO uses only common fields
    "DECRETO": set(),  # DECRETO uses only common fields
    # Add more types as needed
}


def extract_frontmatter(content: str) -> Dict[str, any]:
    """Extract YAML frontmatter from markdown content.

    Returns:
        dict: Parsed frontmatter fields
    """
    frontmatter_match = re.match(r'^---\n(.*?)\n---', content, re.DOTALL)
    if not frontmatter_match:
        return {}

    frontmatter = frontmatter_match.group(1)
    fields = {}

    # Parse simple key: value pairs and key:\n  - list items
    current_key = None
    for line in frontmatter.split('\n'):
        # Key: value or Key: "value"
        key_value_match = re.match(r'^([a-z-]+):\s*(.*)$', line)
        if key_value_match:
            key = key_value_match.group(1)
            value = key_value_match.group(2).strip()
            current_key = key

            # If value is not empty, it's a simple field
            if value:
                fields[key] = value
            else:
                # Empty value means it's a list that follows
                fields[key] = []
        # List item (  - value)
        elif line.strip().startswith('- ') and current_key:
            if current_key in fields and isinstance(fields[current_key], list):
                fields[current_key].append(line.strip()[2:])

    return fields


def validate_file(filepath: Path) -> List[str]:
    """Validate a single markdown file.

    Returns:
        list: Error messages (empty if valid)
    """
    errors = []

    try:
        content = filepath.read_text(encoding='utf-8')
    except Exception as e:
        return [f"Failed to read file: {e}"]

    # Extract frontmatter
    fields = extract_frontmatter(content)

    if not fields:
        return ["No YAML frontmatter found"]

    # Get tipo to determine required fields
    tipo = fields.get("tipo", "").strip('"')
    if not tipo:
        return ["Missing required field: tipo"]

    # Determine required fields for this tipo
    required_fields = COMMON_REQUIRED_FIELDS.copy()
    if tipo in TYPE_SPECIFIC_FIELDS:
        required_fields.update(TYPE_SPECIFIC_FIELDS[tipo])

    # Check for missing required fields
    missing_fields = required_fields - set(fields.keys())

    if missing_fields:
        errors.append(f"Missing required fields for tipo '{tipo}': {', '.join(sorted(missing_fields))}")

    # Validate field formats
    # data-vigenza should be YYYY-MM-DD
    if "data-vigenza" in fields:
        vigenza = fields["data-vigenza"].strip('"')
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', vigenza):
            errors.append(f"Invalid data-vigenza format (expected YYYY-MM-DD): {vigenza}")

    # data-emanazione should be YYYY-MM-DD
    if "data-emanazione" in fields:
        emanazione = fields["data-emanazione"].strip('"')
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', emanazione):
            errors.append(f"Invalid data-emanazione format (expected YYYY-MM-DD): {emanazione}")

    # data-gu should be YYYY-MM-DD
    if "data-gu" in fields:
        data_gu = fields["data-gu"].strip('"')
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', data_gu):
            errors.append(f"Invalid data-gu format (expected YYYY-MM-DD): {data_gu}")

    return errors


def validate_directory(directory: Path, verbose: bool = False) -> int:
    """Validate all markdown files in directory.

    Returns:
        int: Number of files with errors
    """
    # Find all .md files in content/leggi/
    md_files = list(directory.glob("**/*.md"))

    if not md_files:
        print(f"No markdown files found in {directory}")
        return 0

    print(f"Validating {len(md_files)} markdown files in {directory}")
    print("=" * 80)

    error_count = 0
    valid_count = 0

    for filepath in sorted(md_files):
        # Skip files inside 'interventi' folders
        if "interventi" in filepath.parts:
            continue

        relative_path = filepath.relative_to(directory.parent)
        errors = validate_file(filepath)

        if errors:
            error_count += 1
            print(f"\n❌ {relative_path}")
            for error in errors:
                print(f"   • {error}")
        elif verbose:
            valid_count += 1
            print(f"✓ {relative_path}")

    print("\n" + "=" * 80)
    print(f"Results: {valid_count} valid, {error_count} errors")

    if error_count > 0:
        print(f"\n❌ Validation failed: {error_count} file(s) with errors")
        return error_count
    else:
        print("\n✅ All files passed validation!")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Validate legislative markdown files for required fields",
        epilog="Exit code 0 = success, >0 = number of files with errors"
    )
    parser.add_argument(
        "directory",
        type=Path,
        nargs="?",
        default=Path(__file__).parent.parent / "content" / "leggi",
        help="Directory to validate (default: content/leggi/)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show all files (including valid ones)"
    )

    args = parser.parse_args()

    if not args.directory.exists():
        print(f"Error: Directory not found: {args.directory}", file=sys.stderr)
        sys.exit(1)

    if not args.directory.is_dir():
        print(f"Error: Not a directory: {args.directory}", file=sys.stderr)
        sys.exit(1)

    error_count = validate_directory(args.directory, verbose=args.verbose)
    sys.exit(min(error_count, 255))  # Exit code max is 255


if __name__ == "__main__":
    main()
