"""
Global state management for failures and warnings

This module manages global state for:
- HTTP request failures
- Missing law references
- Other warnings that need to be saved for PR descriptions
"""

from pathlib import Path
from datetime import datetime

# Global lists to track failures and warnings
REQUEST_FAILURES = []
MISSING_REFERENCES = []


def save_failures_and_warnings(output_dir: Path):
    """Save all failures and warnings to JSON files.

    Args:
        output_dir: Directory to save JSON files
    """
    from lib.persistence import save_json

    # Save request failures
    if REQUEST_FAILURES:
        failures_file = output_dir / "request_failures.json"
        save_json({
            "failures": REQUEST_FAILURES,
            "count": len(REQUEST_FAILURES),
            "timestamp": datetime.now().isoformat()
        }, failures_file)
        print(f"  ⚠ Request failures: {failures_file} ({len(REQUEST_FAILURES)} failures)")
        print(f"    These will be flagged in the PR for manual review")

    # Save missing references
    if MISSING_REFERENCES:
        missing_refs_file = output_dir / "missing_references.json"
        save_json({
            "missing_references": MISSING_REFERENCES,
            "count": len(MISSING_REFERENCES),
            "timestamp": datetime.now().isoformat()
        }, missing_refs_file)
        print(f"  ⚠ Missing law references: {missing_refs_file} ({len(MISSING_REFERENCES)} missing)")
        print(f"    Referenced laws not found in vault - will be flagged in PR")


def reset_state():
    """Reset all global state (useful for testing)."""
    global REQUEST_FAILURES, MISSING_REFERENCES
    REQUEST_FAILURES = []
    MISSING_REFERENCES = []
