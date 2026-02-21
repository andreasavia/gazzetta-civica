"""
normattiva_enrich.py — Enrich Legge objects with data scraped from normattiva.it.

Responsibility: for each Legge (already populated with core API fields), fetch:
  1. Permalink → legge.normattiva.uri  (URN-NIR) + legge.normattiva.data_vigenza
  2. Approfondimenti → legge.normattiva.{atti_aggiornati, lavori_preparatori, ...}
             also → legge.normattiva.gu_link
  3. Full text HTML → legge.normattiva.full_text_html

Usage (standalone test — reads from a previous --dump file):
    python normattiva_enrich.py dump_2026_01.json
    python normattiva_enrich.py dump_2026_01.json --limit 3   # only first 3 laws
"""

from __future__ import annotations

import argparse
import dataclasses
import html as html_module
import json
import re
import time
from functools import wraps
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from models import Legge, NormattivaData

# ── Constants ─────────────────────────────────────────────────────────────────

NORMATTIVA_SITE = "https://www.normattiva.it"

# Maps the link text on the N2Ls page → NormattivaData field name
_TEXT_TO_FIELD: dict[str, str] = {
    "atti aggiornati":              "atti_aggiornati",
    "atti correlati":               "atti_correlati",
    "lavori preparatori":           "lavori_preparatori",
    "aggiornamenti all'atto":       "aggiornamenti_atto",
    "note atto":                    "note_atto",
    "relazioni":                    "relazioni",
    "aggiornamenti al titolo":      "aggiornamenti_titolo",
    "aggiornamenti alla struttura": "aggiornamenti_struttura",
    "atti parlamentari":            "atti_parlamentari",
    "atti attuativi":               "atti_attuativi",
}

_APPROFONDIMENTO_FIELDS = list(_TEXT_TO_FIELD.values())


# ── Retry decorator ───────────────────────────────────────────────────────────

def retry_request(max_retries: int = 3, initial_delay: float = 2.0, backoff_factor: float = 2.0):
    """Retry HTTP calls with exponential backoff on network/HTTP errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, ConnectionError, TimeoutError) as exc:
                    if attempt < max_retries:
                        print(f"    ⚠ Retry {attempt + 1}/{max_retries} after {delay}s: {str(exc)[:100]}")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        print(f"    ✗ Failed after {max_retries} retries: {str(exc)[:100]}")
                        raise
        return wrapper
    return decorator


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_links(html: str) -> list[str]:
    """Extract normattiva / senato / camera links from an approfondimento HTML fragment."""
    links: list[str] = []
    for m in re.finditer(r'href="([^"]*)"', html):
        href = m.group(1).replace("&amp;", "&")
        if href.startswith("/atto/"):
            href = NORMATTIVA_SITE + href
        if any(x in href for x in ("caricaDettaglioAtto", "senato.it", "camera.it")):
            if href not in links:
                links.append(href)
    return links


def _extract_text(html: str) -> str:
    """Extract cleaned plain text from an HTML fragment."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    lines = [line.strip() for line in soup.get_text(separator="\n").split("\n") if line.strip()]
    return "\n".join(lines)


# ── Fetch functions ───────────────────────────────────────────────────────────

@retry_request(max_retries=3, initial_delay=2)
def _fetch_permalink(session: requests.Session, data_gu: str, codice: str) -> tuple[str, str]:
    """Fetch the URN-NIR permalink and entry-into-force date for a law.

    Args:
        session: Active requests session (cookies maintained across calls).
        data_gu: GU publication date (YYYY-MM-DD).
        codice:  Codice redazionale.

    Returns:
        Tuple of (uri, data_vigenza). Either may be empty string on failure.

    Raises:
        ValueError: If the URN cannot be found on the permalink page.
        requests.RequestException: On HTTP failure.
    """
    # Step 1: load main detail page to get the vigenza date
    main_url = (
        f"{NORMATTIVA_SITE}/atto/caricaDettaglioAtto"
        f"?atto.dataPubblicazioneGazzetta={data_gu}"
        f"&atto.codiceRedazionale={codice}"
    )
    main_resp = session.get(main_url, timeout=30)
    main_resp.raise_for_status()

    vigenza_iso = ""
    m = re.search(r"Entrata in vigore del provvedimento:\s*(\d{2}/\d{2}/\d{4})", main_resp.text)
    if m:
        from datetime import datetime
        vigenza_iso = datetime.strptime(m.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")

    # Step 2: permalink page gives us the canonical URN
    permalink_url = (
        f"{NORMATTIVA_SITE}/do/atto/vediPermalink"
        f"?atto.dataPubblicazioneGazzetta={data_gu}"
        f"&atto.codiceRedazionale={codice}"
    )
    resp = session.get(permalink_url, timeout=30)
    resp.raise_for_status()

    urn_match = re.search(
        r'href="(https://www\.normattiva\.it/uri-res/N2Ls\?urn:nir:[^"]+)"', resp.text
    )
    if not urn_match:
        raise ValueError(f"URN not found on permalink page for {codice}")

    uri = urn_match.group(1).strip()

    # Override or append the !vig= parameter with the correct date
    if vigenza_iso:
        if "!vig=" in uri:
            uri = re.sub(r"!vig=[^&\s]*", f"!vig={vigenza_iso}", uri)
        else:
            uri += f"!vig={vigenza_iso}"

    return uri, vigenza_iso


@retry_request(max_retries=3, initial_delay=2)
def _fetch_approfondimenti(session: requests.Session, uri: str) -> dict:
    """Fetch all approfondimenti sections from the N2Ls page.

    Returns:
        Dict with:
          - field names → list[str] of URLs  (for all approfondimento fields)
          - "gu_link"   → str
          - "lavori_preparatori_testo" → str  (raw text fallback, empty if links found)
    """
    result: dict = {f: [] for f in _APPROFONDIMENTO_FIELDS}
    result["gu_link"] = ""
    result["lavori_preparatori_testo"] = ""

    resp = session.get(uri, timeout=30)
    resp.raise_for_status()

    # GU link
    gu_m = re.search(r'href="(https?://www\.gazzettaufficiale\.it/[^"]+)"', resp.text)
    if gu_m:
        result["gu_link"] = gu_m.group(1).replace("&amp;", "&")

    # Each approfondimento section is an <a data-href="..."> link
    for m in re.finditer(r'<a\s[^>]*data-href="([^"]+)"[^>]*>\s*(.*?)\s*</a>', resp.text, re.DOTALL):
        data_href = m.group(1).replace("&amp;", "&")
        label = html_module.unescape(re.sub(r"\s+", " ", m.group(2)).strip().lower())

        field = _TEXT_TO_FIELD.get(label)
        if not field:
            continue

        sub = session.get(NORMATTIVA_SITE + data_href, timeout=30)
        sub.raise_for_status()

        if "Sessione Scaduta" in sub.text:
            raise RuntimeError(f"Session expired while fetching approfondimenti for {uri}")

        links = _extract_links(sub.text)
        if links:
            result[field] = links                   # list[str]
        elif field == "lavori_preparatori":
            # Fallback: store raw text in a dedicated field
            text = _extract_text(sub.text)
            if text:
                result["lavori_preparatori_testo"] = text

    return result


@retry_request(max_retries=3, initial_delay=2)
def _fetch_full_text(session: requests.Session, data_gu: str, codice: str) -> str:
    """Fetch the act text via the export endpoint, extracting only the printable content.

    The export endpoint returns a full HTML page. We extract only the
    `<div id="printThis">` contents — the actual law text with Akoma Ntoso
    (AKN) semantic classes — discarding navigation, scripts, and page chrome.
    This matches the behaviour of the original ricerca_normattiva.py.

    Args:
        session: Active requests session.
        data_gu: GU publication date (YYYY-MM-DD).
        codice:  Codice redazionale.

    Returns:
        Inner HTML of div#printThis, or full page HTML if the div is not found.
        Empty string if either arg is missing or fetch fails.

    Raises:
        requests.RequestException: On HTTP failure.
    """
    if not data_gu or not codice:
        return ""

    url = (
        f"{NORMATTIVA_SITE}/esporta/attoCompleto"
        f"?atto.dataPubblicazioneGazzetta={data_gu}"
        f"&atto.codiceRedazionale={codice}"
    )
    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    # Extract only the printable law text div (strips nav, scripts, page chrome)
    soup = BeautifulSoup(resp.text, "html.parser")
    print_div = soup.find("div", id="printThis")
    if print_div:
        return print_div.decode_contents()

    # Fallback: return full page if div not found
    return resp.text


# ── Public enrichment function ────────────────────────────────────────────────

def enrich(legge: Legge, session: requests.Session) -> Legge:
    """Enrich a single Legge in-place with all normattiva.it web data.

    Runs three fetch operations in sequence:
      1. Permalink  → legge.normattiva.uri + legge.normattiva.data_vigenza
      2. Approfondimenti → legge.normattiva.{atti_*, lavori_preparatori, gu_link, ...}
      3. Full text  → legge.normattiva.full_text_html

    Args:
        legge:   Legge object (already populated from normattiva_search).
        session: Shared requests.Session (maintains cookies across laws).

    Returns:
        The same Legge object, now enriched (mutates in-place and returns self).
    """
    n = legge.normattiva
    codice = n.codice_redazionale
    data_gu = n.data_gu

    # 1. Permalink + vigenza
    try:
        uri, vigenza = _fetch_permalink(session, data_gu, codice)
        n.uri = uri
        n.data_vigenza = vigenza
    except Exception as exc:
        print(f"    ✗ permalink failed: {str(exc)[:80]}")

    # 2. Approfondimenti (only if we have a valid URI)
    if n.uri:
        try:
            appro = _fetch_approfondimenti(session, n.uri)
            n.gu_link = appro.pop("gu_link", "")
            n.lavori_preparatori_testo = appro.pop("lavori_preparatori_testo", "")
            for field, value in appro.items():
                setattr(n, field, value)  # value is now list[str]
        except Exception as exc:
            print(f"    ✗ approfondimenti failed: {str(exc)[:80]}")

    # 3. Full text HTML
    try:
        n.full_text_html = _fetch_full_text(session, data_gu, codice)
    except Exception as exc:
        print(f"    ✗ full text failed: {str(exc)[:80]}")

    return legge


def enrich_all(leggi: list[Legge]) -> list[Legge]:
    """Enrich a list of Legge objects, sharing one session across all requests.

    Args:
        leggi: List of Legge from normattiva_search.fetch_atti().

    Returns:
        The same list, each Legge enriched in-place.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    for i, legge in enumerate(leggi):
        n = legge.normattiva
        print(f"  [{i+1}/{len(leggi)}] {n.codice_redazionale}...", end=" ", flush=True)
        enrich(legge, session)

        # Summary of what was populated
        populated = [f for f in _APPROFONDIMENTO_FIELDS if getattr(n, f)]
        vigenza_str = f"vig={n.data_vigenza}" if n.data_vigenza else "vig=?"
        text_str = f"{len(n.full_text_html):,}ch" if n.full_text_html else "no text"
        print(f"{vigenza_str} | {text_str} | appro=[{', '.join(populated) or 'none'}]")

    return leggi


# ── CLI / test entrypoint ─────────────────────────────────────────────────────

def main() -> list[Legge]:
    """Test entrypoint: load Legge objects from a prior dump, enrich them, save new dump.

    This avoids re-running the slow API search step during development.

    Usage:
        python normattiva_enrich.py dump_2026_01.json
        python normattiva_enrich.py dump_2026_01.json --limit 2
    """
    parser = argparse.ArgumentParser(
        description="Enrich Legge objects from a normattiva_search dump with web data.",
    )
    parser.add_argument("dump", help="Path to a dump_YYYY_MM.json from normattiva_search --dump")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only enrich the first N laws (for quick testing)")
    args = parser.parse_args()

    dump_path = Path(args.dump)
    if not dump_path.exists():
        parser.error(f"Dump file not found: {dump_path}")

    # Load Legge objects from the dump
    with dump_path.open(encoding="utf-8") as f:
        data = json.load(f)

    leggi = [Legge.from_api(raw) for raw in data["raw_api"]]
    if args.limit:
        leggi = leggi[: args.limit]

    print("=" * 60)
    print(f"Normattiva enrich: {len(leggi)} laws")
    print("=" * 60 + "\n")

    enrich_all(leggi)

    # Save full_text_html to individual files under dump_html/{codice}.html
    html_dir = dump_path.parent / "dump_html"
    html_dir.mkdir(exist_ok=True)

    out_path = dump_path.parent / dump_path.name.replace("dump_", "enriched_")
    dump_data = {
        "meta": data.get("meta", {}),
        "leggi": [],
    }

    for l in leggi:
        n = l.normattiva
        legge_dict = dataclasses.asdict(l)
        normattiva_dict = dataclasses.asdict(n)

        # Write full HTML to its own file, store relative path in dump
        if n.full_text_html:
            html_path = html_dir / f"{n.codice_redazionale}.html"
            html_path.write_text(n.full_text_html, encoding="utf-8")
            normattiva_dict["full_text_html"] = ""
            normattiva_dict["full_text_html_path"] = str(html_path.relative_to(dump_path.parent))
        else:
            normattiva_dict["full_text_html_path"] = ""

        legge_dict["normattiva"] = normattiva_dict
        dump_data["leggi"].append(legge_dict)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(dump_data, f, indent=2, ensure_ascii=False)

    html_count = sum(1 for l in leggi if l.normattiva.full_text_html)
    print(f"\n  💾 Enriched dump saved → {out_path}")
    print(f"  📄 Full text HTML → {html_dir}/ ({html_count} files)")
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)

    return leggi


if __name__ == "__main__":
    main()
