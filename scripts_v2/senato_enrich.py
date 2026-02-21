"""
senato_enrich.py — Enrich Legge objects with data from Senato.it.

Responsibility: for each Legge that has a senato.it DDL link in
legge.normattiva.lavori_preparatori, fetch:
  1. Resolve the URN redirect → did, final URL
  2. Scrape the bill page → legge.senato.{did, legislatura, numero_fase,
                                             url, titolo, titolo_breve,
                                             natura, iniziativa, teseo,
                                             data_presentazione, documenti}
  3. Fetch the votazioni tab → legge.senato.{votazioni_url, votazione_finale}

All calls are retried via the shared retry_request decorator.

Usage (standalone test):
    python senato_enrich.py        # reads legge_dump_26G00024.json
    python senato_enrich.py enriched_2026_01.json --limit 2
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from models import Legge, SenatoData
from utils import retry_request

# Rate-limit delay between laws
_DELAY_BETWEEN_LAWS = 2.0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_senato_url(url: str) -> tuple[str, str]:
    """Extract legislatura and numero_fase from a senato.it URN.

    Expected format: ...N2Ls?urn:senato-it:parl:ddl:senato;19.legislatura;1462

    Returns:
        (legislatura, numero_fase) as strings.

    Raises:
        ValueError: If the pattern is not found.
    """
    m = re.search(r"(\d+)\.legislatura;(\d+)", url)
    if not m:
        raise ValueError(f"Could not parse legislatura/numero_fase from: {url}")
    return m.group(1), m.group(2)


# ── Retried fetch functions ───────────────────────────────────────────────────

@retry_request(max_retries=3, initial_delay=2)
def _fetch_bill_page(session: requests.Session, url: str) -> tuple[str, str]:
    """Follow the URN redirect and return (final_url, html).

    The senato.it URN redirects to a page with ?did=NNNN in the URL,
    which is the stable identifier we need for the votazioni tab.

    Raises:
        ValueError: If did is not found in the redirect URL.
        requests.RequestException: On HTTP failure.
    """
    resp = session.get(url, timeout=30, allow_redirects=True)
    resp.raise_for_status()

    did_m = re.search(r"[?&]did=(\d+)", resp.url)
    if not did_m:
        raise ValueError(f"did not found in redirect URL: {resp.url}")

    return resp.url, resp.text


@retry_request(max_retries=3, initial_delay=2)
def _fetch_votazioni_page(session: requests.Session, did: str) -> str:
    """Fetch the votazioni tab for a given bill did.

    Raises:
        requests.RequestException: On HTTP failure.
    """
    url = (
        f"https://www.senato.it/leggi-e-documenti/disegni-di-legge/scheda-ddl"
        f"?tab=votazioni&did={did}"
    )
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


# ── Parsing helpers (pure functions) ─────────────────────────────────────────

def _parse_bill_html(html: str) -> dict:
    """Extract all metadata fields from the bill page HTML.

    Returns a dict with keys matching SenatoData fields (minus did/legislatura/
    numero_fase/url which are set by the caller).
    """
    result: dict = {}
    soup = BeautifulSoup(html, "html.parser")

    # titolo — first <span> inside <div class="boxTitolo">
    box = soup.find("div", class_="boxTitolo")
    if box:
        span = box.find("span")
        if span:
            result["titolo"] = span.get_text(strip=True)

    # titolo_breve — <em> after <strong>Titolo breve</strong>
    strong = soup.find("strong", string=re.compile("Titolo breve"))
    if strong:
        em = strong.find_next("em")
        if em:
            result["titolo_breve"] = em.get_text(strip=True)

    # natura — first <p> inside <h2>Natura</h2>, first <span> or plain text
    natura_h2 = soup.find("h2", string=re.compile("Natura", re.IGNORECASE))
    if natura_h2:
        p = natura_h2.find_next("p")
        if p:
            span = p.find("span")
            raw = (span or p).get_text(strip=True)
            # Strip trailing details after "Contenente", "Relazione", "Include"
            raw = re.split(r"(?:Contenente|Relazione|Include)", raw)[0].strip()
            result["natura"] = re.sub(r"[,.\s]+$", "", raw)

    # iniziativa — string search
    if "Iniziativa Parlamentare" in html:
        result["iniziativa"] = "Parlamentare"
    elif "Iniziativa Governativa" in html:
        result["iniziativa"] = "Governativa"

    # data_presentazione — try two common patterns
    for pattern in [
        r"Data(?:\s+di)?\s+presentazione[:\s]+(\d{1,2}/\d{1,2}/\d{4})",
        r"Presentato il[:\s]+(\d{1,2}/\d{1,2}/\d{4})",
    ]:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            result["data_presentazione"] = m.group(1)
            break

    # teseo — list of terms from <h2>Classificazione TESEO</h2>
    teseo_h2 = soup.find("h2", string=re.compile("Classificazione TESEO", re.IGNORECASE))
    if teseo_h2:
        p = teseo_h2.find_next("p")
        if p:
            terms = [
                span.get_text(strip=True).strip(",").strip()
                for span in p.find_all("span")
                if span.get_text(strip=True).strip(",").strip()
            ]
            if terms:
                result["teseo"] = terms

    # documenti — PDF / XML / /stampe/ / /testi/ links
    doc_links: list[str] = []
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if not any(x in href.lower() for x in [".pdf", ".xml", ".doc", "/stampe/", "/testi/"]):
            continue
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = "https://www.senato.it" + href
        elif not href.startswith("http"):
            href = "https://www.senato.it/" + href
        href = re.sub(r"([^:])//+", r"\1/", href)
        if href not in doc_links:
            doc_links.append(href)
    if doc_links:
        result["documenti"] = doc_links

    return result


def _parse_votazioni_html(html: str, did: str) -> tuple[str, str | None]:
    """Extract the votazioni tab URL and final vote link.

    Returns:
        (votazioni_url, votazione_finale)
        votazione_finale is None if not found on the page.
    """
    votazioni_url = (
        f"https://www.senato.it/leggi-e-documenti/disegni-di-legge/scheda-ddl"
        f"?tab=votazioni&did={did}"
    )
    votazione_finale: str | None = None

    soup = BeautifulSoup(html, "html.parser")
    for li in soup.find_all("li"):
        strong = li.find("strong")
        if strong and "Votazione finale" in strong.get_text():
            a = li.find("a", class_="schedaCamera")
            if a and a.get("href"):
                href = a["href"]
                if not href.startswith("http"):
                    href = "https://www.senato.it" + href
                votazione_finale = href
            break

    return votazioni_url, votazione_finale


# ── Public enrichment function ────────────────────────────────────────────────

def enrich(legge: Legge, session: requests.Session) -> Legge:
    """Enrich a single Legge in-place with Senato.it data.

    Skips silently if no senato.it DDL link is in lavori_preparatori.

    Args:
        legge:   Legge object (normattiva + camera stages already complete).
        session: Shared requests.Session.

    Returns:
        The same Legge object, now enriched in-place.
    """
    # Filter: must be a senato.it DDL link (not just any senato.it URL)
    senato_links = [
        l for l in legge.normattiva.lavori_preparatori
        if "senato.it" in l and "ddl" in l
    ]
    if not senato_links:
        return legge

    senato_url = senato_links[0]
    s = legge.senato

    try:
        legislatura, numero_fase = _parse_senato_url(senato_url)
        s.legislatura = legislatura
        s.numero_fase = numero_fase
    except ValueError as exc:
        print(f"    ✗ URL parse failed: {exc}")
        return legge

    # 1. Fetch + parse the bill page
    try:
        final_url, html = _fetch_bill_page(session, senato_url)

        # Extract did from final URL
        did_m = re.search(r"[?&]did=(\d+)", final_url)
        s.did = did_m.group(1) if did_m else None
        s.url = final_url

        bill_data = _parse_bill_html(html)
        s.titolo          = bill_data.get("titolo")
        s.titolo_breve    = bill_data.get("titolo_breve")
        s.natura          = bill_data.get("natura")
        s.iniziativa      = bill_data.get("iniziativa")
        s.data_presentazione = bill_data.get("data_presentazione")
        s.teseo           = bill_data.get("teseo", [])
        s.documenti       = bill_data.get("documenti", [])
    except Exception as exc:
        legge.failures.append({"stage": "senato_page", "error": str(exc)[:200]})
        return legge

    # 2. Fetch votazioni tab (non-fatal if it fails)
    if s.did:
        try:
            vot_html = _fetch_votazioni_page(session, s.did)
            s.votazioni_url, s.votazione_finale = _parse_votazioni_html(vot_html, s.did)
            if s.votazione_finale is None:
                print(f"    ⚠ votazione finale not found on voting page")
        except Exception as exc:
            legge.failures.append({"stage": "senato_votazioni", "error": str(exc)[:200]})

    return legge


def enrich_all(leggi: list[Legge]) -> list[Legge]:
    """Enrich a list of Legge objects with Senato.it data.

    Args:
        leggi: List of Legge (normattiva + camera stages already complete).

    Returns:
        The same list, each Legge enriched in-place.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    for i, legge in enumerate(leggi):
        n = legge.normattiva
        has_senato = any(
            "senato.it" in l and "ddl" in l
            for l in n.lavori_preparatori
        )

        print(f"  [{i+1}/{len(leggi)}] {n.codice_redazionale}...", end=" ", flush=True)

        if not has_senato:
            print("no senato.it DDL link")
            continue

        enrich(legge, session)

        s = legge.senato
        info = f"DDL {s.numero_fase}, did={s.did}"
        if s.iniziativa:
            info += f" ({s.iniziativa})"
        if s.teseo:
            info += f" | TESEO: {', '.join(s.teseo[:3])}"
        if s.votazione_finale:
            info += " | ✓ votazione"
        print(info)

        time.sleep(_DELAY_BETWEEN_LAWS)

    return leggi


# ── CLI / test entrypoint ─────────────────────────────────────────────────────

def main() -> list[Legge]:
    """Test entrypoint: load from a dump and enrich with senato data.

    Usage:
        python senato_enrich.py legge_dump_26G00024.json
        python senato_enrich.py enriched_2026_01.json --limit 2
    """
    parser = argparse.ArgumentParser(
        description="Enrich Legge objects with Senato.it metadata.",
    )
    parser.add_argument(
        "dump",
        nargs="?",
        default="legge_dump_26G00024.json",
        help="Path to a Legge JSON dump (default: legge_dump_26G00024.json)",
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N laws with a senato.it link")
    args = parser.parse_args()

    dump_path = Path(args.dump)
    if not dump_path.exists():
        parser.error(f"Dump file not found: {dump_path}")

    with dump_path.open(encoding="utf-8") as f:
        data = json.load(f)

    # Support both single-Legge dumps and list dumps
    if "leggi" in data:
        # enriched_YYYY_MM.json format
        leggi = [Legge.from_api({
            "codiceRedazionale":   l["normattiva"]["codice_redazionale"],
            "descrizioneAtto":     l["normattiva"]["descrizione_atto"],
            "titoloAtto":          l["normattiva"]["titolo_atto"],
            "numeroProvvedimento": l["normattiva"]["numero_provvedimento"],
            "denominazioneAtto":   l["normattiva"]["denominazione_atto"],
            "dataGU":              l["normattiva"]["data_gu"],
            "numeroGU":            l["normattiva"]["numero_gu"],
            "dataEmanazione":      l["normattiva"]["data_emanazione"],
        }) for l in data["leggi"]]
        for legge, l in zip(leggi, data["leggi"]):
            legge.normattiva.lavori_preparatori = l["normattiva"].get(
                "lavori_preparatori", []
            )
    else:
        # Single legge_dump_XXXXX.json — reconstruct one Legge
        import dataclasses
        n = data["normattiva"]
        legge = Legge.from_api({
            "codiceRedazionale":   n["codice_redazionale"],
            "descrizioneAtto":     n["descrizione_atto"],
            "titoloAtto":          n["titolo_atto"],
            "numeroProvvedimento": n["numero_provvedimento"],
            "denominazioneAtto":   n["denominazione_atto"],
            "dataGU":              n["data_gu"],
            "numeroGU":            n["numero_gu"],
            "dataEmanazione":      n["data_emanazione"],
        })
        legge.normattiva.lavori_preparatori = n.get("lavori_preparatori", [])
        legge.normattiva.data_vigenza = n.get("data_vigenza", "")
        leggi = [legge]

    # Apply limit to laws with senato links
    if args.limit:
        senato_leggi = [
            l for l in leggi
            if any("senato.it" in x and "ddl" in x for x in l.normattiva.lavori_preparatori)
        ]
        leggi = senato_leggi[: args.limit]

    print("=" * 60)
    print(f"Senato enrich: {len(leggi)} laws")
    print("=" * 60 + "\n")

    enrich_all(leggi)

    # Print result for the first enriched law
    for legge in leggi:
        if legge.senato.did:
            import dataclasses, json as _json
            print("\nSenatoData dump:")
            print(_json.dumps(dataclasses.asdict(legge.senato), indent=2, ensure_ascii=False))
            break

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)

    return leggi


if __name__ == "__main__":
    main()
