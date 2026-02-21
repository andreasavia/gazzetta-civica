"""
camera_enrich.py — Enrich Legge objects with data from Camera.it.

Responsibility: for each Legge that has a camera.it link in
legge.normattiva.lavori_preparatori, fetch:
  1. RDF metadata → legge.camera.{legislatura, atto, natura, iniziativa,
                                   data_presentazione, relazioni,
                                   firmatari, relatori}
  2. HTML page   → legge.camera.{votazione_finale, dossier}
                   (for gov. bills: overrides firmatari from HTML)
  3. Interventi  → esame_assemblea.json + seduta_N.md files in
                   content/leggi/.../interventi/

All HTTP sub-calls (including per-person RDF lookups) are retried with
exponential backoff via the shared retry_request decorator.

Usage (standalone test):
    python camera_enrich.py enriched_2026_01.json
    python camera_enrich.py enriched_2026_01.json --limit 2
"""

from __future__ import annotations

import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import requests

from models import Legge, CameraData, EsameAssemblea
from utils import retry_request
from parse_esame_assemblea import (
    parse_esame_assemblea,
    enrich_with_stenografico_text,
    generate_markdown_files,
)

# ── RDF namespaces ────────────────────────────────────────────────────────────

_NS = {
    "rdf":  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc":   "http://purl.org/dc/elements/1.1/",
    "ocd":  "http://dati.camera.it/ocd/",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "foaf": "http://xmlns.com/foaf/0.1/",
}

_MONTHS_IT = [
    "", "gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno",
    "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre",
]

# Rate-limit delay between laws (Camera.it is sensitive to rapid requests)
_DELAY_BETWEEN_LAWS = 3.0


# ── Pure helpers ──────────────────────────────────────────────────────────────

def _parse_camera_url(url: str) -> tuple[str, str]:
    """Extract legislatura and atto number from a camera.it URI.

    Expected format: ...N2Ls?urn:camera-it:parlamento:...:camera;19.legislatura;1621

    Returns:
        (legislatura, atto_num) as strings.

    Raises:
        ValueError: If the pattern is not found.
    """
    m = re.search(r"(\d+)\.legislatura;(\d+)", url)
    if not m:
        raise ValueError(f"Could not parse legislatura/atto from camera URL: {url}")
    return m.group(1), m.group(2)


def _resolve_blank_node(root, node_id: str) -> str:
    """Resolve an RDF blank node to the persona/deputato URI it wraps."""
    for desc in root.findall(".//rdf:Description", _NS):
        if desc.get(f"{{{_NS['rdf']}}}nodeID", "") == node_id:
            rif = desc.find("ocd:rif_persona", _NS)
            if rif is not None:
                return rif.get(f"{{{_NS['rdf']}}}resource", "")
    return ""


def _parse_html_firmatari(html: str, legislatura: str) -> list[dict]:
    """Parse firmatari from the HTML page for government bills.

    Government bills show ministerial roles in a <div class="iniziativa">
    rather than parliamentary groups.
    """
    firmatari: list[dict] = []
    m = re.search(r'<div class="iniziativa">(.*?)</div>', html, re.DOTALL)
    if not m:
        return firmatari

    section = m.group(1)
    pattern = (
        r'<a\s+href="[^"]*idPersona=(\d+)"[^>]*>([^<]+)</a>'
        r'\s*</span>\s*\(<em>([^<]+)</em>\)'
    )
    for match in re.finditer(pattern, section):
        person_id, name, role = match.groups()
        firmatari.append({
            "name":  re.sub(r"\s+", " ", name).strip(),
            "role":  role.strip(),
            "link":  (
                f"https://documenti.camera.it/apps/commonServices/getDocumento.ashx"
                f"?sezione=deputati&tipoDoc=schedaDeputato"
                f"&idlegislatura={legislatura}&idPersona={person_id}"
            ),
        })
    return firmatari


# ── Retried fetch functions ───────────────────────────────────────────────────

@retry_request(max_retries=3, initial_delay=2)
def _fetch_rdf(session: requests.Session, rdf_url: str) -> ET.Element:
    """Fetch and parse an RDF/XML document.

    Raises:
        requests.RequestException: On HTTP failure.
        ET.ParseError: If response is not valid XML.
    """
    resp = session.get(rdf_url, headers={"Accept": "application/rdf+xml"}, timeout=30)
    resp.raise_for_status()
    return ET.fromstring(resp.text)


@retry_request(max_retries=3, initial_delay=2)
def _fetch_html(session: requests.Session, url: str) -> str:
    """Fetch an HTML page. Raises on HTTP error."""
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


@retry_request(max_retries=3, initial_delay=2)
def _fetch_parliamentary_group(
    session: requests.Session, person_uri: str, legislatura: str
) -> str:
    """Fetch the parliamentary group abbreviation for a deputy.

    Converts persona.rdf URI → deputato.rdf URI if needed, then follows
    the ocd:rif_gruppoParlamentare link to get the group sigla.

    Returns:
        Group abbreviation string, or empty string if not found.
    """
    # persona.rdf/p50204 → deputato.rdf/d50204_19
    if "persona.rdf" in person_uri:
        m = re.search(r"/p(\d+)", person_uri)
        if m:
            person_uri = (
                f"http://dati.camera.it/ocd/deputato.rdf/d{m.group(1)}_{legislatura}"
            )

    root = _fetch_rdf(session, person_uri)

    gruppo_uri = ""
    for desc in root.findall(".//rdf:Description", _NS):
        elem = desc.find("ocd:rif_gruppoParlamentare", _NS)
        if elem is not None:
            gruppo_uri = elem.get(f"{{{_NS['rdf']}}}resource", "")
            if gruppo_uri:
                break

    if not gruppo_uri:
        return ""

    gruppo_root = _fetch_rdf(session, gruppo_uri)
    for desc in gruppo_root.findall(".//rdf:Description", _NS):
        if desc.get(f"{{{_NS['rdf']}}}about", "") != gruppo_uri:
            continue
        sigla = desc.find("ocd:sigla", _NS)
        if sigla is not None and sigla.text:
            return sigla.text.strip()
        label = desc.find("rdfs:label", _NS)
        if label is not None and label.text:
            m = re.search(r"\(([A-Z\-]+)\)\s*\(", label.text)
            return m.group(1) if m else label.text.strip()

    return ""


@retry_request(max_retries=3, initial_delay=2)
def _fetch_relatore_name(session: requests.Session, ref_uri: str) -> str:
    """Fetch a rapporteur's name from their RDF URI.

    Returns:
        Name string, or empty string if not found.
    """
    root = _fetch_rdf(session, ref_uri)
    for desc in root.findall(".//rdf:Description", _NS):
        creator = desc.find("dc:creator", _NS)
        if creator is not None and creator.text:
            return creator.text.strip()
    return ""


# ── Core enrichment logic ─────────────────────────────────────────────────────

def _parse_rdf_metadata(
    session: requests.Session, legislatura: str, atto_num: str
) -> dict:
    """Fetch the attocamera RDF and extract all bill metadata.

    Returns a partial dict with keys matching CameraData fields.
    """
    rdf_url = f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{atto_num}"
    root = _fetch_rdf(session, rdf_url)

    result: dict = {
        "atto_iri": rdf_url,
        "relazioni": [],
        "firmatari": [],
        "relatori": [],
    }

    # Find the Description element for this atto
    atto_elem = None
    for desc in root.findall(".//rdf:Description", _NS):
        about = desc.get(f"{{{_NS['rdf']}}}about", "")
        if f"ac{legislatura}_{atto_num}" in about:
            atto_elem = desc
            break
    if atto_elem is None:
        descs = root.findall(".//rdf:Description", _NS)
        atto_elem = descs[0] if descs else None
    if atto_elem is None:
        return result

    # natura (dc:type)
    el = atto_elem.find("dc:type", _NS)
    if el is not None and el.text:
        result["natura"] = el.text.strip()

    # iniziativa (ocd:iniziativa)
    el = atto_elem.find("ocd:iniziativa", _NS)
    if el is not None and el.text:
        result["iniziativa"] = el.text.strip()

    # data_presentazione (dc:date — YYYYMMDD → Italian text)
    el = atto_elem.find("dc:date", _NS)
    if el is not None and el.text:
        raw = el.text.strip()
        if len(raw) == 8 and raw.isdigit():
            try:
                dt = datetime.strptime(raw, "%Y%m%d")
                result["data_presentazione"] = (
                    f"{dt.day} {_MONTHS_IT[dt.month]} {dt.year}"
                )
            except ValueError:
                result["data_presentazione"] = raw
        else:
            result["data_presentazione"] = raw

    # relazioni (dc:relation — PDF links only)
    for el in atto_elem.findall("dc:relation", _NS):
        res = el.get(f"{{{_NS['rdf']}}}resource", "")
        if res.endswith(".pdf"):
            result["relazioni"].append(res)

    # ── firmatari ─────────────────────────────────────────────────────────────
    # primo_firmatario: names from dc:creator, URIs from ocd:primo_firmatario
    deputies: list[dict] = []
    for el in atto_elem.findall("dc:creator", _NS):
        if el.text:
            name = el.text.strip()
            if not any(d["name"] == name for d in deputies):
                deputies.append({"name": name, "link": ""})

    primo_uris: list[str] = []
    for el in atto_elem.findall("ocd:primo_firmatario", _NS):
        uri = el.get(f"{{{_NS['rdf']}}}resource", "")
        if uri:
            primo_uris.append(uri)
        else:
            node_id = el.get(f"{{{_NS['rdf']}}}nodeID", "")
            if node_id:
                uri = _resolve_blank_node(root, node_id)
                if uri:
                    primo_uris.append(uri)

    # Fetch group for each primo_firmatario
    for i, dep in enumerate(deputies):
        if i < len(primo_uris):
            try:
                group = _fetch_parliamentary_group(session, primo_uris[i], legislatura)
                if group:
                    dep["group"] = group
            except Exception:
                pass  # Non-fatal: deputy name still captured

    # altro_firmatario: names from dc:contributor, URIs from ocd:altro_firmatario
    contributors = [
        el.text.strip()
        for el in atto_elem.findall("dc:contributor", _NS)
        if el.text
    ]
    altro_uris: list[str] = []
    for el in atto_elem.findall("ocd:altro_firmatario", _NS):
        uri = el.get(f"{{{_NS['rdf']}}}resource", "")
        if uri:
            altro_uris.append(uri)
        else:
            node_id = el.get(f"{{{_NS['rdf']}}}nodeID", "")
            if node_id:
                uri = _resolve_blank_node(root, node_id)
                if uri:
                    altro_uris.append(uri)

    for i, name in enumerate(contributors):
        if not any(d["name"] == name for d in deputies):
            group = ""
            if i < len(altro_uris):
                try:
                    group = _fetch_parliamentary_group(
                        session, altro_uris[i], legislatura
                    )
                except Exception:
                    pass
            deputies.append({"name": name, "group": group})

    if deputies:
        result["firmatari"] = deputies

    # relatori (ocd:rif_relatore — separate RDF fetch per relatore)
    relatori_refs = [
        el.get(f"{{{_NS['rdf']}}}resource", "")
        for el in atto_elem.findall("ocd:rif_relatore", _NS)
        if el.get(f"{{{_NS['rdf']}}}resource", "")
    ]
    relatori: list[str] = []
    for ref in relatori_refs:
        try:
            name = _fetch_relatore_name(session, ref)
            if name and name not in relatori:
                relatori.append(name)
        except Exception:
            pass
    if relatori:
        result["relatori"] = relatori

    return result


def _parse_html_metadata(html: str, legislatura: str) -> dict:
    """Extract votazione_finale, dossier links, and (for gov. bills) firmatari."""
    result: dict = {"dossier": []}

    # Final vote link
    m = re.search(r'href="([^"]*votazioni[^"]*schedaVotazione[^"]*)"', html)
    if m:
        link = m.group(1).replace("&amp;", "&")
        if not link.startswith("http"):
            link = "https://www.camera.it" + link
        result["votazione_finale"] = link

    # Dossier links
    for m in re.finditer(r'href="([^"]*dossier[^"]*)"', html, re.IGNORECASE):
        link = m.group(1).replace("&amp;", "&")
        if not link.startswith("http"):
            link = "https://www.camera.it" + link
        if link not in result["dossier"]:
            result["dossier"].append(link)

    return result


def _compute_interventi_dir(legge: Legge, vault_dir: Path) -> Path | None:
    """Compute the interventi directory path from the law's emanation date."""
    n = legge.normattiva
    try:
        dt = datetime.strptime(n.data_emanazione, "%Y-%m-%d")
        norm_dir = (
            vault_dir
            / str(dt.year)
            / f"{dt.month:02d}"
            / f"{dt.day:02d}"
            / f"n. {n.numero_provvedimento}"
        )
        return norm_dir / "interventi"
    except ValueError:
        return None


# ── Public enrichment function ────────────────────────────────────────────────

def enrich(legge: Legge, session: requests.Session, vault_dir: Path) -> Legge:
    """Enrich a single Legge in-place with Camera.it data.

    Skips silently if no camera.it link is present in lavori_preparatori.

    Args:
        legge:     Legge object (normattiva stage already complete).
        session:   Shared requests.Session.
        vault_dir: Root vault directory (content/leggi/) for computing
                   the interventi output path.

    Returns:
        The same Legge object, now enriched in-place.
    """
    camera_links = [
        l for l in legge.normattiva.lavori_preparatori if "camera.it" in l
    ]
    if not camera_links:
        return legge

    camera_url = camera_links[0]
    c = legge.camera

    try:
        legislatura, atto_num = _parse_camera_url(camera_url)
        c.legislatura = legislatura
        c.atto = f"C. {atto_num}"
    except ValueError as exc:
        print(f"    ✗ URL parse failed: {exc}")
        return legge

    # 1. RDF metadata
    try:
        rdf_data = _parse_rdf_metadata(session, legislatura, atto_num)
        c.atto_iri      = rdf_data.get("atto_iri", "")
        c.natura        = rdf_data.get("natura")
        c.iniziativa    = rdf_data.get("iniziativa")
        c.data_presentazione = rdf_data.get("data_presentazione")
        c.relazioni     = rdf_data.get("relazioni", [])
        c.firmatari     = rdf_data.get("firmatari", [])
        c.relatori      = rdf_data.get("relatori", [])
    except Exception as exc:
        legge.failures.append({"stage": "camera_rdf", "error": str(exc)[:200]})

    # 2. HTML metadata
    try:
        html = _fetch_html(session, camera_url)
        html_data = _parse_html_metadata(html, legislatura)
        c.votazione_finale = html_data.get("votazione_finale")
        c.dossier          = html_data.get("dossier", [])

        # Government bills: override firmatari from HTML (shows ministerial roles)
        if c.iniziativa == "Governo":
            html_firmatari = _parse_html_firmatari(html, legislatura)
            if html_firmatari:
                c.firmatari = html_firmatari
    except Exception as exc:
        legge.failures.append({"stage": "camera_html", "error": str(exc)[:200]})
        html = ""

    # 3. Interventi (Esame in Assemblea)
    interventi_dir = _compute_interventi_dir(legge, vault_dir)
    if interventi_dir and html:
        try:
            interventi_dir.mkdir(parents=True, exist_ok=True)
            esame_data = parse_esame_assemblea(html)

            if esame_data.get("sessions"):
                esame_data = enrich_with_stenografico_text(esame_data, fetch_xml=True)

                # Save JSON
                (interventi_dir / "esame_assemblea.json").write_text(
                    json.dumps(esame_data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                # Save per-seduta markdown files
                generate_markdown_files(esame_data, str(interventi_dir))

                # Convert to typed object and store on the model
                legge.camera.esame_assemblea = EsameAssemblea.from_dict(esame_data)

                sessions  = len(esame_data.get("sessions", []))
                interventions = sum(
                    len(p.get("interventions", []))
                    for s in esame_data.get("sessions", [])
                    for p in s.get("phases", [])
                )
                print(f"    interventi: {sessions} sedute, {interventions} interventi")
        except Exception as exc:
            legge.failures.append({"stage": "camera_interventi", "error": str(exc)[:200]})

    return legge


def enrich_all(leggi: list[Legge], vault_dir: Path) -> list[Legge]:
    """Enrich a list of Legge objects with Camera.it data.

    Shares one session across all requests. Applies a rate-limit delay
    between laws that have camera.it links.

    Args:
        leggi:     List of Legge (normattiva stage already complete).
        vault_dir: Root vault directory for interventi path computation.

    Returns:
        The same list, each Legge enriched in-place.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    for i, legge in enumerate(leggi):
        n = legge.normattiva
        has_camera = any("camera.it" in l for l in n.lavori_preparatori)

        print(f"  [{i+1}/{len(leggi)}] {n.codice_redazionale}...", end=" ", flush=True)

        if not has_camera:
            print("no camera.it link")
            continue

        enrich(legge, session, vault_dir)

        c = legge.camera
        info = f"leg={c.legislatura} {c.atto or '?'}"
        if c.iniziativa:
            info += f" ({c.iniziativa})"
        if c.firmatari:
            info += f" | {len(c.firmatari)} firmatari"
        if c.relatori:
            info += f" | relatori: {', '.join(c.relatori)}"
        print(info)

        time.sleep(_DELAY_BETWEEN_LAWS)

    return leggi


# ── CLI / test entrypoint ─────────────────────────────────────────────────────

def main() -> list[Legge]:
    """Test entrypoint: load from an enriched dump and enrich with camera data.

    Usage:
        python camera_enrich.py enriched_2026_01.json
        python camera_enrich.py enriched_2026_01.json --limit 2
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Enrich Legge objects with Camera.it metadata.",
    )
    parser.add_argument("dump", help="Path to enriched_YYYY_MM.json")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N laws with a camera.it link")
    parser.add_argument("--vault", default="../content/leggi",
                        help="Vault directory for interventi output (default: ../content/leggi)")
    args = parser.parse_args()

    dump_path = Path(args.dump)
    vault_dir = Path(args.vault).resolve()

    if not dump_path.exists():
        parser.error(f"Dump file not found: {dump_path}")

    with dump_path.open(encoding="utf-8") as f:
        data = json.load(f)

    # Reconstruct Legge objects from the raw_api data in the dump
    # (enriched dumps don't have raw_api, so fall back to leggi list)
    if "raw_api" in data:
        leggi = [Legge.from_api(raw) for raw in data["raw_api"]]
    else:
        # Re-build from the normattiva section of the enriched dump
        # (re-runs normattiva enrich would be needed for full data;
        #  for now load what's available from the dump)
        leggi = [Legge.from_api({
            "codiceRedazionale":  l["normattiva"]["codice_redazionale"],
            "descrizioneAtto":    l["normattiva"]["descrizione_atto"],
            "titoloAtto":         l["normattiva"]["titolo_atto"],
            "numeroProvvedimento":l["normattiva"]["numero_provvedimento"],
            "denominazioneAtto":  l["normattiva"]["denominazione_atto"],
            "dataGU":             l["normattiva"]["data_gu"],
            "numeroGU":           l["normattiva"]["numero_gu"],
            "dataEmanazione":     l["normattiva"]["data_emanazione"],
        }) for l in data["leggi"]]
        # Restore lavori_preparatori so enrich() knows which link to use
        for legge, l in zip(leggi, data["leggi"]):
            legge.normattiva.lavori_preparatori = l["normattiva"].get(
                "lavori_preparatori", []
            )

    # Apply limit to laws that have camera.it links
    if args.limit:
        camera_leggi = [l for l in leggi if any(
            "camera.it" in x for x in l.normattiva.lavori_preparatori
        )]
        leggi = camera_leggi[: args.limit]

    print("=" * 60)
    print(f"Camera enrich: {len(leggi)} laws")
    print("=" * 60 + "\n")

    enrich_all(leggi, vault_dir)

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)

    return leggi


if __name__ == "__main__":
    main()
