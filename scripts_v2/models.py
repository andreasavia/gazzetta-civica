"""
models.py — Core data models for the Gazzetta Civica pipeline.

`Legge` is the single object passed between pipeline stages. It is composed
of three nested dataclasses, one per data source:

    NormattivaData  ← populated by normattiva_search + normattiva_enrich
    CameraData      ← populated by camera_enrich
    SenatoData      ← populated by senato_enrich

Pipeline flow:
    normattiva_search  →  Legge(normattiva=NormattivaData(...))
    normattiva_enrich  →  fills normattiva.uri, vigenza, approfondimenti, full_text_html
    camera_enrich      →  fills legge.camera.*
    senato_enrich      →  fills legge.senato.*
    markdown_writer    →  reads all three sub-objects and writes .md to vault
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ── NormattivaData ────────────────────────────────────────────────────────────

@dataclass
class NormattivaData:
    """All data sourced from normattiva.it.

    Populated in two stages:
      - Stage 1 (normattiva_search):  core fields from the /ricerca/avanzata API
      - Stage 2 (normattiva_enrich):  uri, vigenza, approfondimenti, full text
    """

    # ── Stage 1: API search ───────────────────────────────────────────────────
    codice_redazionale: str
    descrizione_atto: str
    titolo_atto: str
    numero_provvedimento: str
    denominazione_atto: str     # e.g. "LEGGE", "DECRETO-LEGGE"
    data_gu: str                # GU publication date (YYYY-MM-DD)
    numero_gu: str
    data_emanazione: str        # Emanation date (YYYY-MM-DD) — used for vault folder

    # GU supplement info (present for laws published in a Supplemento Ordinario/Straordinario)
    numero_supplemento: Optional[str] = None    # e.g. "4"
    tipo_supplemento: Optional[str] = None      # e.g. "SO" (Ordinario) / "SS" (Straordinario)
    tipo_supplemento_it: Optional[str] = None   # e.g. "Suppl. Ordinario n. 4"

    # Modification tracking (may be None if law has never been modified)
    data_ultima_modifica: Optional[str] = None
    ultimi_atti_modificanti: Optional[str] = None

    # ── Stage 2: enrichment ───────────────────────────────────────────────────
    uri: str = ""               # Full URN-NIR permalink (includes !vig= date)
    data_vigenza: str = ""      # Entry-into-force date (YYYY-MM-DD)
    gu_link: str = ""           # Link to gazzettaufficiale.it

    # Approfondimenti (links from the N2Ls page)
    atti_aggiornati: list[str] = field(default_factory=list)
    atti_correlati: list[str] = field(default_factory=list)
    lavori_preparatori: list[str] = field(default_factory=list)   # camera.it / senato.it links
    lavori_preparatori_testo: str = ""                            # raw text fallback (when no links)
    aggiornamenti_atto: list[str] = field(default_factory=list)
    note_atto: list[str] = field(default_factory=list)
    relazioni: list[str] = field(default_factory=list)
    aggiornamenti_titolo: list[str] = field(default_factory=list)
    aggiornamenti_struttura: list[str] = field(default_factory=list)
    atti_parlamentari: list[str] = field(default_factory=list)
    atti_attuativi: list[str] = field(default_factory=list)

    # Full act text (HTML from /esporta/attoCompleto)
    full_text_html: str = ""

    @classmethod
    def from_api(cls, raw: dict) -> "NormattivaData":
        """Construct from a raw /ricerca/avanzata API response dict.

        Args:
            raw: A single item from the `listaAtti` API response list.

        Returns:
            NormattivaData populated with all available API fields.
        """
        # Reconstruct emanation date from day/month/year parts
        # (more reliable than slicing dataEmanazione which has a time suffix)
        giorno = raw.get("giornoProvvedimento", "")
        mese = raw.get("meseProvvedimento", "")
        anno = raw.get("annoProvvedimento", "")
        if giorno and mese and anno:
            data_emanazione = f"{anno}-{int(mese):02d}-{int(giorno):02d}"
        else:
            data_emanazione = raw.get("dataEmanazione", "")[:10]

        return cls(
            codice_redazionale=raw.get("codiceRedazionale", "unknown"),
            descrizione_atto=raw.get("descrizioneAtto", ""),
            titolo_atto=raw.get("titoloAtto", "").strip().strip("[]").strip(),
            numero_provvedimento=str(raw.get("numeroProvvedimento", "0")),
            denominazione_atto=raw.get("denominazioneAtto", ""),
            data_gu=raw.get("dataGU", ""),
            numero_gu=str(raw.get("numeroGU", "")),
            data_emanazione=data_emanazione,
            numero_supplemento=str(raw["numeroSupplemento"]) if raw.get("numeroSupplemento") else None,
            tipo_supplemento=raw.get("tipoSupplemento") or None,
            tipo_supplemento_it=raw.get("tipoSupplementoIt", "").strip() or None,
            data_ultima_modifica=raw.get("dataUltimaModifica") or None,
            ultimi_atti_modificanti=raw.get("ultimiAttiModificanti") or None,
        )


# ── CameraData ────────────────────────────────────────────────────────────────

@dataclass
class CameraData:
    """All data sourced from camera.it (parliamentary lower house).

    Populated by the camera_enrich pipeline stage.
    """
    legislatura: Optional[str] = None
    atto: Optional[str] = None              # e.g. "A.C. 1234"
    atto_iri: Optional[str] = None          # Linked data IRI
    natura: Optional[str] = None            # e.g. "Disegno di Legge"
    iniziativa: Optional[str] = None        # e.g. "Governativa"
    data_presentazione: Optional[str] = None
    relazioni: list[str] = field(default_factory=list)
    firmatari: list[dict] = field(default_factory=list)   # [{name, role/group}, ...]
    relatori: list[str] = field(default_factory=list)
    votazione_finale: Optional[str] = None
    dossier: list[str] = field(default_factory=list)


# ── SenatoData ────────────────────────────────────────────────────────────────

@dataclass
class SenatoData:
    """All data sourced from senato.it (parliamentary upper house).

    Populated by the senato_enrich pipeline stage.
    """
    did: Optional[str] = None               # Internal Senato document ID
    legislatura: Optional[str] = None
    numero_fase: Optional[str] = None       # DDL number / phase
    url: Optional[str] = None
    titolo: Optional[str] = None
    titolo_breve: Optional[str] = None
    natura: Optional[str] = None
    iniziativa: Optional[str] = None
    data_presentazione: Optional[str] = None
    teseo: list[str] = field(default_factory=list)      # Subject index terms
    votazioni_url: Optional[str] = None
    votazione_finale: Optional[str] = None
    documenti: list[str] = field(default_factory=list)


# ── Legge (top-level container) ───────────────────────────────────────────────

@dataclass
class Legge:
    """Top-level container for a single Italian law across all data sources.

    Compose one Legge per law in the pipeline. Each enrichment stage fills
    a different nested object without touching the others.

    Example:
        legge = Legge.from_api(raw_dict)
        # Stage 2:
        legge.normattiva.uri = "https://..."
        legge.normattiva.full_text_html = "..."
        # Stage 3:
        legge.camera.legislatura = "19"
        # Stage 4:
        legge.senato.did = "12345"
    """
    normattiva: NormattivaData
    camera: CameraData = field(default_factory=CameraData)
    senato: SenatoData = field(default_factory=SenatoData)

    # Pipeline-internal flags (not written to markdown)
    interventi_error: Optional[str] = None

    @classmethod
    def from_api(cls, raw: dict) -> "Legge":
        """Construct a Legge from a raw Normattiva API atto dict.

        Args:
            raw: A single item from the `listaAtti` API response list.

        Returns:
            Legge with normattiva populated; camera and senato are empty defaults.
        """
        return cls(normattiva=NormattivaData.from_api(raw))

    def __repr__(self) -> str:
        n = self.normattiva
        return (
            f"Legge({n.denominazione_atto} n. {n.numero_provvedimento} "
            f"del {n.data_emanazione}, codice={n.codice_redazionale})"
        )
