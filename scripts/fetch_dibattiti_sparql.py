#!/usr/bin/env python3
"""
Fetch dibattiti (parliamentary debates) for a given atto using SPARQL queries.
This is more efficient than web scraping as it queries the RDF triplestore directly.
"""

import argparse
import json
import sys
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import quote

import requests


SPARQL_ENDPOINT = "http://dati.camera.it/sparql"


def build_atto_iri(input_str: str) -> str:
    """
    Convert various input formats to full atto IRI.

    Examples:
        - "ac19_1621" -> "http://dati.camera.it/ocd/attocamera.rdf/ac19_1621"
        - "19 1621" -> "http://dati.camera.it/ocd/attocamera.rdf/ac19_1621"
        - "http://dati.camera.it/ocd/attocamera.rdf/ac19_1621" -> unchanged
    """
    if input_str.startswith("http://"):
        return input_str

    if "_" in input_str:
        # Format: ac19_1621
        return f"http://dati.camera.it/ocd/attocamera.rdf/{input_str}"

    # Assume format: "19 1621"
    parts = input_str.split()
    if len(parts) == 2:
        legislatura, numero = parts
        return f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{numero}"

    raise ValueError(f"Cannot parse atto identifier: {input_str}")


def execute_sparql_query(query: str, debug: bool = False) -> List[Dict]:
    """Execute SPARQL query and return results as list of dicts."""
    headers = {
        "Accept": "application/sparql-results+json",
    }

    params = {"query": query}

    try:
        # Use GET request (Camera.it SPARQL endpoint prefers GET)
        response = requests.get(SPARQL_ENDPOINT, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        if debug:
            print(f"Response status: {response.status_code}")
            print(f"Response headers: {response.headers}")
            print(f"Response text (first 500 chars): {response.text[:500]}")

        result = response.json()
        bindings = result.get("results", {}).get("bindings", [])

        # Convert bindings to simple dict format
        simplified = []
        for binding in bindings:
            row = {}
            for key, value in binding.items():
                row[key] = value.get("value")
            simplified.append(row)

        return simplified

    except requests.RequestException as e:
        print(f"Error executing SPARQL query: {e}", file=sys.stderr)
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}", file=sys.stderr)
        return []


def fetch_atto_metadata(atto_iri: str) -> Optional[Dict]:
    """
    Fetch basic atto metadata using SPARQL.
    """
    query = f"""
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?tipo ?titolo ?iniziativa ?dataPresentazione
    WHERE {{
        <{atto_iri}> rdf:type ?tipo .
        OPTIONAL {{ <{atto_iri}> dc:title ?titolo }}
        OPTIONAL {{ <{atto_iri}> ocd:iniziativa ?iniziativa }}
        OPTIONAL {{ <{atto_iri}> ocd:dataPresentazione ?dataPresentazione }}
    }}
    LIMIT 1
    """

    results = execute_sparql_query(query)

    if not results:
        return None

    return results[0]


def fetch_dibattiti_for_atto(atto_iri: str) -> List[Dict]:
    """
    Fetch all dibattiti (debates) associated with this atto.

    The atto directly references dibattiti via ocd:rif_dibattito.
    """
    query = f"""
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?dibattito ?titolo ?data
    WHERE {{
        <{atto_iri}> ocd:rif_dibattito ?dibattito .

        OPTIONAL {{ ?dibattito dc:title ?titolo }}
        OPTIONAL {{ ?dibattito dc:date ?data }}
    }}
    ORDER BY ?data
    """

    results = execute_sparql_query(query)

    print(f"Found {len(results)} dibattiti for atto")
    return results


def fetch_discussioni_for_dibattito(dibattito_iri: str) -> List[Dict]:
    """
    Fetch discussioni (discussions) within a dibattito.
    """
    query = f"""
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?discussione ?argomento ?seduta ?dataSeduta
    WHERE {{
        <{dibattito_iri}> ocd:rif_discussione ?discussione .

        OPTIONAL {{ ?discussione rdfs:label ?argomento }}
        OPTIONAL {{
            ?discussione ocd:rif_seduta ?seduta .
            ?seduta dc:date ?dataSeduta
        }}
    }}
    ORDER BY ?dataSeduta
    """

    results = execute_sparql_query(query)
    return results


def fetch_interventi_for_discussione(discussione_iri: str) -> List[Dict]:
    """
    Fetch all interventi (speeches) for a specific discussione.
    """
    query = f"""
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?intervento ?deputato ?nome ?cognome ?testo
    WHERE {{
        <{discussione_iri}> ocd:rif_intervento ?intervento .

        OPTIONAL {{
            ?intervento ocd:rif_deputato ?deputato .
            OPTIONAL {{ ?deputato foaf:firstName ?nome }}
            OPTIONAL {{ ?deputato foaf:surname ?cognome }}
        }}
        OPTIONAL {{ ?intervento dc:relation ?testo }}
    }}
    """

    results = execute_sparql_query(query)
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Fetch dibattiti for an atto using SPARQL queries"
    )
    parser.add_argument(
        "atto",
        nargs="+",
        help="Atto identifier (e.g., 'ac19_1621', '19 1621', or full IRI)"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output JSON file (default: data/dibattiti/{atto_id}.json)"
    )
    parser.add_argument(
        "--fetch-interventi",
        action="store_true",
        help="Also fetch interventi (speeches) for each discussione (slower)"
    )

    args = parser.parse_args()

    # Build atto IRI
    atto_input = " ".join(args.atto)
    atto_iri = build_atto_iri(atto_input)

    # Extract atto ID for filename
    atto_id = atto_iri.split("/")[-1]

    print(f"Fetching dibattiti for: {atto_iri}")
    print(f"Atto ID: {atto_id}")
    print()

    # Step 1: Fetch atto metadata
    print("Step 1: Fetching atto metadata...")
    metadata = fetch_atto_metadata(atto_iri)

    if not metadata:
        print(f"Error: Could not fetch metadata for {atto_iri}", file=sys.stderr)
        sys.exit(1)

    print(f"  Tipo: {metadata.get('tipo', 'N/A')}")
    print(f"  Titolo: {metadata.get('titolo', 'N/A')[:80]}...")
    print(f"  Iniziativa: {metadata.get('iniziativa', 'N/A')}")
    print(f"  Data presentazione: {metadata.get('dataPresentazione', 'N/A')}")
    print()

    # Step 2: Fetch dibattiti
    print("Step 2: Fetching dibattiti...")
    dibattiti = fetch_dibattiti_for_atto(atto_iri)

    if not dibattiti:
        print("No dibattiti found for this atto")
    else:
        print(f"Found {len(dibattiti)} dibattiti")
        print()

        # Print sample
        for i, dib in enumerate(dibattiti[:5]):
            print(f"  {i+1}. {dib.get('data', 'N/A')} - {dib.get('titolo', 'N/A')[:60]}...")

        if len(dibattiti) > 5:
            print(f"  ... and {len(dibattiti) - 5} more")
        print()

    # Step 3: Optionally fetch discussioni and interventi
    if args.fetch_interventi and dibattiti:
        print("Step 3: Fetching discussioni and interventi for each dibattito...")
        for i, dib in enumerate(dibattiti):
            # Get discussioni within this dibattito
            discussioni = fetch_discussioni_for_dibattito(dib['dibattito'])
            dib['discussioni'] = discussioni

            # Get interventi for each discussione
            total_interventi = 0
            for disc in discussioni:
                interventi = fetch_interventi_for_discussione(disc['discussione'])
                disc['interventi'] = interventi
                total_interventi += len(interventi)

            dib['num_discussioni'] = len(discussioni)
            dib['num_interventi'] = total_interventi

            if i < 3:
                print(f"  Dibattito {i+1}: {len(discussioni)} discussioni, {total_interventi} interventi")
        print()

    # Prepare output
    output_data = {
        "metadata": {
            "atto_iri": atto_iri,
            "atto_id": atto_id,
            **metadata
        },
        "dibattiti": dibattiti,
        "fetched_at": datetime.now().isoformat(),
        "fetch_method": "sparql"
    }

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        output_path = f"data/dibattiti/{atto_id}.sparql.json"

    # Save to file
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"✓ Saved to: {output_path}")
    print(f"✓ Total dibattiti: {len(dibattiti)}")


if __name__ == "__main__":
    main()
