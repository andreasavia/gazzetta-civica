#!/usr/bin/env python3
"""
Fetch all dibattiti data for an atto using a SINGLE comprehensive SPARQL query.
This is the most efficient approach - one query to fetch everything.
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import requests


SPARQL_ENDPOINT = "http://dati.camera.it/sparql"


def build_atto_iri(input_str: str) -> str:
    """Convert various input formats to full atto IRI."""
    if input_str.startswith("http://"):
        return input_str
    if "_" in input_str:
        return f"http://dati.camera.it/ocd/attocamera.rdf/{input_str}"
    parts = input_str.split()
    if len(parts) == 2:
        legislatura, numero = parts
        return f"http://dati.camera.it/ocd/attocamera.rdf/ac{legislatura}_{numero}"
    raise ValueError(f"Cannot parse atto identifier: {input_str}")


def fetch_all_dibattiti_data(atto_iri: str, filter_title: str = None) -> Dict:
    """
    Fetch ALL data (metadata, dibattiti, discussioni, interventi) in ONE query.

    This comprehensive query traverses the entire graph:
    Atto → Dibattiti → Discussioni → Interventi → Deputies

    Args:
        atto_iri: The IRI of the atto
        filter_title: Optional filter to only include dibattiti with this exact title
    """

    # Build title filter clause
    title_filter = ""
    if filter_title:
        title_filter = f'FILTER(?dibattitoTitolo = "{filter_title}")'

    query = f"""
    PREFIX ocd: <http://dati.camera.it/ocd/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT DISTINCT
        ?attoTipo ?attoTitolo ?attoIniziativa ?attoDataPresentazione
        ?dibattito ?dibattitoTitolo ?dibattitoData
        ?discussione ?discussioneArgomento ?seduta ?dataSeduta
        ?intervento ?interventoLabel ?deputato ?deputatoNome ?deputatoCognome ?testoLink
    WHERE {{
        # Atto metadata
        <{atto_iri}> rdf:type ?attoTipo .
        OPTIONAL {{ <{atto_iri}> dc:title ?attoTitolo }}
        OPTIONAL {{ <{atto_iri}> ocd:iniziativa ?attoIniziativa }}
        OPTIONAL {{ <{atto_iri}> ocd:dataPresentazione ?attoDataPresentazione }}

        # Dibattiti linked to this atto
        <{atto_iri}> ocd:rif_dibattito ?dibattito .
        OPTIONAL {{ ?dibattito dc:title ?dibattitoTitolo }}
        OPTIONAL {{ ?dibattito dc:date ?dibattitoData }}

        # Filter by title if specified
        {title_filter}

        # Discussioni within each dibattito
        OPTIONAL {{
            ?dibattito ocd:rif_discussione ?discussione .
            OPTIONAL {{ ?discussione rdfs:label ?discussioneArgomento }}
            OPTIONAL {{
                ?discussione ocd:rif_seduta ?seduta .
                ?seduta dc:date ?dataSeduta
            }}

            # Interventi within each discussione
            OPTIONAL {{
                ?discussione ocd:rif_intervento ?intervento .
                OPTIONAL {{ ?intervento rdfs:label ?interventoLabel }}
                OPTIONAL {{ ?intervento dc:relation ?testoLink }}
                OPTIONAL {{
                    ?intervento ocd:rif_deputato ?deputato .
                    OPTIONAL {{ ?deputato foaf:firstName ?deputatoNome }}
                    OPTIONAL {{ ?deputato foaf:surname ?deputatoCognome }}
                }}
            }}
        }}
    }}
    ORDER BY ?dibattitoData ?dataSeduta
    """

    headers = {"Accept": "application/sparql-results+json"}
    params = {"query": query}

    print("Executing comprehensive SPARQL query...")
    print("(This single query fetches: metadata + dibattiti + discussioni + interventi)")
    print()

    try:
        response = requests.get(SPARQL_ENDPOINT, params=params, headers=headers, timeout=60)
        response.raise_for_status()

        result = response.json()
        bindings = result.get("results", {}).get("bindings", [])

        print(f"✓ Query successful: {len(bindings)} rows returned")
        print()

        return process_query_results(atto_iri, bindings)

    except requests.RequestException as e:
        print(f"Error executing SPARQL query: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}", file=sys.stderr)
        sys.exit(1)


def process_query_results(atto_iri: str, bindings: List[Dict]) -> Dict:
    """
    Process the flat SPARQL results into a structured hierarchy.

    Transforms flat rows into:
    {
        metadata: {...},
        dibattiti: [
            {
                dibattito: "...",
                discussioni: [
                    {
                        discussione: "...",
                        interventi: [...]
                    }
                ]
            }
        ]
    }
    """
    if not bindings:
        return {
            "metadata": {"atto_iri": atto_iri},
            "dibattiti": []
        }

    # Extract metadata from first row
    first_row = bindings[0]
    metadata = {
        "atto_iri": atto_iri,
        "atto_id": atto_iri.split("/")[-1],
        "tipo": first_row.get("attoTipo", {}).get("value"),
        "titolo": first_row.get("attoTitolo", {}).get("value"),
        "iniziativa": first_row.get("attoIniziativa", {}).get("value"),
        "dataPresentazione": first_row.get("attoDataPresentazione", {}).get("value")
    }

    # Build nested structure
    dibattiti_map = defaultdict(lambda: {
        "discussioni_map": defaultdict(lambda: {
            "interventi": []
        })
    })

    for row in bindings:
        # Dibattito level
        dib_iri = row.get("dibattito", {}).get("value")
        if not dib_iri:
            continue

        if "dibattito" not in dibattiti_map[dib_iri]:
            dibattiti_map[dib_iri]["dibattito"] = dib_iri
            dibattiti_map[dib_iri]["titolo"] = row.get("dibattitoTitolo", {}).get("value")
            dibattiti_map[dib_iri]["data"] = row.get("dibattitoData", {}).get("value")

        # Discussione level
        disc_iri = row.get("discussione", {}).get("value")
        if disc_iri:
            disc_map = dibattiti_map[dib_iri]["discussioni_map"][disc_iri]
            if "discussione" not in disc_map:
                disc_map["discussione"] = disc_iri
                disc_map["argomento"] = row.get("discussioneArgomento", {}).get("value")
                disc_map["seduta"] = row.get("seduta", {}).get("value")
                disc_map["dataSeduta"] = row.get("dataSeduta", {}).get("value")

            # Intervento level
            int_iri = row.get("intervento", {}).get("value")
            if int_iri:
                # Check if already added (avoid duplicates)
                if not any(i.get("intervento") == int_iri for i in disc_map["interventi"]):
                    intervento = {
                        "intervento": int_iri,
                        "label": row.get("interventoLabel", {}).get("value"),
                        "deputato": row.get("deputato", {}).get("value"),
                        "nome": row.get("deputatoNome", {}).get("value"),
                        "cognome": row.get("deputatoCognome", {}).get("value"),
                        "testo": row.get("testoLink", {}).get("value")
                    }
                    disc_map["interventi"].append(intervento)

    # Convert maps to lists and add counts
    dibattiti = []
    for dib_iri, dib_data in dibattiti_map.items():
        discussioni = []
        for disc_iri, disc_data in dib_data["discussioni_map"].items():
            discussioni.append({
                "discussione": disc_data["discussione"],
                "argomento": disc_data.get("argomento"),
                "seduta": disc_data.get("seduta"),
                "dataSeduta": disc_data.get("dataSeduta"),
                "interventi": disc_data["interventi"]
            })

        total_interventi = sum(len(d["interventi"]) for d in discussioni)

        dibattiti.append({
            "dibattito": dib_data["dibattito"],
            "titolo": dib_data.get("titolo"),
            "data": dib_data.get("data"),
            "discussioni": discussioni,
            "num_discussioni": len(discussioni),
            "num_interventi": total_interventi
        })

    return {
        "metadata": metadata,
        "dibattiti": dibattiti
    }


def main():
    parser = argparse.ArgumentParser(
        description="Fetch all dibattiti data for an atto using ONE comprehensive SPARQL query"
    )
    parser.add_argument(
        "atto",
        nargs="+",
        help="Atto identifier (e.g., 'ac19_1621', '19 1621', or full IRI)"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output JSON file (default: data/dibattiti/{atto_id}.single.json)"
    )
    parser.add_argument(
        "--filter-title",
        help='Filter dibattiti by exact title match (e.g., "Discussione in Assemblea")'
    )

    args = parser.parse_args()

    # Build atto IRI
    atto_input = " ".join(args.atto)
    atto_iri = build_atto_iri(atto_input)
    atto_id = atto_iri.split("/")[-1]

    print(f"Fetching dibattiti for: {atto_iri}")
    print(f"Atto ID: {atto_id}")
    if args.filter_title:
        print(f"Filtering by title: {args.filter_title}")
    print()

    # Execute single comprehensive query
    data = fetch_all_dibattiti_data(atto_iri, filter_title=args.filter_title)

    # Print summary
    metadata = data["metadata"]
    dibattiti = data["dibattiti"]

    print("=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    print()
    print("Atto Metadata:")
    print(f"  Tipo: {metadata.get('tipo', 'N/A')}")
    print(f"  Titolo: {metadata.get('titolo', 'N/A')[:80]}...")
    print(f"  Iniziativa: {metadata.get('iniziativa', 'N/A')}")
    print(f"  Data presentazione: {metadata.get('dataPresentazione', 'N/A')}")
    print()

    print(f"Found {len(dibattiti)} dibattiti")

    if dibattiti:
        print()
        print("Sample dibattiti:")
        for i, dib in enumerate(dibattiti[:5]):
            titolo = dib.get('titolo') or 'N/A'
            titolo_short = titolo[:60] + "..." if len(titolo) > 60 else titolo
            print(f"  {i+1}. {dib.get('data', 'N/A')} - {titolo_short}")
            print(f"     Discussioni: {dib.get('num_discussioni', 0)}, Interventi: {dib.get('num_interventi', 0)}")

        if len(dibattiti) > 5:
            print(f"  ... and {len(dibattiti) - 5} more")

        # Total counts
        total_discussioni = sum(d.get("num_discussioni", 0) for d in dibattiti)
        total_interventi = sum(d.get("num_interventi", 0) for d in dibattiti)

        print()
        print("Total counts:")
        print(f"  Dibattiti: {len(dibattiti)}")
        print(f"  Discussioni: {total_discussioni}")
        print(f"  Interventi: {total_interventi}")

    print()
    print("=" * 60)
    print()

    # Prepare output
    output_data = {
        **data,
        "fetched_at": datetime.now().isoformat(),
        "fetch_method": "single_sparql_query"
    }

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        output_path = f"data/dibattiti/{atto_id}.single.json"

    # Save to file
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"✓ Saved to: {output_path}")
    print()


if __name__ == "__main__":
    main()
