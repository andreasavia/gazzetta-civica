#!/usr/bin/env python3
"""Test what relations exist for ac19_1621"""

import requests

SPARQL_ENDPOINT = "http://dati.camera.it/sparql"
atto_iri = "http://dati.camera.it/ocd/attocamera.rdf/ac19_1621"

# Get all properties and objects
query = f"""
PREFIX ocd: <http://dati.camera.it/ocd/>

SELECT DISTINCT ?p
WHERE {{
    <{atto_iri}> ?p ?o .
}}
"""

headers = {"Accept": "application/sparql-results+json"}
params = {"query": query}

response = requests.get(SPARQL_ENDPOINT, params=params, headers=headers, timeout=30)
data = response.json()
bindings = data.get("results", {}).get("bindings", [])

print(f"Properties of {atto_iri}:")
print()
for b in bindings:
    p = b['p']['value']
    # Shorten the URI
    if 'dati.camera.it/ocd/' in p:
        p_short = 'ocd:' + p.split('/ocd/')[-1]
    elif 'purl.org/dc/elements/1.1/' in p:
        p_short = 'dc:' + p.split('/dc/elements/1.1/')[-1]
    elif 'rdf-syntax-ns#' in p:
        p_short = 'rdf:' + p.split('#')[-1]
    else:
        p_short = p
    print(f"  {p_short}")
