#!/usr/bin/env python3
"""Quick test of the SPARQL endpoint"""

import requests

SPARQL_ENDPOINT = "http://dati.camera.it/sparql"

# Test query - look for our specific atto
atto_iri = "http://dati.camera.it/ocd/attocamera.rdf/ac19_1621"

query = f"""
PREFIX ocd: <http://dati.camera.it/ocd/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>

SELECT ?p ?o
WHERE {{
    <{atto_iri}> ?p ?o .
}}
LIMIT 20
"""

print("Testing SPARQL endpoint...")
print(f"Endpoint: {SPARQL_ENDPOINT}")
print()

# Try POST
headers = {
    "Accept": "application/sparql-results+json",
}

params = {"query": query}

try:
    print("Trying GET request...")
    response = requests.get(SPARQL_ENDPOINT, params=params, headers=headers, timeout=30)
    print(f"Status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type')}")
    print(f"Response length: {len(response.text)}")
    print()

    if response.status_code == 200:
        print("First 500 chars of response:")
        print(response.text[:500])
        print()

        try:
            data = response.json()
            results = data.get("results", {}).get("bindings", [])
            print(f"✓ Got {len(results)} results")
            if results:
                print("First result:", results[0])
        except Exception as e:
            print(f"Error parsing JSON: {e}")
    else:
        print(f"Error: {response.text[:500]}")

except Exception as e:
    print(f"Request failed: {e}")
