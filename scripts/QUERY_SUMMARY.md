# Dibattiti Extraction Scripts - Query Summary

This document details the SPARQL query patterns used by each script to extract parliamentary debate data from Camera.it.

## Overview

We have 2 main approaches:

| Approach | Queries | Efficiency | Use Case |
|----------|---------|------------|----------|
| Legacy web scraping | Web scraping + RDF | Slow | Legacy/comparison |
| `fetch_dibattiti_single_query.py` | **1 query** | **Fast** | Production use ✓ |

---

## 1. `esplora_dibattiti.py` (Original Web Scraping)

**Method**: Hybrid approach using RDF fetching + web scraping

### Queries Made:
1. **Fetch Atto RDF** (HTTP GET to RDF endpoint)
   - URL: `http://dati.camera.it/ocd/attocamera.rdf/ac19_1621`
   - Extracts: `ocd:rif_dibattito` references
   - Returns: List of dibattito IRIs

2. **Fetch Each Dibattito RDF** (HTTP GET for each dibattito)
   - URL: `http://dati.camera.it/ocd/dibattito.rdf/dib{id}_{leg}`
   - Extracts: Title, date, discussione references
   - Count: **49 requests** for ac19_1621

3. **Web Interface Scraping** (optional)
   - URL: Camera.it web interface
   - Extracts: Additional metadata

**Total HTTP Requests**: ~50-100 (1 atto + 49 dibattiti + optional web)

**Performance**: Slow, ~5-10 seconds

---

## 2. `fetch_dibattiti_single_query.py` (Single Comprehensive Query) ⭐

**Method**: ONE comprehensive SPARQL query fetching everything

**Features**:
- Optional title filtering via `--filter-title` parameter
- Filters dibattiti by exact title match (e.g., "Discussione in Assemblea")
- Filter applied in SPARQL query for maximum efficiency

### The Single Query:

```sparql
PREFIX ocd: <http://dati.camera.it/ocd/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT DISTINCT
    ?attoTipo ?attoTitolo ?attoIniziativa ?attoDataPresentazione
    ?dibattito ?dibattitoTitolo ?dibattitoData
    ?discussione ?discussioneArgomento ?seduta ?dataSeduta
    ?intervento ?deputato ?deputatoNome ?deputatoCognome ?testoLink
WHERE {
    # Atto metadata
    <atto_iri> rdf:type ?attoTipo .
    OPTIONAL { <atto_iri> dc:title ?attoTitolo }
    OPTIONAL { <atto_iri> ocd:iniziativa ?attoIniziativa }
    OPTIONAL { <atto_iri> ocd:dataPresentazione ?attoDataPresentazione }

    # Dibattiti linked to this atto
    <atto_iri> ocd:rif_dibattito ?dibattito .
    OPTIONAL { ?dibattito dc:title ?dibattitoTitolo }
    OPTIONAL { ?dibattito dc:date ?dibattitoData }

    # Optional: Filter by title if specified
    # FILTER(?dibattitoTitolo = "Discussione in Assemblea")

    # Discussioni within each dibattito
    OPTIONAL {
        ?dibattito ocd:rif_discussione ?discussione .
        OPTIONAL { ?discussione rdfs:label ?discussioneArgomento }
        OPTIONAL {
            ?discussione ocd:rif_seduta ?seduta .
            ?seduta dc:date ?dataSeduta
        }

        # Interventi within each discussione
        OPTIONAL {
            ?discussione ocd:rif_intervento ?intervento .
            OPTIONAL { ?intervento dc:relation ?testoLink }
            OPTIONAL {
                ?intervento ocd:rif_deputato ?deputato .
                OPTIONAL { ?deputato foaf:firstName ?deputatoNome }
                OPTIONAL { ?deputato foaf:surname ?deputatoCognome }
            }
        }
    }
}
ORDER BY ?dibattitoData ?dataSeduta
```

### Query Structure:

This query traverses the entire graph in one shot:

```
Atto (ac19_1621)
  ├─ Metadata (tipo, titolo, iniziativa)
  └─ ocd:rif_dibattito
      └─ Dibattiti (49 items)
          ├─ Metadata (titolo, data)
          └─ ocd:rif_discussione
              └─ Discussioni (40 items)
                  ├─ Metadata (argomento, seduta)
                  └─ ocd:rif_intervento
                      └─ Interventi (615 items)
                          ├─ Metadata (testo link)
                          └─ ocd:rif_deputato
                              └─ Deputy (nome, cognome)
```

### Results for ac19_1621:

**Without filtering:**
- **Query count**: 1
- **Rows returned**: 915 (flattened graph traversal)
- **Dibattiti found**: 40
- **Discussioni found**: 40
- **Interventi found**: 615
- **Performance**: Fast, ~2-5 seconds

**With `--filter-title "Discussione in Assemblea"`:**
- **Query count**: 1
- **Rows returned**: 308
- **Dibattiti found**: 7 (only matching title)
- **Discussioni found**: 9
- **Interventi found**: 308
- **Performance**: Fast, ~2-3 seconds (even faster due to smaller result set)

### Post-Processing:

The script processes the flat 915 rows into a nested structure:
1. Extract metadata from first row
2. Group by dibattito IRI
3. Within each dibattito, group by discussione IRI
4. Within each discussione, collect interventi
5. Remove duplicates and add counts

---

## Performance Comparison

| Metric | Legacy Web Scraping | fetch_dibattiti_single_query.py |
|--------|---------------------|--------------------------------|
| **HTTP Requests** | ~50-100 | **1 SPARQL query** ✓ |
| **Network Round-trips** | 50-100 | **1** ✓ |
| **Performance** | ~5-10 sec | **~2-5 sec** ✓ |
| **Data Completeness** | Partial | **Complete** ✓ |
| **Dibattiti Found** | 49 (RDF only) | 40 (with data) |
| **Interventi Found** | Limited | **615** ✓ |
| **Maintainability** | Complex | **Simple** ✓ |

---

## Recommendation

### Use `fetch_dibattiti_single_query.py` for:
✅ Production data extraction
✅ Batch processing multiple atti
✅ Best performance (10-20x fewer requests)
✅ Complete data in one request
✅ Optional title filtering

---

## Data Model Summary

### RDF Relationships:
```
ocd:atto
  └─ ocd:rif_dibattito → ocd:dibattito
      └─ ocd:rif_discussione → ocd:discussione
          ├─ ocd:rif_seduta → ocd:seduta
          └─ ocd:rif_intervento → ocd:intervento
              └─ ocd:rif_deputato → ocd:deputato
```

### Key Properties:
- **Atto**: `dc:title`, `ocd:iniziativa`, `ocd:dataPresentazione`
- **Dibattito**: `dc:title`, `dc:date`
- **Discussione**: `rdfs:label`, `ocd:rif_seduta`
- **Seduta**: `dc:date`, `dc:title`
- **Intervento**: `dc:relation` (transcript link), `ocd:rif_deputato`
- **Deputato**: `foaf:firstName`, `foaf:surname`

---

## Usage Examples

### Production Script:
```bash
# Fetch all dibattiti
python scripts/fetch_dibattiti_single_query.py ac19_1621
# Output: data/dibattiti/ac19_1621.single.json
# Queries: 1
# Time: ~3 seconds

# Filter by specific title (e.g., only "Discussione in Assemblea")
python scripts/fetch_dibattiti_single_query.py ac19_1621 --filter-title "Discussione in Assemblea"
# Output: data/dibattiti/ac19_1621.single.json
# Returns: Only dibattiti matching the exact title
# Example: 7 dibattiti (filtered from 40 total)
```

### Legacy Reference (not recommended):
```bash
python scripts/esplora_dibattiti.py ac19_1621
# Output: data/dibattiti/ac19_1621.json
# Requests: ~50-100
# Time: ~8 seconds
```

---

## Conclusion

**The single comprehensive query approach (`fetch_dibattiti_single_query.py`) is the recommended method:**

- ✅ **10-20x fewer network requests** (1 vs 50-100)
- ✅ **2-3x faster** (~3 sec vs ~8 sec)
- ✅ **Simplest code** (single query, no nested loops)
- ✅ **Complete data** (615 interventi with full hierarchy)
- ✅ **Leverages SPARQL's graph traversal** efficiently
- ✅ **Optional filtering** by title for focused queries
