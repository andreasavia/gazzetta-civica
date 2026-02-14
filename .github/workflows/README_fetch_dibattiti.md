# Fetch Dibattiti Workflow

This GitHub Actions workflow automatically fetches parliamentary debate data (dibattiti) for a specific atto and saves it to the corresponding folder in the repository.

## Overview

The workflow:
1. Takes an atto IRI as input (manual trigger)
2. Fetches dibattiti data using SPARQL queries
3. Finds the corresponding markdown file in `content/leggi/` by matching the `camera-atto-iri` field
4. Saves the output as `dibattiti.json` in that folder
5. Creates a pull request for review

## Usage

### Triggering the Workflow

1. Go to **Actions** tab in GitHub
2. Select **"Fetch Dibattiti for Atto"** workflow
3. Click **"Run workflow"**
4. Fill in the inputs:
   - **atto_iri**: (Required) The atto identifier in any of these formats:
     - Full IRI: `http://dati.camera.it/ocd/attocamera.rdf/ac19_1621`
     - Short format: `ac19_1621`
     - Space-separated: `19 1621`
   - **filter_title**: (Optional) Filter dibattiti by exact title match
     - Example: `Discussione in Assemblea`
     - Leave empty to fetch all dibattiti

### Examples

#### Fetch all dibattiti for an atto:
- **atto_iri**: `ac19_1621`
- **filter_title**: (leave empty)

**Result**: Fetches all 40 dibattiti with 615 interventi

#### Fetch only "Discussione in Assemblea" dibattiti:
- **atto_iri**: `ac19_1621`
- **filter_title**: `Discussione in Assemblea`

**Result**: Fetches only 7 dibattiti matching the title with 308 interventi

## Output

The workflow saves a JSON file at:
```
content/leggi/YYYY/MM/DD/n. X/dibattiti.json
```

Where the folder is determined by finding the markdown file with the matching `camera-atto-iri` field.

### JSON Structure

```json
{
  "metadata": {
    "atto_iri": "http://dati.camera.it/ocd/attocamera.rdf/ac19_1621",
    "atto_id": "ac19_1621",
    "tipo": "http://dati.camera.it/ocd/atto",
    "titolo": "FOTI: \"Modifiche alla legge...",
    "iniziativa": "Parlamentare",
    "dataPresentazione": null
  },
  "dibattiti": [
    {
      "dibattito": "http://dati.camera.it/ocd/dibattito.rdf/dib10193_19",
      "titolo": "Discussione in Assemblea",
      "data": null,
      "discussioni": [
        {
          "discussione": "http://dati.camera.it/ocd/discussione.rdf/disc_10193.19.2024-01-31",
          "argomento": "C.1621 - Modifiche alla legge 14 gennaio 1994...",
          "seduta": "http://dati.camera.it/ocd/seduta.rdf/sed395",
          "dataSeduta": "2024-01-31",
          "interventi": [
            {
              "intervento": "http://dati.camera.it/ocd/intervento.rdf/int_s395_4",
              "deputato": "http://dati.camera.it/ocd/deputato.rdf/d35889",
              "nome": "Deborah",
              "cognome": "Bergamini",
              "testo": "https://www.camera.it/leg19/410?idSeduta=395&tipo=html"
            }
          ]
        }
      ],
      "num_discussioni": 1,
      "num_interventi": 1
    }
  ],
  "fetched_at": "2026-02-14T01:15:30.123456",
  "fetch_method": "single_sparql_query"
}
```

## How It Works

### 1. Fetch Dibattiti Data

Uses the optimized single-query SPARQL script:
```bash
python scripts/fetch_dibattiti_single_query.py <atto_iri> [--filter-title "..."]
```

This script:
- Executes ONE comprehensive SPARQL query (not 50-100 separate queries)
- Fetches metadata, dibattiti, discussioni, interventi, and deputies in a single request
- Performance: ~2-5 seconds for complete data

### 2. Find Target Folder

Uses the helper script:
```bash
python scripts/find_atto_folder.py <atto_iri>
```

This script:
- Searches all markdown files in `content/leggi/`
- Matches the `camera-atto-iri` frontmatter field
- Returns the folder path where the markdown file is located

### 3. Create Pull Request

- Moves the JSON file to the target folder as `dibattiti.json`
- Creates a new branch: `dibattiti/ac19_1621-<run_number>`
- Commits with message: `Add dibattiti data for ac19_1621`
- Opens a pull request with:
  - Title: `Add dibattiti data for ac19_1621`
  - Body: Summary of atto, filter used, and target folder
  - Labels: `dibattiti`, `automated`
  - Assignee: @andreasavia

## Requirements

The workflow requires:
- Python 3.11+
- `requests` library (installed automatically)
- Markdown files in `content/leggi/` with `camera-atto-iri` field
- Write permissions for contents and pull requests

## Local Testing

You can test the workflow locally:

```bash
# 1. Activate virtual environment
source venv/bin/activate

# 2. Fetch dibattiti data
python scripts/fetch_dibattiti_single_query.py ac19_1621 \
  --filter-title "Discussione in Assemblea" \
  -o /tmp/dibattiti.json

# 3. Find target folder
target_folder=$(python scripts/find_atto_folder.py ac19_1621)
echo "Target: $target_folder"

# 4. Copy to target
cp /tmp/dibattiti.json "$target_folder/dibattiti.json"
```

## Troubleshooting

### Error: "Could not find folder for atto IRI"

**Cause**: No markdown file in `content/leggi/` has the specified `camera-atto-iri` in its frontmatter.

**Solution**:
- Verify the atto IRI is correct
- Check that a markdown file exists with the matching `camera-atto-iri` field
- Ensure the markdown file has valid frontmatter with the field

### Error: "SPARQL query failed"

**Cause**: The SPARQL endpoint at `http://dati.camera.it/sparql` is unavailable or the query timed out.

**Solution**:
- Check if the endpoint is accessible
- Try again later
- Verify the atto IRI exists in the Camera.it dataset

## Related Scripts

- [`fetch_dibattiti_single_query.py`](../../scripts/fetch_dibattiti_single_query.py) - Main script for fetching dibattiti
- [`find_atto_folder.py`](../../scripts/find_atto_folder.py) - Helper script to locate the correct folder
- [`QUERY_SUMMARY.md`](../../scripts/QUERY_SUMMARY.md) - Documentation of SPARQL query approaches

## Performance

- **Network requests**: 1 SPARQL query (vs 50-100 in legacy approach)
- **Response time**: ~2-5 seconds for complete data
- **Data completeness**: Full hierarchy with all interventi and deputy information
