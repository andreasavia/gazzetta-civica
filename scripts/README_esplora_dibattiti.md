# Esplora Dibattiti / Explore Parliamentary Debates

This script explores dibattiti (debates) for a given Camera.it atto (parliamentary bill), showing where and when the bill was discussed.

## Usage

```bash
# Activate virtual environment
source venv/bin/activate

# Run with full RDF URL
python scripts/esplora_dibattiti.py http://dati.camera.it/ocd/attocamera.rdf/ac19_1621

# Run with short form
python scripts/esplora_dibattiti.py ac19_1621

# Run with legislature and bill number
python scripts/esplora_dibattiti.py 19 1621

# Specify custom output file
python scripts/esplora_dibattiti.py 19 1621 -o my_output.json
```

## Output

The script generates a JSON file with the following structure:

```json
{
  "metadata": {
    "atto_iri": "http://dati.camera.it/ocd/attocamera.rdf/ac19_1621",
    "legislatura": "19",
    "numero": "1621",
    "tipo": "Progetto di Legge",
    "titolo": "...",
    "iniziativa": "Parlamentare",
    "data_presentazione": "2023-12-19",
    "firmatari": [...]
  },
  "dibattiti": [
    {
      "dibattito_iri": "http://dati.camera.it/ocd/dibattito.rdf/dib177625_19",
      "data": "20241001-20250402",
      "titolo": "II Commissione (Giustizia)",
      "interventi": [],
      "num_interventi": 0
    },
    ...
  ],
  "votazioni": [...],
  "dibattiti_web": [...],
  "fetched_at": "2026-02-13T..."
}
```

## What is a Dibattito?

A **dibattito** represents a debate session where the bill was discussed. This can be:
- **Commission sessions** (e.g., "II Commissione (Giustizia)")
- **Assembly discussions** (e.g., "Discussione in Assemblea")
- **Committee reviews** (e.g., "Comitato per la legislazione")

Each dibattito has:
- An IRI (unique identifier)
- Date or date range when discussions occurred
- Title indicating which commission or body discussed the bill
- References to individual interventi (speeches) - if available in the data

## Example: ac19_1621

For bill 1621 from legislature 19, the script found **40 dibattiti** across:
- **Commissione Giustizia** (Justice Committee) - Multiple sessions from Oct 2024 to Apr 2025
- **Commissione Affari Costituzionali** (Constitutional Affairs Committee)
- **Commissione Finanze** (Finance Committee)
- **Commissione Ambiente** (Environment Committee)
- **Discussione in Assemblea** (Assembly Floor Debate) - Multiple sessions in Apr 2025
- **Comitato per la legislazione** (Legislative Committee)

## Features

- ✅ **Fetches dibattiti from RDF** - Extracts all `ocd:rif_dibattito` references from atto
- ✅ **Detailed debate metadata** - Date ranges, commission names, session types
- ✅ **Basic atto metadata** - Type, title, signers, presentation date
- ✅ **Multiple input formats** - URL, short form, or separate arguments
- ✅ **Structured JSON output** - Complete data for further analysis
- ✅ **Human-readable summary** - Console output with key information

## Data Sources

1. **Atto RDF** (`http://dati.camera.it/ocd/attocamera.rdf/`) - Contains `ocd:rif_dibattito` references
2. **Dibattito RDF** (`http://dati.camera.it/ocd/dibattito.rdf/`) - Detailed debate session information
3. **SPARQL Endpoint** (optional) - Additional voting data
4. **Web Interface** (optional) - Supplementary information

## Output Location

Files are saved to `data/dibattiti/ac{legislature}_{number}.json`

Example: `data/dibattiti/ac19_1621.json`

## Notes

- Dibattiti group related discussions by commission/session
- Individual interventi (speeches) may not always be included in dibattito RDF
- For detailed speech transcripts, check the web interface links
- Some fields may be empty depending on data availability
- The script handles both successful and partial data retrievals gracefully
