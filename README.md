# Gazzetta Civica

Analisi e approfondimenti sulla legislazione italiana.

## Structure

- `content/leggi/` - Legislative data (auto-generated)
- `content/articoli/` - Articles (human-written)
- `scripts/` - Data collection scripts
- `src/` - Astro frontend

## Development 

```bash
# Install dependencies
npm install

# Run dev server
npm run dev

# Build for production
npm run build
```

## Data Collection

The `scripts/ricerca_normattiva.py` script fetches legislative data from the Normattiva API.

### API Details

- **Endpoint**: `POST https://api.normattiva.it/t/normattiva.api/bff-opendata/v1/api/v1/ricerca/avanzata`
- **Date Parameters**: `dataInizioPubProvvedimento` and `dataFinePubProvvedimento`
- **Important**: The date parameters filter by **Gazzetta Ufficiale publication date** (dataGU), not the law's emanation date

This means laws are retrieved based on when they were published in the Gazzetta Ufficiale, but the script organizes them by their emanation date. For example, a law emanated on December 30, 2025 but published in the Gazzetta Ufficiale on January 2, 2026 will be:
- Retrieved when searching for January 2026 publications
- Stored in `content/leggi/2025/12/` based on its emanation date

### Usage

```bash
# Install Python dependencies (first time only)
pip install -r scripts/requirements.txt

# Activate virtual environment (recommended)
source venv/bin/activate

# Run data collection for a specific month
cd scripts
python ricerca_normattiva.py <year> <month>

# Example: Fetch laws published in February 2026
python ricerca_normattiva.py 2026 2
```

The script will:
1. Query the API for all laws published in the specified month
2. Download full legislative text from Normattiva
3. Save JSON files in `content/leggi/{year}/{month}/` based on emanation date
4. Generate markdown frontmatter for each law

### Validation

The `scripts/validate_leggi.py` script validates that all legislative markdown files have the required YAML frontmatter fields.

```bash
# Validate all files in content/leggi/
python scripts/validate_leggi.py

# Validate with verbose output (shows all files)
python scripts/validate_leggi.py --verbose

# Validate a specific directory
python scripts/validate_leggi.py path/to/directory
```

Required fields for all legislative acts (LEGGE, DECRETO LEGISLATIVO, etc.):
- Core metadata: `codice-redazionale`, `tipo`, `numero-atto`
- Dates: `data-emanazione`, `data-gu`, `data-vigenza`, `numero-gu`
- Links: `normattiva-urn`, `normattiva-link`, `gu-link`
- Descriptions: `titolo-atto`, `descrizione-atto`, `titolo-alternativo`

Optional fields (not validated but commonly present):
- `atti-aggiornati`, `atti-correlati`, `lavori-preparatori`, `atti-parlamentari`
- Camera metadata: `camera-legislatura`, `camera-atto`, `camera-natura`, `camera-iniziativa`, etc.
- Senato metadata: `senato-did`, `senato-legislatura`, `senato-natura`, `senato-iniziativa`, etc.

## Workflows

- **Daily Update** - Runs at 6 AM UTC, creates PR with new legislative data
- **Validate Legislative Data** - Validates markdown files on PRs affecting `content/leggi/`
- **Deploy** - Deploys to GitHub Pages on push to main
