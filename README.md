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

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run data collection for current month
cd scripts
python ricerca_normattiva.py 2026 2
```

## Workflows

- **Daily Update** - Runs at 6 AM UTC, creates PR with new legislative data
- **Deploy** - Deploys to GitHub Pages on push to main
