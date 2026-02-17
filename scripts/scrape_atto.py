
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import os
from datetime import datetime

BASE_URL = "https://www.normattiva.it"

def parse_normattiva_url(url):
    """Estrae i parametri da un URL di normattiva.it"""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)

    return {
        'data_gu': params.get('atto.dataPubblicazioneGazzetta', [None])[0],
        'codice': params.get('atto.codiceRedazionale', [None])[0],
        'data_vigenza': params.get('dataVigenza', [None])[0]
    }

def extract_law_metadata(soup):
    """Estrae metadati della legge (numero, data, titolo)"""
    metadata = {
        'law_type': 'LEGGE',  # Default
        'law_date': '',
        'law_number': '',
        'title': ''
    }

    # Cerca il tipo di atto nel testo
    for div in soup.find_all('div'):
        text = div.get_text(strip=True)
        # Match pattern like "LEGGE 19 gennaio 2026, n. 11"
        match = re.search(r'(LEGGE|DECRETO LEGISLATIVO|DECRETO LEGGE|DECRETO DEL PRESIDENTE DELLA REPUBBLICA)\s+(\d+\s+\w+\s+\d{4}),?\s*n\.\s*(\d+)', text, re.IGNORECASE)
        if match:
            metadata['law_type'] = match.group(1).upper()
            metadata['law_date'] = match.group(2)
            metadata['law_number'] = match.group(3)
            break

    # Estrai il titolo dal meta tag
    meta_title = soup.find('meta', property='eli:title')
    if meta_title and meta_title.get('content'):
        full_title = meta_title.get('content').strip()
        # Remove the code in parentheses at the end (e.g., "(26G00025)")
        metadata['title'] = re.sub(r'\s*\([A-Z0-9]+\)\s*$', '', full_title)
    else:
        # Fallback: cerca h1.akn-p
        title_elem = soup.find('h1', class_='akn-p')
        if title_elem:
            metadata['title'] = title_elem.get_text(strip=True)

    return metadata

def extract_law_title(soup):
    """Estrae il titolo completo della legge dalla pagina"""
    metadata = extract_law_metadata(soup)

    if metadata['law_date'] and metadata['law_number']:
        # Create full title like "LEGGE 19 gennaio 2026, n. 11"
        full_header = f"{metadata['law_type']} {metadata['law_date']}, n. {metadata['law_number']}"
        if metadata['title']:
            return f"{full_header}\n\n**{metadata['title']}**"
        return full_header
    elif metadata['title']:
        return metadata['title']
    else:
        return "Atto Normativo"

def generate_filename(title, codice, soup=None):
    """Genera un nome file basato sul titolo e codice"""
    if soup:
        metadata = extract_law_metadata(soup)
        if metadata['law_type'] and metadata['law_date'] and metadata['law_number']:
            # Extract year from date
            year_match = re.search(r'\d{4}', metadata['law_date'])
            if year_match:
                year = year_match.group()
                law_type_short = metadata['law_type'].replace(' ', '_')
                return f"{law_type_short}_{year}_n_{metadata['law_number']}.md"

    # Fallback: try to extract from title string
    match = re.search(r'(LEGGE|DECRETO)\s+.*?(\d{4}).*?n\.\s*(\d+)', title, re.IGNORECASE)
    if match:
        year = match.group(2)
        num = match.group(3)
        law_type = match.group(1).upper()
        return f"{law_type}_{year}_n_{num}.md"

    # Last resort: use codice
    return f"{codice}.md"

def html_to_markdown(element):
    """Converte un elemento HTML in markdown preservando i link"""
    result = []

    for child in element.children:
        if isinstance(child, str):
            text = child.strip()
            if text:
                result.append(text)
        elif child.name == 'a' and child.get('href'):
            href = child.get('href')
            text = child.get_text(strip=True)
            # Converti link relativi in assoluti
            if href.startswith('/'):
                href = urljoin(BASE_URL, href)
            result.append(f"[{text}]({href})")
        elif child.name == 'br':
            result.append('\n')
        elif child.name in ['p', 'div']:
            inner = html_to_markdown(child)
            if inner:
                result.append(inner)
        elif child.name in ['strong', 'b']:
            text = child.get_text(strip=True)
            if text:
                result.append(f"**{text}**")
        elif child.name in ['em', 'i']:
            text = child.get_text(strip=True)
            if text:
                result.append(f"*{text}*")
        else:
            text = child.get_text(strip=True)
            if text:
                result.append(text)

    return ' '.join(result)

def scrape_normattiva(data_gu, codice, data_vigenza=None, preserve_html=False, output_dir="scripts"):
    """
    Scarica un atto normativo da normattiva.it

    Args:
        data_gu: Data pubblicazione Gazzetta (formato YYYY-MM-DD)
        codice: Codice redazionale
        data_vigenza: Data vigenza (formato DD/MM/YYYY)
        preserve_html: Se True, preserva la struttura HTML con link
        output_dir: Directory di output
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })

    # Se data_vigenza non è fornita, usa una data futura
    if not data_vigenza:
        data_vigenza = datetime.now().strftime("%d/%m/%Y")

    # 1. Carichiamo la pagina principale dell'atto
    main_url = f"{BASE_URL}/atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta={data_gu}&atto.codiceRedazionale={codice}&tipoDettaglio=singolavigenza&dataVigenza={data_vigenza}"
    print(f"[*] Caricamento indice atto: {main_url}")
    resp = session.get(main_url)

    if resp.status_code != 200:
        print(f"[-] Errore caricamento pagina iniziale: {resp.status_code}")
        return None

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Estraiamo il titolo della legge
    law_title = extract_law_title(soup)
    print(f"[+] Titolo: {law_title}")

    # 2. Troviamo tutti i link degli articoli nell'indice
    articles_links = soup.find_all('a', class_='numero_articolo', onclick=True)

    if not articles_links:
        print("[-] Nessun articolo trovato nell'indice.")
        print("    Potrebbe essere un atto senza articoli o con struttura diversa.")
        # Salviamo comunque il contenuto della pagina principale
        print("    Tentativo di salvare il contenuto principale...")
        main_content = soup.find('div', class_='bodyTesto')
        if main_content:
            filename = generate_filename(law_title, codice, soup)
            filepath = os.path.join(output_dir, filename)

            md_content = f"# {law_title}\n\n"
            md_content += f"**Codice Redazionale:** {codice}\n"
            md_content += f"**Data GU:** {data_gu}\n"
            md_content += f"**Data Vigenza:** {data_vigenza}\n\n"
            md_content += "---\n\n"

            if preserve_html:
                md_content += html_to_markdown(main_content)
            else:
                md_content += main_content.get_text(separator='\n', strip=True)

            os.makedirs(output_dir, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(md_content)
            print(f"[!] File salvato: {filepath}")
            return filepath
        return None

    print(f"[+] Trovati {len(articles_links)} articoli. Inizio export...")

    md_content = f"# {law_title}\n\n"
    md_content += f"**Codice Redazionale:** {codice}\n"
    md_content += f"**Data GU:** {data_gu}\n"
    md_content += f"**Data Vigenza:** {data_vigenza}\n"
    md_content += f"**URL:** [{main_url}]({main_url})\n\n"
    md_content += "---\n\n"

    for link in articles_links:
        art_num = link.get_text(strip=True)
        onclick_content = link['onclick']

        match = re.search(r"showArticle\('([^']+)'", onclick_content)
        if not match:
            continue

        art_path = match.group(1)
        art_url = urljoin(BASE_URL, art_path).replace("&amp;", "&")

        print(f"    -> Recupero Articolo {art_num}...", end=" ", flush=True)

        art_resp = session.get(art_url)
        if art_resp.status_code == 200:
            art_soup = BeautifulSoup(art_resp.text, 'html.parser')
            content_div = art_soup.find('div', class_='bodyTesto')

            if content_div:
                # Estraiamo il titolo dell'articolo
                title_elem = content_div.find('h2', class_='article-num-akn')
                title = title_elem.get_text(strip=True) if title_elem else f"Articolo {art_num}"

                # Rimuoviamo il titolo dal div per non duplicarlo
                if title_elem:
                    title_elem.decompose()

                md_content += f"## {title}\n\n"

                if preserve_html:
                    # Preserviamo la struttura HTML con i link
                    body_text = html_to_markdown(content_div)
                else:
                    # Estrazione testo pulito
                    body_text = content_div.get_text(separator='\n', strip=True)

                md_content += f"{body_text}\n\n"
                md_content += "---\n\n"
                print(f"OK")
            else:
                print("∅")
        else:
            print(f"✖ ({art_resp.status_code})")

    # Generiamo il filename
    filename = generate_filename(law_title, codice, soup)
    filepath = os.path.join(output_dir, filename)

    os.makedirs(output_dir, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"\n[!] File salvato con successo: {filepath}")
    return filepath

def scrape_from_url(url, preserve_html=False, output_dir="scripts"):
    """Scarica un atto normativo partendo dall'URL completo"""
    params = parse_normattiva_url(url)

    if not params['data_gu'] or not params['codice']:
        print("[-] Errore: URL non valido o parametri mancanti")
        return None

    print(f"[*] Parametri estratti dall'URL:")
    print(f"    - Data GU: {params['data_gu']}")
    print(f"    - Codice: {params['codice']}")
    print(f"    - Data Vigenza: {params['data_vigenza']}")
    print()

    return scrape_normattiva(
        params['data_gu'],
        params['codice'],
        params['data_vigenza'],
        preserve_html=preserve_html,
        output_dir=output_dir
    )

if __name__ == "__main__":
    # Test URLs
    test_urls = [
        "https://www.normattiva.it/atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta=2026-01-07&atto.codiceRedazionale=25G00211&tipoDettaglio=singolavigenza&dataVigenza=22/01/2026",
        "https://www.normattiva.it/atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta=2026-02-04&atto.codiceRedazionale=26G00025&tipoDettaglio=singolavigenza&dataVigenza=19/02/2026",
        "https://www.normattiva.it/atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta=2026-02-09&atto.codiceRedazionale=26G00030&tipoDettaglio=singolavigenza&dataVigenza=24/02/2026",
        "https://www.normattiva.it/atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta=2026-02-10&atto.codiceRedazionale=26G00032&tipoDettaglio=singolavigenza&dataVigenza=25/02/2026"
    ]

    print("="*80)
    print("TEST SCRAPING NORMATTIVA")
    print("="*80)
    print()

    # Test con preserve_html=True per mantenere i link
    for i, url in enumerate(test_urls, 1):
        print(f"\n{'='*80}")
        print(f"TEST {i}/{len(test_urls)}")
        print(f"{'='*80}\n")

        result = scrape_from_url(url, preserve_html=True, output_dir="scripts")

        if result:
            print(f"✓ Completato: {result}")
        else:
            print(f"✗ Fallito")

        print()

    print("\n" + "="*80)
    print("TUTTI I TEST COMPLETATI")
    print("="*80)
