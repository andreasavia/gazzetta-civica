import * as fs from 'fs';
import * as path from 'path';
import matter from 'gray-matter';

export interface Legge {
  'codice-redazionale': string;
  tipo: string;
  'numero-atto': number;
  'data-emanazione': string;
  'data-gu': string;
  'numero-gu': number;
  'normattiva-link': string;
  'gu-link': string;
  'titolo-atto': string;
  'descrizione-atto': string;
  'camera-iniziativa'?: string;
  'camera-firmatari'?: string[];
  'camera-votazione-finale'?: string;
  'senato-natura'?: string;
  'senato-votazione-finale'?: string;
  [key: string]: unknown;
}

export interface DeputyPosition {
  deputato: string;
  posizione: 'favorevole' | 'contrario' | 'astenuto';
  gruppo?: string;
  argomento: string;
  citazioni: string[];
  deputato_url?: string;
}

const leggiCache = new Map<string, Legge>();

function scanLeggiDir(dir: string): void {
  if (!fs.existsSync(dir)) return;

  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      scanLeggiDir(fullPath);
    } else if (entry.name.endsWith('.md')) {
      const content = fs.readFileSync(fullPath, 'utf-8');
      const { data } = matter(content);
      if (data['codice-redazionale']) {
        leggiCache.set(data['codice-redazionale'], data as Legge);
      }
    }
  }
}

// Initialize cache
const contentRoot = path.resolve(process.cwd(), 'content/leggi');
scanLeggiDir(contentRoot);

export function getLegge(codiceRedazionale: string): Legge | undefined {
  return leggiCache.get(codiceRedazionale);
}

export function getAllLeggi(): Legge[] {
  return Array.from(leggiCache.values());
}

export function getPosizioni(slug: string): DeputyPosition[] | undefined {
  try {
    const filePath = path.resolve(process.cwd(), 'src/data/posizioni', `${slug}.json`);
    if (!fs.existsSync(filePath)) return undefined;
    const content = fs.readFileSync(filePath, 'utf-8');
    return JSON.parse(content) as DeputyPosition[];
  } catch (error) {
    console.warn(`Failed to load posizioni for ${slug}:`, error);
    return undefined;
  }
}
