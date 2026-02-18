import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import sitemap from '@astrojs/sitemap';
import pagefind from 'astro-pagefind';

export default defineConfig({
  site: 'https://andreasavia.github.io',
  base: '/gazzetta-civica',
  output: 'static',
  integrations: [
    mdx(),
    pagefind(),
  ]
});
