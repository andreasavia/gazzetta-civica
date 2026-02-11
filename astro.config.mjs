import { defineConfig } from 'astro/config';
import AstroPWA from '@vite-pwa/astro';

export default defineConfig({
  site: 'https://andreasavia.github.io',
  base: '/gazzetta-civica',
  output: 'static',
  integrations: [
    AstroPWA({
      registerType: 'autoUpdate',
      manifest: {
        name: 'Gazzetta Civica',
        short_name: 'Gazzetta',
        description: 'Legislazione italiana e analisi',
        theme_color: '#1a1a1a',
        background_color: '#f5f3f0',
        display: 'standalone',
        icons: [
          { src: 'icon-192.png', sizes: '192x192', type: 'image/png' },
          { src: 'icon-512.png', sizes: '512x512', type: 'image/png' }
        ]
      }
    })
  ]
});
