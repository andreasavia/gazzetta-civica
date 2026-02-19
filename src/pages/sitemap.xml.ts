import { getCollection } from 'astro:content';
import type { APIRoute } from 'astro';

export const GET: APIRoute = async () => {
  const dossiers = await getCollection('dossier', ({ data }) => !data.draft);
  const inIter = await getCollection('in-iter', ({ data }) => !data.draft);

  const site = 'https://andreasavia.github.io';
  const base = '/gazzetta-civica';
  const baseUrl = `${site}${base}`;

  const staticPages = [
    { url: '', changefreq: 'daily', priority: 1.0 },
    { url: '/dossier', changefreq: 'daily', priority: 0.9 },
    { url: '/in-iter', changefreq: 'daily', priority: 0.9 },
    { url: '/chi-siamo', changefreq: 'monthly', priority: 0.5 },
    { url: '/privacy', changefreq: 'monthly', priority: 0.3 },
    { url: '/contatti', changefreq: 'monthly', priority: 0.5 },
  ];

  const dossierUrls = dossiers.map((article) => ({
    url: `/dossier/${article.data.publishedDate.getFullYear()}/${article.data.urlSlug}`,
    lastmod: article.data.publishedDate.toISOString().split('T')[0],
    changefreq: 'monthly',
    priority: 0.8
  }));

  const inIterUrls = inIter.map((article) => ({
    url: `/in-iter/${article.data.publishedDate.getFullYear()}/${article.data.urlSlug}`,
    lastmod: article.data.publishedDate.toISOString().split('T')[0],
    changefreq: 'weekly',
    priority: 0.7
  }));

  const allUrls = [...staticPages, ...dossierUrls, ...inIterUrls];

  const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${allUrls.map(({ url, lastmod, changefreq, priority }) => `  <url>
    <loc>${baseUrl}${url}/</loc>
    ${lastmod ? `<lastmod>${lastmod}</lastmod>` : ''}
    <changefreq>${changefreq}</changefreq>
    <priority>${priority}</priority>
  </url>`).join('\n')}
</urlset>`;

  return new Response(sitemap, {
    headers: {
      'Content-Type': 'application/xml; charset=utf-8',
    },
  });
};
