import { getCollection } from 'astro:content';
import type { APIRoute } from 'astro';

export const GET: APIRoute = async () => {
  const articles = await getCollection('articoli', ({ data }) => !data.draft);

  const site = 'https://andreasavia.github.io';
  const base = '/gazzetta-civica';
  const baseUrl = `${site}${base}`;

  const staticPages = [
    { url: '', changefreq: 'daily', priority: 1.0 },
    { url: '/chi-siamo', changefreq: 'monthly', priority: 0.5 },
    { url: '/privacy', changefreq: 'monthly', priority: 0.3 },
    { url: '/contatti', changefreq: 'monthly', priority: 0.5 },
  ];

  const articleUrls = articles.map((article) => ({
    url: `/articoli/${article.data.publishedDate.getFullYear()}/${article.data.urlSlug}`,
    lastmod: article.data.publishedDate.toISOString().split('T')[0],
    changefreq: 'monthly',
    priority: 0.8
  }));

  const allUrls = [...staticPages, ...articleUrls];

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
