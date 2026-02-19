import rss from '@astrojs/rss';
import { getCollection } from 'astro:content';
import type { APIRoute } from 'astro';

export const GET: APIRoute = async (context) => {
  const dossiers = await getCollection('dossier', ({ data }) => !data.draft);
  const inIter = await getCollection('in-iter', ({ data }) => !data.draft);

  // Merge and sort all publications
  const allPublications = [
    ...dossiers.map(d => ({ ...d, collection: 'dossier' as const })),
    ...inIter.map(i => ({ ...i, collection: 'in-iter' as const }))
  ];

  const sortedArticles = allPublications.sort(
    (a, b) => b.data.publishedDate.valueOf() - a.data.publishedDate.valueOf()
  );

  return rss({
    title: 'Gazzetta Civica',
    description: 'Trasparenza legislativa per una cittadinanza informata',
    site: context.site || 'https://andreasavia.github.io/gazzetta-civica',
    items: sortedArticles.map((article) => ({
      title: article.data.title,
      description:
        article.data.excerpt ||
        article.data.subtitle ||
        `Analisi e approfondimento: ${article.data.title}`,
      pubDate: article.data.publishedDate,
      link: `/${article.collection}/${article.data.publishedDate.getFullYear()}/${article.data.urlSlug}/`,
    })),
    customData: '<language>it-IT</language>',
    stylesheet: '/rss/styles.xsl',
  });
};
