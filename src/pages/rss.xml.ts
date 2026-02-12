import rss from '@astrojs/rss';
import { getCollection } from 'astro:content';
import type { APIRoute } from 'astro';

export const GET: APIRoute = async (context) => {
  const articles = await getCollection('articoli', ({ data }) => !data.draft);
  const sortedArticles = articles.sort(
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
      link: `/articoli/${article.slug}/`,
    })),
    customData: '<language>it-IT</language>',
    stylesheet: '/rss/styles.xsl',
  });
};
