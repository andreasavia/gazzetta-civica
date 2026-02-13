import { defineCollection, z } from 'astro:content';

const articoli = defineCollection({
  type: 'content',
  schema: z.object({
    title: z.string(),
    urlSlug: z.string(), // URL-friendly slug (used in /articoli/{year}/{urlSlug})
    subtitle: z.string().optional(),
    publishedDate: z.coerce.date(),
    draft: z.boolean().optional().default(false),
    legge: z.string().optional(), // codice-redazionale from content/leggi

    // Optional SEO fields
    excerpt: z.string().optional(), // Custom meta description (150-160 chars recommended)
    image: z.string().optional(), // Featured image for social sharing
    modifiedDate: z.coerce.date().optional(), // Last update date
    author: z.string().optional(), // Article author (defaults to "Gazzetta Civica")
  })
});

export const collections = { articoli };
