import { defineCollection, z } from 'astro:content';

const articoli = defineCollection({
  type: 'content',
  schema: z.object({
    title: z.string(),
    subtitle: z.string().optional(),
    publishedDate: z.coerce.date(),
    draft: z.boolean().optional().default(false),
    legge: z.string().optional(), // codice-redazionale from content/leggi
  })
});

export const collections = { articoli };
