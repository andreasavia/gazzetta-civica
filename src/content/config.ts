import { defineCollection, z } from 'astro:content';

// Shared schema for both dossier and in-iter
const publicationSchema = z.object({
  title: z.string(),
  urlSlug: z.string(), // URL-friendly slug (used in /{collection}/{year}/{urlSlug})
  subtitle: z.string().optional(),
  publishedDate: z.coerce.date(),
  draft: z.boolean().optional().default(false),
  legge: z.string().optional(), // codice-redazionale from content/leggi

  // Optional SEO fields
  excerpt: z.string().optional(), // Custom meta description (150-160 chars recommended)
  image: z.string().optional(), // Featured image for social sharing
  modifiedDate: z.coerce.date().optional(), // Last update date
  author: z.string().optional(), // Article author (defaults to "Gazzetta Civica")
});

const dossier = defineCollection({
  type: 'content',
  schema: publicationSchema,
});

const inIter = defineCollection({
  type: 'content',
  schema: publicationSchema,
});

export const collections = { dossier, 'in-iter': inIter };
