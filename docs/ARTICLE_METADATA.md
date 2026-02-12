# Article Metadata Guide

This document explains the available frontmatter fields for articles and how they affect SEO.

## Required Fields

```yaml
title: "Article Title"
publishedDate: 2026-01-22
```

## Optional Basic Fields

```yaml
subtitle: "Optional subtitle shown below title"
draft: false  # Set to true to hide from public listings
legge: "25G00211"  # Links to law metadata for automatic categorization
```

## Optional SEO Fields

These fields enhance your article's SEO and social media presence:

### `excerpt`
**Purpose**: Custom meta description for search engines and RSS feeds
**Recommended length**: 150-160 characters
**Priority**: Used in preference to `subtitle` for meta descriptions

```yaml
excerpt: "La riforma della Corte dei Conti introduce nuovi criteri per la responsabilità amministrativa e il danno erariale."
```

**Fallback behavior**: If not provided, uses `subtitle`, then auto-generates from title.

### `image`
**Purpose**: Featured image for social media sharing (Open Graph, Twitter Cards)
**Format**: Relative path from `public/` directory or absolute URL
**Recommended size**: 1200x630px for optimal social sharing

```yaml
image: "/images/corte-dei-conti-2026.jpg"
```

**Fallback behavior**: If not provided, uses site favicon.

### `modifiedDate`
**Purpose**: Signals content freshness to search engines
**When to use**: Set this when you significantly update an article
**Format**: YYYY-MM-DD

```yaml
modifiedDate: 2026-02-11
```

**Fallback behavior**: If not provided, uses `publishedDate`.

### `author`
**Purpose**: Attribution and E-E-A-T (Experience, Expertise, Authoritativeness, Trustworthiness)
**When to use**: If you have multiple authors or want specific attribution

```yaml
author: "Nome Cognome"
```

**Fallback behavior**: If not provided, defaults to "Gazzetta Civica".

## Complete Example

```yaml
---
# Required
title: "Riforma della Corte dei Conti"
publishedDate: 2026-01-22

# Basic optional
subtitle: "La Legge n. 1/2026 ridefinisce funzioni e responsabilità amministrativa"
legge: "25G00211"
draft: false

# SEO optional
excerpt: "La riforma della Corte dei Conti introduce nuovi criteri per la responsabilità amministrativa e il danno erariale, con l'obiettivo di modernizzare il controllo sulla spesa pubblica."
image: "/images/riforma-corte-conti.jpg"
author: "Redazione Gazzetta Civica"
modifiedDate: 2026-02-15
---

Your article content here...
```

## Priority Order for Descriptions

The system uses this priority order when generating meta descriptions:

1. `excerpt` (if provided)
2. `subtitle` (if provided)
3. Auto-generated: `"Analisi e approfondimento: {title}"`

## SEO Impact Summary

| Field | Direct SEO Impact | User Experience | Recommended |
|-------|------------------|-----------------|-------------|
| `excerpt` | ⭐⭐⭐ High | Better search snippets | Yes, for key articles |
| `image` | ⭐⭐⭐ High | Better social sharing | Yes, when available |
| `modifiedDate` | ⭐⭐ Medium | Freshness signal | Yes, when updating |
| `author` | ⭐⭐ Medium | Authority/Trust | Optional |

## Best Practices

1. **For new articles**: At minimum, add `excerpt` if you want custom search snippets
2. **For updated articles**: Always update `modifiedDate` for significant changes
3. **For social sharing**: Add `image` for articles you expect to be shared
4. **For authority**: Add `author` if building author reputation matters

## Technical Notes

- All fields are optional and have sensible fallbacks
- Existing articles work without changes
- Add fields gradually as needed
- SEO components automatically use these fields when present
