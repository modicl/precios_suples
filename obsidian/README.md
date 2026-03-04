# ComparaFit — Knowledge Base

Vault de documentación técnica del proyecto **Precios Suples / ComparaFit**.

> **Regla:** toda la documentación técnica nueva va aquí. Usa `[[Nombre]]` para vincular notas.

---

## Módulos

### [[arquitectura/Flujo de Datos|Flujo de Datos]]
Descripción end-to-end del pipeline: scrapers → CSV → ETL → PostgreSQL.

### [[arquitectura/Schema de Base de Datos|Schema de Base de Datos]]
Tablas, constraints y relaciones del modelo Prisma.

---

## ETL Pipeline

| Step | Archivo | Nota |
|------|---------|------|
| 1 | `step1_clean_names.py` | Encoding y estandarización |
| 2 | `step2_normalization.py` | [[etl/Normalizacion Fuzzy]] |
| 3 | `step3_db_insertion.py` | [[etl/Step 3 - Insercion Bulk]] |
| 4 | `step4_deduplication.py` | Dedup residual en DB |
| 5 | `step5_generate_descriptions.py` | GPT-4o-mini async |
| 6 | `step6_tag_keywords.py` | Tags por categoría |
| 7 | `step7_refresh_views.py` | Vistas materializadas |

---

## Scraping

- [[scraping/BaseScraper]] — clase base Playwright con anti-bot
- [[scraping/SharedSeenUrls]] — deduplicación cross-process de URLs

---

## Bugs & Decisiones

- [[bugs/Filtro Brand ND]] — filtro de productos sin marca (Mar 2026)
