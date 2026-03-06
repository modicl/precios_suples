# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Precios Suples** is an automated pipeline for scraping, cleaning, normalizing, and storing supplement prices from ~20 Chilean online retailers into a PostgreSQL database (Neon on production).

# REGLAS DE SISTEMA: INTEGRACIÓN CON OBSIDIAN Y MEMORIA PERSISTENTE

1. **El Vault es la Fuente de Verdad:** La carpeta `/obsidian` ubicada en la raíz de este repositorio actúa como la memoria a largo plazo del proyecto. Debes leer proactivamente los archivos `.md` existentes en esta carpeta para obtener contexto antes de proponer cambios arquitectónicos, crear nuevos endpoints o modificar esquemas de base de datos.
2. **Escritura Estricta en el Vault:** Toda documentación que generes (análisis de bugs, diseño de arquitectura, flujos de datos o registros de decisiones) DEBE ser escrita y guardada exclusivamente dentro de la carpeta `/obsidian`.
3. **Estructura de Subcarpetas Dinámica:** Tienes la obligación de crear subcarpetas lógicas dentro de `/obsidian` para segmentar el conocimiento (ej. `/obsidian/backend`, `/obsidian/etl`, `/obsidian/bugs`, `/obsidian/scraping`). Clasifica la información según el módulo afectado para dar contexto a futuros cambios.
4. **Sintaxis de Enlaces de Obsidian:** Es estrictamente obligatorio usar la sintaxis de corchetes dobles (`[[Nombre del Documento]]`) para vincular conceptos, archivos relacionados o tickets. Cero enlaces Markdown tradicionales para archivos internos locales. Tu objetivo es mantener un grafo de conocimiento interconectado.
5. **Auto-Documentación:** Cada vez que resuelvas un problema complejo, refactorices código o implementes una nueva característica importante, crea o actualiza silenciosamente el archivo `.md` correspondiente en `/obsidian` detallando el "qué" y el "por qué" de los cambios, entrelazando las dependencias.

## Common Commands

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run all scrapers (Windows batch orchestrator)
run_scrapers.bat

# Run the 7-step data processing pipeline
run_pipeline.bat

# Run individual pipeline steps
python local_processing_testing/step1_clean_names.py
python local_processing_testing/step2_normalization.py
python local_processing_testing/step3_db_insertion.py
python local_processing_testing/step4_deduplication.py
python local_processing_testing/step5_generate_descriptions.py --dry-run
python local_processing_testing/step6_tag_keywords.py --dry-run
python local_processing_testing/step7_refresh_views.py

# Steps 5 & 6 flags
--dry-run          # Preview without DB writes
--forzar           # Regenerate existing entries
--concurrencia N   # Set async concurrency (default: 10)
```

### Cloud Run (Google Cloud)

```bash
# Build and push Docker image
docker build --platform linux/amd64 -t us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest .
docker push us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest

# Update and execute the Cloud Run job
gcloud run jobs update comparafit-scraper \
  --image=us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest \
  --region=us-central1 \
  --set-env-vars="START_INDEX=0,END_INDEX=19,MAX_SCRAPER_WORKERS=4"

gcloud run jobs execute comparafit-scraper --region=us-central1

# View logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=comparafit-scraper" --limit=200
```

## Architecture

### Data Flow

```
Scrapers (scrapers_v2/) → raw_data/*.csv
    ↓
Step 1: Clean names (encoding fixes, standardization)
    ↓
Step 2: Fuzzy normalization (RapidFuzz token_set_ratio ≥ 83, clusters equivalent products)
    ↓
Step 3: Bulk DB insertion (dedup URLs, brand inheritance, deactivate stale records)
    ↓
Step 4: DB deduplication cleanup
    ↓
Step 5: LLM descriptions (GPT-4o-mini, async, incremental)
    ↓
Step 6: LLM keyword tagging (GPT-4o-mini, async, category-specific dictionaries)
    ↓
Step 7: Refresh materialized views
```

### Key Modules

- **`main.py`** — Cloud Run entrypoint; reads `START_INDEX`/`END_INDEX` env vars to shard scrapers across containers
- **`scrapers_v2/BaseScraper.py`** — Playwright-based base class; all store scrapers extend this
- **`scrapers_v2/RunAll.py`** — Launches all scrapers in parallel, streams logs, generates post-run report with anomaly detection
- **`scrapers_v2/BrandClassifier.py`** / **`CategoryClassifier.py`** — Keyword-based auto-classification
- **`scrapers_v2/diccionarios/`** — JSON keyword files used by classifiers during scraping
- **`shared/normalize_products.py`** — Core text normalization: size/flavor/packaging extraction, clustering helpers
- **`shared/db_multiconnect.py`** — Returns SQLAlchemy engine for local or production DB based on target arg
- **`local_processing_testing/dictionaries/`** — Keyword JSON files used by Step 6 for LLM tag selection

### Scraper Architecture: Runners y Partes Paralelas

Las tiendas con mayor volumen están divididas en partes que corren en paralelo dentro de su Runner. Cada parte cubre un subconjunto de categorías y escribe su propio CSV (`site_name_partN.csv`).

| Runner | Partes | Estrategia |
|--------|--------|------------|
| `ChileSuplementosScraperRunner.py` | Part1 + Part2 + Part3 | Part1+Part2 paralelo → Part3 secuencial (Ofertas dedup via `SharedSeenUrls`) |
| `SportNutriShopScraperRunner.py` | Part1 + Part2 + Part3 | Paralelo total |
| `AllNutritionScraperRunner.py` | Part1 + Part2 | Paralelo total |
| `BYONScraperRunner.py` | Part1 + Part2 | Paralelo total |
| `SuplesScraperRunner.py` | Part1 + Part2 | Paralelo total |

**Regla:** Todos los runners aceptan `--headless` como argumento CLI y están declarados en `RUNNER_SCRIPTS` dentro de `RunAll.py`. Los scrapers monolíticos tienen `headless=True` hardcodeado en su `__main__`.

### RunAll — Reporte Post-Scrapeo

Al finalizar todos los scrapers, `RunAll.py` genera automáticamente `logs/reporte_total/reporte_YYYY-MM-DD_HH-MM-SS.txt` con:

- Productos encontrados por tienda (contando filas en `raw_data/*.csv` modificados durante el run)
- Comparación contra promedio diario de los últimos 14 días (desde `historia_precios` vía psycopg2 local)
- **Anomalía** si `count < 70%` del promedio histórico → advertencia en consola y en el reporte
- Duración total y estado (OK/FALLO) por scraper

### Database Schema (PostgreSQL + Prisma)

Schema defined in `bd/prisma/schema.prisma`. Core tables:

- **`productos`** — Master catalog; unique on `(nombre_producto, id_marca, id_subcategoria)`
- **`tiendas`** — Store metadata
- **`producto_tienda`** — Junction table with price, URL, active status; unique on `(id_tienda, url_link)`
- **`historia_precios`** — Price history time series
- **`marcas`** / **`categorias`** / **`subcategorias`** — Taxonomy
- **`usuarios`**, **`favoritos`**, **`alertas`** — User features
- **`click_analytics`**, **`search_analytics`** — Frontend analytics

### Normalization Logic

Step 2 uses `token_set_ratio` fuzzy matching with hard blockers to prevent false merges:

- Vegan vs. non-vegan products are never clustered together
- Keyword mismatches (e.g., "hidrolizada" vs. standard whey) block pairing
- Products with different cacao percentages are kept separate

### Cross-Process URL Deduplication

`SharedSeenUrls` en `BaseScraper.py` usa un lock basado en archivo para deduplicar URLs entre procesos paralelos sin race conditions. Crítico en `ChileSuplementos`: la categoría Ofertas (Part3) espera a que Part1+Part2 registren sus URLs antes de ejecutarse.

### Performance Notes

Step 3 was rewritten (Feb 2026) from Python loops to SQL bulk operations (`psycopg2.execute_values` + temp tables), reducing runtime from ~15 min to ~2 sec.

Scrapers divididos en partes paralelas (Mar 2026): ChileSuplementos (3 partes), SportNutriShop (3 partes), AllNutrition (2), BYON (2), Suples.cl (2). Reduce el tiempo total de scraping al paralelizar las categorías más voluminosas.

## Configuration (`.env`)

```
# Local DB (Docker container: suples-db)
DB_HOST=localhost
DB_NAME=suplementos
DB_USER=root
DB_PASSWORD=root
DB_PORT=5432

# Production (Neon)
DB_HOST_PROD=postgresql://user:pass@host:port/db?sslmode=require

# LLM
CHATGPT_MINI4=sk-proj-...

# AWS S3 (backups)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-2
```

## Scraper Output Format

Each scraper writes a CSV to `raw_data/` with columns:
`product_name, brand, price, url_link, category, subcategory`

Products with `brand == "N/D"` are filtered out to prevent data corruption (added Mar 2026).
