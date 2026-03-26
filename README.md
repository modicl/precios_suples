# Precios Suples

Pipeline automatizado de scraping, normalización e inserción de precios de suplementos deportivos de ~20 tiendas chilenas en una base de datos PostgreSQL.

---

## Estructura de directorios

```
precios_suples/
├── scrapers_v2/              # Scrapers y clasificadores
│   ├── BaseScraper.py        # Clase base Playwright + SharedSeenUrls
│   ├── RunAll.py             # Orquestador paralelo + reporte post-scrapeo
│   ├── BrandClassifier.py    # Clasificador de marcas por keywords
│   ├── CategoryClassifier.py # Clasificador de categorías/subcategorías
│   ├── diccionarios/         # JSONs de keywords para clasificadores
│   └── *.py                  # Scrapers individuales y runners
├── local_processing_testing/ # Pipeline de procesamiento (steps 1–8)
├── shared/                   # Módulos compartidos (normalize, db_connect)
├── bd/prisma/                # Schema PostgreSQL (Prisma)
├── raw_data/                 # CSVs generados por los scrapers
├── logs/                     # Logs de scraping, QA y pipeline
├── obsidian/                 # Documentación interna (vault Obsidian)
├── check_data_quality.py     # QA pre-carga de CSVs
├── run_scrapers.bat          # Ejecuta los scrapers (menú interactivo)
└── run_pipeline.bat          # Ejecuta el pipeline completo (steps 1–8)
```

---

## Flujo general

```
run_scrapers.bat
    └── RunAll.py → scrapers en paralelo → raw_data/*.csv
                 → reporte post-scrapeo con detección de anomalías

check_data_quality.py   ← ejecutar entre scrapers y pipeline
    └── Analiza CSVs: nulos, precios inválidos, URLs duplicadas, basura HTML
    └── Genera logs/reporte_calidad/reporte_calidad_YYYY-MM-DD.html

run_pipeline.bat
    ├── Step 1: Limpieza de nombres          (step1_clean_names.py)
    ├── Step 2: Normalización / clustering   (step2_normalization.py)
    ├── Step 3: Inserción en BD              (step3_db_insertion.py)
    ├── Step 4: Deduplicación                (step4_deduplication.py)
    ├── Step 5: Descripciones LLM            (step5_generate_descriptions.py)
    ├── Step 6: Tagging de keywords          (step6_tag_keywords.py)
    ├── Step 7: Refresh vistas materializadas(step7_refresh_views.py)
    └── Step 8: Trigger notificaciones precio(step8_trigger_notificaciones.py)
```

---

## Scrapers

### Tiendas con runners (partes paralelas)

Para tiendas de alto volumen, el scraping se divide en partes que corren en paralelo dentro de un Runner:

| Runner | Partes | Estrategia |
|--------|--------|------------|
| `ChileSuplementosScraperRunner.py` | Part1 + Part2 → Part3 | Part3 secuencial (Ofertas usa SharedSeenUrls) |
| `SportNutriShopScraperRunner.py` | Part1 + Part2 + Part3 | Paralelo total |
| `AllNutritionScraperRunner.py` | Part1 + Part2 | Paralelo total |
| `BYONScraperRunner.py` | Part1 + Part2 | Paralelo total |
| `SuplesScraperRunner.py` | Part1 + Part2 | Paralelo total |
| `FarmaciaKnopScraperRunner.py` | Part1 + Part2 | Paralelo total |
| `CruzVerdeScraperRunner.py` | Part1 + Part2 | Paralelo total |

### Scrapers VTEX API (sin Playwright)

Dos tiendas usan la VTEX Catalog API directamente, ~10x más rápido que el scraper browser:

| Scraper | Tienda |
|---------|--------|
| `SupleTechApiScraper.py` | SupleTech (~21 categorías, ~1-2 min) |
| `SuplementosMayoristasApiScraper.py` | SuplementosMayoristas (~15 categorías, ~1-2 min) |

Activar con `--api_mode` en `RunAll.py` o seleccionando `S` en `run_scrapers.bat`.

### Scrapers monolíticos

`AllNutritionScraper.py`, `BYONScraper.py`, `CruzVerdeScraper.py`, `DecathlonScraper.py`, `DrSimiScraper.py`, `FarmaciaKnopScraper.py`, `FitMarketChileScraper.py`, `KoteSportScraper.py`, `MuscleFactoryScraper.py`, `OneNutritionScraper.py`, `OutletFitScraper.py`, `StrongestScraper.py`, `SuplementosBullChileScraper.py`, `SupleStoreScraper.py`, `WildFoodsScraper.py`, `WinklerNutritionScraper.py`

### Formato CSV de salida

Cada scraper escribe en `raw_data/` con columnas:
```
date, site_name, category, subcategory, product_name, brand, price,
link, rating, reviews, active_discount, thumbnail_image_url, image_url, sku, description
```

> Productos con `brand == "N/D"` son filtrados automáticamente en el pipeline.

---

## Pipeline — detalle de pasos

| Step | Script | Descripción |
|------|--------|-------------|
| 1 | `step1_clean_names.py` | Fix de encoding, estandarización de nombres |
| 2 | `step2_normalization.py` | Fuzzy matching RapidFuzz (`token_set_ratio ≥ 83`), clustering de productos equivalentes |
| 3 | `step3_db_insertion.py` | Bulk insert via `psycopg2` + tablas temporales (~2 seg) |
| 4 | `step4_deduplication.py` | Limpieza de duplicados en BD |
| 5 | `step5_generate_descriptions.py` | Descripciones comerciales con GPT-4o-mini (async) |
| 6 | `step6_tag_keywords.py` | Tagging de keywords por categoría con GPT-4o-mini (async) |
| 7 | `step7_refresh_views.py` | Refresca vistas materializadas PostgreSQL |
| 8 | `step8_trigger_notificaciones.py` | Dispara alertas de precio en producción |

**Flags disponibles en steps 5 y 6:**
```bash
--dry-run        # Preview sin escritura en BD
--forzar         # Regenera entradas existentes
--concurrencia N # Concurrencia async (default: 10)
```

---

## Base de datos

### Tablas principales

| Tabla | Descripción |
|-------|-------------|
| `productos` | Catálogo maestro; único en `(nombre_producto, id_marca, id_subcategoria)` |
| `tiendas` | Metadata de tiendas |
| `producto_tienda` | Junction table con precio, URL, estado activo; único en `(id_tienda, url_link)` |
| `historia_precios` | Serie de tiempo de precios |
| `marcas` / `categorias` / `subcategorias` | Taxonomía |
| `usuarios`, `favoritos`, `alertas` | Features de usuario |
| `click_analytics`, `search_analytics` | Analytics de frontend |

Schema completo en `bd/prisma/schema.prisma`.

### Conexiones

```
# Local (Docker)
postgresql://root:root@localhost:5432/suplementos   (contenedor: suples-db)

# Producción (Neon)
DB_HOST_PROD=postgresql://...   (en .env)
```

---

## Configuración

### Variables de entorno (`.env`)

```env
# BD local
DB_HOST=localhost
DB_NAME=suplementos
DB_USER=root
DB_PASSWORD=root
DB_PORT=5432

# BD producción (Neon)
DB_HOST_PROD=postgresql://user:pass@host:port/db?sslmode=require

# LLM
CHATGPT_MINI4=sk-proj-...

# AWS S3 (imágenes)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-2
```

### Instalación

```bash
pip install -r requirements.txt
```

---

## Ejecución

```bash
# 1. Correr scrapers
run_scrapers.bat          # menú interactivo (headless / visible / API mode)

# 2. QA pre-carga (opcional pero recomendado)
python check_data_quality.py

# 3. Pipeline completo
run_pipeline.bat

# Steps individuales
python local_processing_testing/step1_clean_names.py
python local_processing_testing/step3_db_insertion.py
python local_processing_testing/step5_generate_descriptions.py --dry-run
python local_processing_testing/step6_tag_keywords.py --dry-run --concurrencia 5

# Runner específico
python scrapers_v2/ChileSuplementosScraperRunner.py --headless
```

---

## Funcionalidades destacadas

- **RunAll post-scraping report**: al finalizar genera `logs/reporte_total/reporte_YYYY-MM-DD.txt` con conteo de productos por tienda, comparación contra promedio histórico 14 días, y alertas de anomalía si un scraper captura < 70% de su promedio.
- **QA pre-carga**: `check_data_quality.py` detecta nulos, precios inválidos, outliers, basura HTML y URLs duplicadas antes de insertar en BD. Genera reporte HTML interactivo en `logs/reporte_calidad/`.
- **SharedSeenUrls**: deduplicación de URLs entre procesos paralelos via archivo JSON + lock, sin race conditions.
- **Imágenes S3**: bucket `suplescrapper-images` (us-east-2). Solo URLs de S3 sobreescriben valores existentes; hotlinks solo rellenan NULLs.
- **Normalización fuzzy**: `token_set_ratio ≥ 83` con blockers para prevenir falsos merge (vegano vs. no vegano, hidrolizada vs. concentrada, % de cacao distintos).
