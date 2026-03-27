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

## Servicios externos

El pipeline depende de tres servicios externos que deben estar configurados antes de correr cualquier paso. Sin ellos el proceso falla parcial o totalmente.

### PostgreSQL — Neon (producción)

**Qué es:** base de datos serverless PostgreSQL en la nube ([neon.tech](https://neon.tech)). Es donde viven todos los datos en producción: catálogo de productos, precios históricos, tiendas, usuarios, alertas.

**Por qué se usa:** permite connections pooling, branching de base de datos y escala a cero cuando no hay tráfico, sin necesidad de administrar instancias.

**Cómo funciona en el pipeline:** `shared/db_multiconnect.py` decide qué conexión abrir según el argumento `target`. Los steps 3–8 y los scrapers en Cloud Run siempre usan la conexión de producción (`DB_HOST_PROD`).

**Variable requerida:**
```env
DB_HOST_PROD=postgresql://user:password@host/db?sslmode=require&channel_binding=require
```
> Solicitar la cadena de conexión al administrador del proyecto. En Cloud Run se inyecta vía Secret Manager o `--set-env-vars`.

---

### OpenAI API (GPT-4o-mini)

**Qué es:** API de OpenAI para modelos de lenguaje.

**Dónde se usa:**
- **Step 5** (`step5_generate_descriptions.py`): genera descripciones comerciales para cada producto del catálogo.
- **Step 6** (`step6_tag_keywords.py`): asigna keywords por categoría a cada producto para búsqueda y filtrado.

Ambos steps son async con concurrencia configurable (`--concurrencia N`, default 10) e incrementales (solo procesan productos sin descripción/keywords, salvo `--forzar`).

**Variable requerida:**
```env
CHATGPT_MINI4=sk-proj-...   # API key de OpenAI
```

---

### Google Gemini API (alternativa a OpenAI)

**Qué es:** API de Google para modelos Gemini, usada como proveedor LLM alternativo.

**Dónde se usa:** mismos steps 5 y 6. El proveedor activo se controla con la variable `AI_PROVIDER`.

**Variables requeridas:**
```env
GOOGLE_API_KEY=AIza...
AI_PROVIDER=google   # o "openai" para usar CHATGPT_MINI4
```

---

### AWS S3 (imágenes de productos)

**Qué es:** bucket S3 `suplescrapper-images` (región `us-east-2`) donde se almacenan imágenes de productos descargadas desde las tiendas.

**Por qué se usa:** las URLs de imagen de los scrapers son hotlinks directos a las tiendas; son inestables y pueden romperse o bloquearse. Las imágenes se descargan y re-hospedan en S3 para tener URLs propias y estables.

**Cómo funciona:** `step3_db_insertion.py` distingue entre URLs de S3 (`%suplescrapper-images%`) y hotlinks. Solo una URL de S3 puede sobreescribir una imagen ya guardada; un hotlink solo rellena valores `NULL`. Esto evita degradar imágenes ya procesadas.

**Variables requeridas:**
```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-2
```

---

## Configuración

### Variables de entorno (`.env`)

Crear un archivo `.env` en la raíz del proyecto con los siguientes valores. Las variables marcadas con `*` son obligatorias para correr el pipeline completo.

```env
# BD local (Docker: contenedor suples-db)
DB_HOST=localhost
DB_NAME=suplementos
DB_USER=root
DB_PASSWORD=root
DB_PORT=5432

# BD producción (Neon) *
DB_HOST_PROD=postgresql://user:password@host/db?sslmode=require&channel_binding=require

# LLM — OpenAI *
CHATGPT_MINI4=sk-proj-...

# LLM — Google Gemini (alternativa a OpenAI)
GOOGLE_API_KEY=AIza...
AI_PROVIDER=openai   # "openai" | "google"

# AWS S3 (imágenes de productos) *
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-2

# Backend (step 8 — notificaciones de precio)
BACKEND_URL=https://...run.app
INTERNAL_API_SECRET=...
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

## Deploy en Google Cloud Run

El proyecto incluye un `Dockerfile` listo para construir y desplegar como **Cloud Run Job** en Google Cloud.

### Requisitos de plataforma

La imagen debe compilarse para **Linux x86-64 (`linux/amd64`)**, que es el único target soportado por Cloud Run. Si compilas desde macOS (Apple Silicon) o Windows ARM, el flag `--platform` es obligatorio.

### Build y push

```bash
docker build --platform linux/amd64 \
  -t us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest .

docker push us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest
```

### Actualizar y ejecutar el job

```bash
gcloud run jobs update comparafit-scraper \
  --image=us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest \
  --region=us-central1 \
  --set-env-vars="START_INDEX=0,END_INDEX=19,MAX_SCRAPER_WORKERS=4"

gcloud run jobs execute comparafit-scraper --region=us-central1
```

### Sharding horizontal

`main.py` lee `START_INDEX` y `END_INDEX` para ejecutar solo un subconjunto de los 20 scrapers. Esto permite distribuir la carga en múltiples tareas paralelas:

| Tarea | START_INDEX | END_INDEX |
|-------|-------------|-----------|
| 0 | 0 | 6 |
| 1 | 7 | 13 |
| 2 | 14 | 19 |

### Variables de entorno requeridas en Cloud Run

No incluir en la imagen; pasarlas vía `--set-env-vars` o Secret Manager:

| Variable | Descripción |
|----------|-------------|
| `START_INDEX` / `END_INDEX` | Rango del shard de scrapers |
| `MAX_SCRAPER_WORKERS` | Paralelismo de scrapers (default: `4`) |
| `DB_HOST_PROD` | Connection string Neon (producción) |
| `CHATGPT_MINI4` | API key OpenAI (steps 5 y 6) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Credenciales S3 para imágenes |
| `AWS_DEFAULT_REGION` | Región S3 (`us-east-2`) |
| `BACKEND_URL` / `INTERNAL_API_SECRET` | Backend para step 8 (notificaciones) |

### Ver logs

```bash
gcloud logging read \
  "resource.type=cloud_run_job AND resource.labels.job_name=comparafit-scraper" \
  --limit=200
```

### Imagen base

Usa `mcr.microsoft.com/playwright/python:v1.57.0-jammy` (Ubuntu Jammy, amd64), que incluye Chromium preinstalado con todas sus dependencias de sistema. Coincide exactamente con `playwright==1.57.0` del `requirements.txt`.

---

## Funcionalidades destacadas

- **RunAll post-scraping report**: al finalizar genera `logs/reporte_total/reporte_YYYY-MM-DD.txt` con conteo de productos por tienda, comparación contra promedio histórico 14 días, y alertas de anomalía si un scraper captura < 70% de su promedio.
- **QA pre-carga**: `check_data_quality.py` detecta nulos, precios inválidos, outliers, basura HTML y URLs duplicadas antes de insertar en BD. Genera reporte HTML interactivo en `logs/reporte_calidad/`.
- **SharedSeenUrls**: deduplicación de URLs entre procesos paralelos via archivo JSON + lock, sin race conditions.
- **Imágenes S3**: bucket `suplescrapper-images` (us-east-2). Solo URLs de S3 sobreescriben valores existentes; hotlinks solo rellenan NULLs.
- **Normalización fuzzy**: `token_set_ratio ≥ 83` con blockers para prevenir falsos merge (vegano vs. no vegano, hidrolizada vs. concentrada, % de cacao distintos).
