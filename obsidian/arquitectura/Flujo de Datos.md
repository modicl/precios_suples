# Flujo de Datos — ComparaFit

## Visión General

```
Scrapers (scrapers_v2/)  →  raw_data/*.csv
         ↓
   Step 1: clean_names    (encoding, estandarización)
         ↓
   Step 2: normalization  (fuzzy clustering RapidFuzz)   → [[etl/Normalizacion Fuzzy]]
         ↓
   Step 3: db_insertion   (bulk SQL, Neon PostgreSQL)     → [[etl/Step 3 - Insercion Bulk]]
         ↓
   Step 4: deduplication  (limpieza residual en DB)
         ↓
   Step 5: descriptions   (GPT-4o-mini, async, incremental)
         ↓
   Step 6: tag_keywords   (GPT-4o-mini, diccionarios por categoría)
         ↓
   Step 7: refresh_views  (vistas materializadas PostgreSQL)
```

---

## Orquestación

### Local
`run_scrapers.bat` → lanza todos los scrapers en paralelo
`run_pipeline.bat` → ejecuta los 7 steps en orden

### Cloud Run (`main.py`)
Entrypoint del Docker. Lee `START_INDEX` / `END_INDEX` para sharding horizontal.

**Lógica:**
1. **Fase 1 — Scrapers**: `ThreadPoolExecutor` con `MAX_SCRAPER_WORKERS` (default 4). Con 2–4 GB RAM en Cloud Run no se pueden levantar más de 4 Chromiums en paralelo.
2. **Fase 2 — Pipeline**: secuencial estricto. Si un step falla con exit ≠ 0, el pipeline aborta y Cloud Run marca la tarea como fallida.

**Scrapers disponibles (20 tiendas):**

| Índice | Nombre | Script |
|--------|--------|--------|
| 0 | ChileSuplementos | ChileSuplementosScraperRunner.py |
| 1 | AllNutrition | AllNutritionScraper.py |
| 2 | Byon | BYONScraper.py |
| 3 | CruzVerde | CruzVerdeScraper.py |
| 4 | Decathlon | DecathlonScraper.py |
| 5 | DrSimi | DrSimiScraper.py |
| 6 | FarmaciaKnop | FarmaciaKnopScraper.py |
| 7 | KoteSport | KoteSportScraper.py |
| 8 | Suples | SuplesScraper.py |
| 9 | SuplementosBullChile | SuplementosBullChileScraper.py |
| 10 | SuplementosMayoristas | SuplementosMayoristasScraper.py |
| 11 | SportNutriShop | SportNutriShopScraper.py |
| 12 | SupleTech | SupleTechScraper.py |
| 13 | SupleStore | SupleStoreScraper.py |
| 14 | OneNutrition | OneNutritionScraper.py |
| 15 | MuscleFactory | MuscleFactoryScraper.py |
| 16 | FitMarketChile | FitMarketChileScraper.py |
| 17 | Strongest | StrongestScraper.py |
| 18 | WildFoods | WildFoodsScraper.py |
| 19 | WinklerNutrition | WinklerNutritionScraper.py |

---

## Formato CSV (salida de scrapers)

Cada scraper escribe `raw_data/productos_{site_name}_{YYYY-MM-DD}.csv` con columnas:

```
date, site_name, category, subcategory, product_name, brand,
price, link, rating, reviews, active_discount,
thumbnail_image_url, image_url, sku, description
```

---

## Imágenes (S3)

Ver [[scraping/BaseScraper]] — estrategia "Fire and Forget":
1. Hash MD5 de la URL → nombre de archivo estable entre ejecuciones
2. Buscar en `assets/img/resized/{subfolder}/` — si existe, retornar URL canónica
3. Si no: subir a `assets/img/originals/` y retornar URL de resized (404 hasta que Lambda procese)

---

## Referencias

- [[arquitectura/Schema de Base de Datos]]
- [[scraping/BaseScraper]]
- [[etl/Step 3 - Insercion Bulk]]
