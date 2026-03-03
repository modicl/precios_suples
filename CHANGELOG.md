# Changelog - Precios Suplementos

---

## [2026-03-02] - Robustez y performance en step3: dedup de URLs, remap bulk y fix de packs

### Contexto
Al ejecutar el step3 con datos reales aparecieron tres errores en cadena: `uq_tienda_url` (misma URL con dos productos distintos en el CSV), `uq_producto_tienda` (el remap creaba un duplicado cuando el destino ya tenía fila), y `fk_producto_tienda` (DELETE sin reasignar `historia_precios`). Además el Paso A era un loop de Python con queries individuales que tardaba ~15 minutos en Neon.

### Cambios Implementados

#### 1. `local_processing_testing/step3_db_insertion.py` — dedup de URLs en el batch

- Nuevo dict `url_dedup` keyed por `(tid, url)`: detecta cuando la misma URL aparece en el CSV con dos `id_producto` distintos (el scraper la recogió desde dos categorías).
- Criterio de desempate: se prefiere la subcategoría más específica; si la fila nueva es `packs`, `ofertas`, `promo`, etc. se descarta. Si es más específica, reemplaza a la anterior.
- Previene que el bulk INSERT reciba dos filas con la misma `(id_tienda, url_link)` → eliminaba el error `uq_tienda_url`.

#### 2. `local_processing_testing/step3_db_insertion.py` — Paso A reescrito como operación SQL bulk

Reemplaza el loop de Python (~2990 iteraciones × 3 queries individuales) por una tabla temporal + 3 queries SQL:

- `CREATE TEMP TABLE _batch_links` con los datos del batch
- `UPDATE historia_precios ... JOIN _batch_links JOIN winner` — reasigna historial de huérfanos al winner (caso A2)
- `DELETE FROM producto_tienda ... JOIN _batch_links JOIN winner` — elimina huérfanos cuyo destino ya existe (caso A2, resuelve FK violation)
- `UPDATE producto_tienda ... LEFT JOIN dest ... WHERE dest IS NULL` — reasigna URL cuando el destino no existe aún (caso A1)

**Impacto en performance:** ~15 minutos → ~2 segundos en Neon.

#### 3. `scrapers_v2/OneNutritionScraper.py` — `_strip_bundle_suffix` corregido

- Nueva constante `_SUPPLEMENT_SUFFIX_RE`: regex que detecta si el sufijo del bundle es otro suplemento (creatina, omega, whey, caseína, vitamina, etc.).
- Si el sufijo es un suplemento → **no stripear** (es un pack real, se conserva como producto separado).
- Si el sufijo es un accesorio (shaker, balde, etc.) → stripear como antes.
- Antes: `"ISO 100 5lb + CREATINA 500 OSTROVIT"` → `"ISO 100 5lb"` (incorrecto, generaba duplicados).
- Ahora: `"ISO 100 5lb + CREATINA 500 OSTROVIT"` → sin cambio (producto separado).

---

## [2026-03-02] - Corrección de URL reutilizada en OneNutrition y limpieza de duplicados en producto_tienda

### Contexto
OneNutrition reutilizó la URL `iso-100-14-lb-dymatize` para un bundle (`ISO 100 (1.4 Lb) DYMATIZE + SHAKER DYMATIZE`), causando que el producto `id=414` (`Iso 100 1.4lb`, Dymatize, Proteína Aislada) dejara de actualizarse desde el 22 de febrero y que se creara un duplicado `id=7758` en la categoría Packs. Se corrigió el pipeline de inserción, el scraper y la BD para que el próximo run resuelva esto automáticamente.

### Cambios Implementados

#### 1. `local_processing_testing/step3_db_insertion.py` — pipeline de inserción reforzado
- **Fase 2 (URL remap)**: segunda pasada que, si una URL ya existe asignada a otro `id_producto`, la reasigna al producto correcto mediante `UPDATE` por `url_link`. Antes solo se hacía upsert por `id_producto`, dejando URLs huérfanas.
- **Fase 2b (desactivación)**: ahora desactiva por `url_link != ALL(active_urls)` en lugar de por `id_producto`. Más preciso: un producto se marca `is_active=False` cuando su URL ya no aparece en el scraping, independientemente de si el `id_producto` cambió.

#### 2. `scrapers_v2/OneNutritionScraper.py` — normalización de títulos de bundles
- Nueva función `_normalize_size_notation()`: elimina paréntesis alrededor de cantidades (`(1.4 Lb)` → `1.4lb`) y colapsa el espacio entre número y unidad (`300 GR` → `300gr`). Crítico para que el fuzzy match en Step 2 alcance score 100 contra el nombre canónico en BD.
- Nueva función `_strip_bundle_suffix()`: si el título contiene un sufijo de bundle (`+ SHAKER`, `+ BALDE`, etc.) y el prefijo ya incluye una cantidad de presentación (ej. `1.4lb`, `5 lb`), stripea el sufijo. Respeta fórmulas combinadas legítimas (`ZMA + B6`, `Calcio + Magnesio + Zinc`). Llama a `_normalize_size_notation` antes de procesar.
- Test de 20 casos ejecutado y confirmado.

#### 3. Limpieza de duplicados en `producto_tienda` (BD local y producción)
- Detectados y eliminados grupos donde la misma tienda+URL apuntaba a distintos `id_producto` (122 losers eliminados en producción en dos pasadas).
- 399 registros de `historia_precios` reasignados del loser al winner antes de eliminar.
- Agregado `UNIQUE CONSTRAINT uq_tienda_url (id_tienda, url_link)` en `producto_tienda` en **ambas BDs** para prevenir recurrencia.

### Estado post-cambio
- `id=414` (`Iso 100 1.4lb`, Dymatize): activo en 4 tiendas; OneNutrition se reconectará automáticamente en el próximo run.
- `id=7758` (`Iso 100 1.4lb Dymatize + Shaker Dymatize`, Packs): será marcado `is_active=False` automáticamente en el próximo run por la Fase 2b, sin intervención manual.

---

## [2026-02-25] - Deduplicación fuzzy de productos en BD producción

### Contexto
Productos scraped con nombres equivalentes pero en distinto orden de palabras no se clusterizaban correctamente en el pipeline, generando duplicados en producción. Se corrigió la lógica de clustering y se ejecutó una limpieza one-shot en la BD.

### Cambios Implementados

#### 1. `data_processing_v2/normalize_products.py` — lógica de clustering mejorada
- Scorer primario cambiado de `token_sort_ratio` a `token_set_ratio` (captura reordenamientos de palabras como "Creatina ON 300g" vs "ON Creatina 300g").
- Umbral bajado de 87 → **83** (necesario para capturar el par Serious Mass 12lb con score TSET=83.95).
- Guard secundario: `token_sort_ratio >= 65` en AND logic para evitar falsos positivos por subconjuntos de tokens.
- Keyword `ksm` agregado a `check_critical_mismatch` para separar Ashwagandha KSM-66 de Ashwagandha base.
- Keywords nuevos: `xxl`, `junior`, `classic`, `iso`, `whey`, `beef`, `zero`, `nac`, `omega`, `magnesio`, `cromo`, `ashwagandha`, `zma`, `keto`, `hardcore`, `nighttime`, `ultra`, `hersheys`, `reeses`, `kreator`, `cgt`, `shaks`, `4chef`, `snack`, `bite`, `sport`, `adult`, `curcuma`, `melena`, `complejo`, `l-carnitina`, `carnitina`, `alcachofa`, `extracto`, `vitamina`, `perejil`, `espresso`, `ultrapure`, `dha`, `tg`, `penne`, `espaguetis`, `fusilli`, `rigatoni`, `soul`, `protein`, `ksm`.
- `extract_flavors` ampliado: `kiwi`, `guarana`, `frutas tropicales`, `frutos tropicales`, `tropical`, `cereza`, `cherry`, `melocoton`, `peach`, `durazno`, `guanabana`.
- `check_percentage_mismatch` extendido para detectar `"79 Cacao"` vs `"70 Cacao"` (sin símbolo `%`).

#### 2. Nuevo script `tools/fuzzy_merge_db_products.py`
- Detecta y fusiona duplicados ya existentes en la BD de producción que no pasaron por el pipeline.
- Workflow: generar reporte CSV → revisar/editar manualmente → ejecutar solo filas aprobadas.
- Comparte exactamente los mismos umbrales y funciones que `normalize_products.py`.
- Uso: `python tools/fuzzy_merge_db_products.py` (reporte) / `--execute --report <file>` (aplicar).

#### 3. Nuevo test suite `tools/test_fuzzy_clustering.py`
- 28 casos de prueba: pares que deben fusionarse y falsos positivos que deben separarse.
- Todos pasando (28/28).

#### 4. Limpieza ejecutada en BD producción
- **56 duplicados fusionados** en una primera ronda revisada manualmente.
- Ejemplos: `Dymatize Creatina Creapure 300 gr` → `Dymatize Creatina Monohydrate Creapure 300gr`, `NUTREX LIPO 6 BLACK 60 CÁPSULAS` → `Lipo 6 Black Intense 60 Cápsulas`, `Anabolic Mass 12 lbs` → `USN Anabolic Mass 12 lb`, entre otros.

---

## [2026-02-25] - BrandClassifier y limpieza de marcas en BD local

### Contexto
El sistema anterior usaba `BrandMatcher` (basado en `marcas_dictionary.csv`) con lógica duplicada en algunos scrapers. Se reemplazó por un módulo centralizado con separación clara entre scrapers que tienen la marca en el DOM y los que deben inferirla del título.

### Cambios Implementados

#### 1. Nuevo módulo `BrandClassifier` (`scrapers_v2/BrandClassifier.py`)
- Reemplaza completamente a `data_processing/brand_matcher.py` para todos los scrapers v2.
- Fuente de datos: `scrapers_v2/diccionarios/keywords_marcas.json` (223 marcas canónicas, ~290 keywords).
- Singleton, igual que el `BrandMatcher` anterior.
- Expone tres métodos:
  - `normalize_brand(raw_brand)` — para scrapers con marca en el DOM. Mapea variantes al nombre canónico. Nunca escanea el título.
  - `extract_from_title(title)` — para scrapers sin marca en el DOM (DrSimi, KoteSport como fallback).
  - `classify(raw_brand, product_name, scan_title=False)` — punto de entrada desde `BaseScraper.enrich_brand()`. `scan_title=False` por defecto.
- 24 tests automatizados, todos pasando (`scrapers_v2/tests/test_brand_classifier.py`).

#### 2. `BaseScraper.py` actualizado
- Reemplazado `BrandMatcher` por `BrandClassifier`.
- `enrich_brand()` delega a `classify()` con `scan_title=False` por defecto, garantizando que scrapers deterministas nunca escaneen el título aunque el DOM devuelva vacío.

#### 3. `KoteSportScraper.py` simplificado
- Eliminada lógica propia de normalización de marcas (`_normalize_brand`, `_extract_brand_from_text`, `_extract_brand_from_title`) que cargaba el CSV manualmente.
- Ahora usa `enrich_brand(..., scan_title=True)` vía `BaseScraper`.

#### 4. `DrSimiScraper.py` corregido
- Reemplazado uso directo de `brand_matcher.get_best_match()` por `enrich_brand(..., scan_title=True)`.

#### 5. Nuevo script `tools/clean_db_brands.py`
- Unifica marcas duplicadas/variantes en la BD según el diccionario canónico.
- Soporta `--dry-run`, `--only-local`, `--only-prod`.
- Maneja 3 tipos de acciones: RENAME (el canónico no existe, se renombra la fila), MERGE (el canónico existe, se migran productos y se elimina la variante), PACK (marca de múltiples fabricantes, se asigna a la marca especial `PACK`).
- Resuelve conflictos de FK al hacer merge: migra los registros de `producto_tienda` al producto canónico antes de eliminar duplicados.

#### 6. Limpieza ejecutada en BD local
- BD local pasó de **298 → 278 marcas**.
- 1 RENAME: `Natures Truth` → `Nature's Truth`
- 20 MERGES, incluyendo: `BIOTECH USA`→`BioTechUSA`, `BPI`→`BPI SPORTS`, `My Protein`+`MYPROTEIN`→`MYPROTEIN`, `KNOP`+`Pharma Knop`→`Knop Laboratorios`, `NEWSCIENCE`→`New Science`, `QNT Sports`→`QNT`, `Sunvit Life`+`SUNVIT LIFE`→`SUNVIT`, `ANIMAL, UNIVERSAL`+`Animal / Universal`→`ANIMAL`, entre otros.
- 1 PACK creado: `Applied Nutrition / Ostrovit` → nueva marca `PACK` (id=454).

---

## [2026-02-14] - Integración determinística y heurística en clasificación

### Problemas Detectados
- La clasificación de categorías y marcas mezclaba lógica determinista (reglas fijas) con heurística (IA/fuzzy), sin una separación clara.

### Soluciones Implementadas
- Se separaron los pipelines determinista y heurístico en módulos independientes.
- La lógica determinista se aplica primero; solo se recurre a la heurística si el determinista no resuelve.

---

## [2026-02-10] - Integración Google Gemini (Cloud)

### Problemas Detectados
1. **Lentitud local**: Procesamiento con Ollama (RTX 4070 Ti) tardaba ~60 min para 3.000 productos (1.2 seg/prod), bloqueando el uso del PC.
2. **Límites de cuota (Free Tier)**: Errores 429 o 404 por nombres de modelo incorrectos o falta de facturación habilitada.

### Soluciones Implementadas
1. **Integración híbrida**: `tools/categorizer.py` soporta `ollama` o `google` mediante la variable de entorno `AI_PROVIDER`.
2. **SDK oficial**: Migración de peticiones REST crudas al SDK `google-genai`.
3. **Modelo optimizado**: `gemini-2.5-flash-lite` — cuota gratuita abierta, alto rendimiento.
4. **Batching adaptativo**:
   - Google: lotes de 50 + pausa 2 seg (~0.5 seg/prod, ~26 min total).
   - Ollama: lotes de 50 (~1.2 seg/prod, ~60 min total).
5. **Prompt refinado**: Instrucción "RAW JSON ONLY" para evitar markdown y reducir tokens.

### Estado Final
El sistema puede ejecutarse en la nube (gratis y rápido) o localmente (privado), cambiando `AI_PROVIDER` en `.env`.

---

## [2026-02-09] - Optimización y robustez del pipeline

### Problemas Detectados
1. **Duplicación de marcas similares**: Productos como "Wild Protein" aparecían fragmentados por diferencias menores en nombre o marca ("Wild Foods", "Wild Protein", "WILD").
2. **Violación de claves foráneas**: El borrado de productos duplicados fallaba porque `click_analytics` referenciaba los IDs antiguos.
3. **Errores de atributos en limpiador**: `NoneType has no attribute lower` cuando el resultado de limpieza era vacío.

### Soluciones Implementadas
1. **Unificación de marcas** (`step2_clean_names.py`): Lógica para eliminar palabras de relleno ("Barra de proteína", "45g") y agrupar correctamente sabores de una misma línea.
2. **Migración segura** (`debug_tools/unify_brands.py`): Migración de `click_analytics` antes de eliminar duplicados durante fusión de marcas.
3. **Robustez en limpieza**: Chequeos de nulidad antes de aplicar métodos de string.
4. **Organización del proyecto**: Estructura de datos en `data/1_classified`, `data/2_cleaned`, `data/3_normalized`. Scripts renombrados secuencialmente (Step 1–6).
5. **Auditoría del diccionario de marcas**: Eliminadas ~80 entradas inválidas del CSV (términos que no eran marcas reales como "Amino", "ISO 100", "Elite") que causaban borrado incorrecto de partes del nombre del producto.

---

## [2026-02-09] - Implementación Pipeline V2 Modular

### Problemas Detectados
1. **Lentitud**: Scraping + IA en tiempo real era inviable.
2. **Bloqueos de BD**: Inserción fila por fila causaba bloqueos y lentitud.
3. **Datos sucios**: Nombres con marcas repetidas ("Dymatize Iso 100 Dymatize").

### Soluciones Implementadas
1. **Desacople scraping / procesamiento**: Scraping offline rápido + procesamiento batch independiente.
2. **Batch IA**: `step1_ai_classification.py` usa Ollama en lotes de 50 para clasificar y limpiar nombres.
3. **Inserción bulk**: `step4_db_insertion.py` inserta miles de registros en 3 consultas masivas (Upsert).
4. **Limpieza determinista**: `step2_clean_names.py` usa `marcas_dictionary.csv` auditado para eliminar marcas del nombre del producto.

### Estado Final
El sistema procesa ~3.000 productos en minutos con tasa de éxito de inserción del 100% y alta calidad de agrupación.
