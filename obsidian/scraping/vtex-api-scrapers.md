# VTEX API Scrapers (SupleTech + SuplementosMayoristas)

## Contexto
SupleTech y SuplementosMayoristas usan la plataforma VTEX, que expone una API pública de catálogo.
En lugar de abrir un browser con Playwright (~21 min SupleTech, ~24 min Mayoristas), los scrapers API consultan directamente el JSON → **~32 segundos en total para ambas tiendas**.

## Endpoint
```
GET /api/catalog_system/pub/products/search?fq=C:/parent_id/child_id/&_from=N&_to=N
```
- Paginación: header `resources` → `offset-limit/total` (ej: `0-49/250`)
- Sin credenciales, solo `User-Agent` estándar
- Categorías descubiertas via `GET /api/catalog_system/pub/category/tree/3`

## Archivos
- `scrapers_v2/SupleTechApiScraper.py` — 21 categorías de SupleTech
- `scrapers_v2/SuplementosMayoristasApiScraper.py` — 15 categorías de Mayoristas
- `test_vtex_api.py` — script de validación/exploración (NO es el scraper productivo)

## Arquitectura
Ambos scrapers heredan de `BaseScraper` (sin usar Playwright):
- Sobreescriben `run()` completamente (no llaman a `super().run()`)
- Reutilizan `download_image()` para S3 upload (mismo pipeline que otros scrapers)
- Reutilizan `enrich_brand()`, `clean_text()`, `clean_description()`, `_log_info()`
- Mismo formato CSV de salida que el resto del pipeline

## Integración en RunAll
`RunAll.py` acepta `--api_mode` para activar los scrapers API:
```python
SCRAPERS_API_OVERRIDES = {
    "SupleTech":             "SupleTechApiScraper.py",
    "SuplementosMayoristas": "SuplementosMayoristasApiScraper.py",
}
```
Cuando `--api_mode` está activo, se aplican los overrides antes de lanzar los subprocesos. El resto de scrapers corre igual.

`run_scrapers.bat` pregunta interactivamente si usar API mode después de seleccionar el modo de paralelismo.

## Mapeo de Categorías (IDs clave)

### SupleTech
| fq_path | Subcategoría |
|---------|-------------|
| `C:/1/76/77/79/` | Concentradas (Whey) |
| `C:/1/76/77/80/` | Hidrolizadas |
| `C:/1/76/77/81/` | Isolate |
| `C:/1/76/77/105/` | Clear Whey |
| `C:/1/76/78/` | Veganas |
| `C:/1/11/50-51/` | Creatinas |
| `C:/2/17/` | Pre Entrenos |
| `C:/1/13/55,89,107/` | Aminoacidos |
| `C:/2/16/` | Quemadores |
| `C:/4/` | Vitaminas (rama completa) |

### SuplementosMayoristas
| fq_path | Subcategoría |
|---------|-------------|
| `C:/1/13/` | Whey Protein |
| `C:/1/12/` | Isolate |
| `C:/1/14/` | Hidrolizadas |
| `C:/2/17-18/` | Creatinas |
| `C:/3/` | Quemadores |
| `C:/4/` | Pre Entrenos |
| `C:/5/23,9290,9297,9298/` | Aminoacidos |
| `C:/9/` | Ganadores |

## Notas
- `C:/4/27/` y `C:/4/28/` (Omega/Probióticos en SupleTech) retornan 0 resultados con ID simple; se incluye `C:/4/` completa como fallback para cubrir toda la rama Vitaminas
- Los scrapers API NO están en `RUNNER_SCRIPTS` en RunAll.py (no necesitan `--headless`)
- S3 subfolder: `supletech` / `suplementosmayoristas`

## Relacionado
- [[division-scrapers-3-partes]] — contexto de paralelización general
- [[reporte-volumen-tiendas]] — tiempos de ejecución comparados
