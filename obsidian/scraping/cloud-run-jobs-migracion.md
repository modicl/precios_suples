# Migración a Cloud Run Jobs + Cloud Scheduler

Estado (2026-11-10): código listo en `optimizado-v1`; **nada desplegado** (`gcloud run jobs list` vacío en el proyecto `primeproveedores`; `CLAUDE.md` menciona un proyecto `comparafit` que no aparece en la cuenta activa).

## Cambios de código
- `shared/scraper_registry.py`: lista única de scrapers (antes `main.py` y `RunAll.py` divergían: `main.py` usaba scrapers monolíticos, no tenía OutletFit ni pasaba `--headless` a los runners). Ver [[optimizacion-cpu-scraping-v1]].
- `main.py`: usa el registro, pasa `--headless` a los runners, `API_MODE=1` opcional, y define por defecto `BLOCK_RESOURCES=1`, `MAX_BROWSERS=4`, `MAX_SCRAPER_WORKERS=4`.
- El LLM de steps 5/6 es Gemini: el job necesita `GOOGLE_API_KEY` (ver [[llm-gemini-steps-5-6]]).

## Diseño
- **Un solo task** (sin sharding): el pipeline step1→8 corre al final de cada task; con shards procesaría datos parciales y compite por la BD. El guard de [[Step 3 - Insercion Bulk]] mitiga pero no reemplaza esto.
- Sistema de archivos del contenedor es efímero: `raw_data/` vive solo durante la ejecución.
- Secretos en Secret Manager: `DB_HOST_PROD`, `GOOGLE_API_KEY`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
- Riesgo abierto: tiendas que bloquean IPs de datacenter (Cloudflare). Hacer una ejecución de prueba y revisar qué tiendas dan 0 filas.
