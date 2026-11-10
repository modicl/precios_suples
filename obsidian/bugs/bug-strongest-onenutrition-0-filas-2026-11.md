# Bug: Strongest y OneNutrition con 0 filas (oct-nov 2026)

Detectado en la primera corrida de [[optimizacion-cpu-scraping-v1]]: ambos CSV salían con 0 filas y `RunAll` los mostraba `[OK]`. **No lo causó la rama optimizada**: el historial en `raw_data/used` muestra el problema desde antes.

## Strongest — rediseño del sitio
- Desde el 2026-10-17 todas las corridas dieron 0 (antes ~40-50 productos).
- El sitio migró de `/collection/<slug>` a `/tienda/categoria/<slug>/` con HTML renderizado en servidor: `article.p-card`, `.p-card__name`, `.p-card__brand`, `.price__now`, `.price s`, paginación `a[rel="next"]` (`?page=N`). Los selectores `.bs-collection__*` ya no existen.
- Slugs que cambiaron: `salud-y-bienestar` → `salud-y-vitalidad`, `aminoacidos-bcaa` → `aminoacidos`, `snacks` → `bebidas-y-snacks`.
- El JSON-LD del producto viene dentro de `@graph` (antes se leía como nodo raíz); de ahí salen imagen, SKU y descripción. Las rutas relativas se resuelven con `urljoin`.
- Resultado tras el fix: 172 productos, 0 con precio 0 y 0 con marca N/D. Ver [[BaseScraper]].

## OneNutrition — 429 de Cloudflare
- API Shopify (`products.json`) devuelve `429` con `Retry-After: 60`; la IP quedó bloqueada por horas (0 filas el 9 y 10 de nov; corridas parciales el 23-oct y 2-nov; normal ~380-395 productos).
- El scraper hacía `break` al primer error, sin reintento, y terminaba con código 0.
- Fix: reintentos con backoff que respetan `Retry-After` (60 s y 120 s), delays mayores (1,5 s entre páginas, 3 s entre colecciones), corte total si el 429 persiste, y `sys.exit(1)` si falla alguna colección o hay 0 productos.

## RunAll
- Una tienda con 0 filas que históricamente traía productos ahora entra en la lista de anomalías de la consola (antes solo aparecía en el HTML).

## Nota sobre conteos
Contar filas con `wc -l` sobrestima: las descripciones tienen saltos de línea. Usar pandas (`len(pd.read_csv(...))`).
