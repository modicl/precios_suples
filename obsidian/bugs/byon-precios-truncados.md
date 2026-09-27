---
tags: [bug, scraping, byon]
fecha: 2026-09-27
---

# BYON: precios truncados (81.990 → 81)

## Síntoma
Desde el run del 2026-09-24, cerca de la mitad de los productos de BYON quedaban con precio de 2-3 dígitos (ej. `81` en vez de `81990`). Eso generaba ofertas falsas en el sitio.

## Causa raíz (dos fallas encadenadas)
1. **Selector del listado:** el tema Shopify de BYON ahora renderiza `.price__sale` antes que `.price__regular`. En productos **sin oferta**, el primer `.price-item--regular` es un `<s>` vacío dentro de `.price__sale`, así que `price` quedaba en `0`.
2. **Fallback JSON-LD:** con `price == 0`, el scraper lee el JSON-LD del detalle, donde BYON publica `"price": "81.990"` (formato CLP). `parseFloat("81.990")` da `81.99` → `int()` da `81`.

## Fix
En [[BYONScraper]], `BYONScraperPart1` y `BYONScraperPart2`:
- Selector acotado: `.price__regular .price-item--regular`.
- El JS del JSON-LD devuelve el precio como string. En Python, si calza con `^\d{1,3}(\.\d{3})+$`, se eliminan los puntos antes de convertir.

## Datos afectados en producción
`historia_precios` de BYON con `precio < 1000`: 2026-09-24 (373 filas / 179 productos) y 2026-09-27 (188 filas / 180 productos).

**2026-09-27:** se reejecutó BYON con el fix y se cargó solo BYON a producción: 357 precios, 0 bajo $1.000, y se refrescaron las vistas. Los 332 enlaces activos quedaron con su último precio correcto. Después se borraron de producción las 561 filas BYON con `precio < 1000` del 24/09 y 27/09 (una transacción, conteo verificado) y se refrescaron las vistas. Quedan 15 filas antiguas bajo $1.000 (12/05 y 08/07) sin revisar.

## Hallazgo lateral: `raw_data/` acumula runs
`step1` lee *todos* los CSV de `raw_data/`. [[Step 3 - Insercion Bulk]] solo los mueve a `raw_data/used/` si ningún destino falló. Con la BD local caída, el target Local falla siempre, así que los CSV nunca se mueven y cada pipeline reinserta precios de runs anteriores (por eso el 24/09 tiene 373 filas para 179 productos).

Relacionado: [[division-scrapers-3-partes]], [[bug-farmacia-knopp-precio-cero]], [[step0-data-quality]]

## Fix del archivado (2026-09-27)
En [[Step 3 - Insercion Bulk]], el archivado a `raw_data/used/` ahora depende solo de que **Producción** haya salido bien. Si falla solo Local, se muestra un aviso y los CSV se mueven igual. Si no hay destino Production configurado, se mantiene la regla antigua (todos OK).

Se archivaron a mano 240 CSV ya cargados. Quedaron en `raw_data/` solo los 2 de AllNutrition del 27/09: nunca llegaron a producción porque `LOWEST_PRICE_SITES` deduplica por precio mínimo sobre *todos* los CSV acumulados, y ganaba siempre una fila con fecha antigua.

Hallazgos pendientes:
- Duplicados en `historia_precios` de producción, de todas las tiendas, por las reinserciones (ej. BYON 02/09: 1753 filas / 325 productos).
- CSV vacíos recurrentes: ChileSuplementos (desde 02/09), Decathlon, OneNutrition, OutletFit.
