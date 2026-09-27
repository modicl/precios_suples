---
tags: [scraping, bug, diagnostico]
fecha: 2026-09-27
---

# Diagnóstico: CSV vacíos (ChileSuplementos, Decathlon, OneNutrition, OutletFit)

Detectado al archivar `raw_data/` (ver [[byon-precios-truncados]]). Cada tienda falla por una causa distinta.

## ChileSuplementos: bloqueo por User-Agent (vacío desde 02/09)
- El tema CS no cambió: `li.cs-product-card` y el filtro `?tax_pwb-brand=` siguen funcionando.
- [[BaseScraper]] fija el UA `Chrome/124.0.0.0`. El sitio ahora responde **403 Forbidden** a ese UA (y a `131.0.0.0`), pero acepta 125, 128, 130, 139-145.
- Probado con Playwright usando la misma config que BaseScraper: UA 124 → 403 / 0 cards; UA 143 → 200 / 12 cards.
- Fix propuesto: usar la versión real del Chromium de Playwright (143), que además calza con los headers `sec-ch-ua`.
- Relacionado: [[chilesuplementos-theme-migration]].

## OneNutrition: migró de PrestaShop a Shopify
- `/tienda/<cat>` redirige a `/collections/<cat>`. `#js-product-list .product-miniature` ya no existe.
- Shopify expone `/collections/<handle>/products.json?limit=250&page=N` (marca en `vendor`, precio en `variants`).
- Fix propuesto: reescribir como scraper API sin Playwright, igual que los scrapers VTEX API.
- Handles actuales: `proteinas`, `creatinas`, `vitaminas-salud`, `energia-resistencia`, `ganadores-de-masa`, `aminoacidos-1`, `dieta-quemadores`, `barras-snack`, entre otros.

## OutletFit: nueva estructura de URLs
- Sigue en Jumpseller y con `<article>` + JSON-LD, pero todas las URLs `/catalogo/...` dan **404**.
- Nuevas raíces: `/proteinas`, `/creatinas`, `/pre-entrenos-y-energia/*`, `/recuperacion-muscular/amionacidos`, `/dieta-y-control-de-peso/*`, `/snack-saludables/*`, `/salud-y-bienestar/*`.
- Ahora hay paginación (`?page=2`); el scraper solo lee la primera página.
- Relacionado: [[outletfit-scraper]].

## Decathlon: Cloudflare
- Todo `decathlon.cl` responde 403 con challenge "Un momento…", incluidos `robots.txt` y `sitemap.xml`. No se resuelve ni headless ni con navegador visible (20 s).
- No se implementa bypass del challenge. El sitio bloquea explícitamente el acceso automatizado.

## Fixes aplicados (2026-09-27)
| Tienda | Cambio | Verificación |
|---|---|---|
| ChileSuplementos | `BaseScraper.USER_AGENT` (atributo de clase). Las 3 partes usan `USER_AGENT = None`, que toma la versión real de Chromium (143). El resto de scrapers sigue con Chrome/124. | 449 productos (96/198/155) vs 347 el 25/08 |
| OneNutrition | Reescrito como scraper API Shopify (`products.json`, sin Playwright). Mismo archivo y mismos helpers de limpieza de nombre. | 380 productos, 0 bajo $1.000 |
| OutletFit | URLs nuevas (subcategorías antes de la raíz `/proteinas`) + paginación `?page=N` hasta que no haya links nuevos. Se eliminó Pro-Hormonales (ya no existe). | 212 productos (antes ~73) |
| Decathlon | Quitado de `SCRAPERS` en `RunAll.py` y `main.py`. `main.py` ahora recorta `END_INDEX` al largo de la lista. | Quedan 22 `producto_tienda` activos en prod con precios de julio. |
