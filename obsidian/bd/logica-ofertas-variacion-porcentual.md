---
tags: [bd, vistas, ofertas, backend, bug]
fecha: 2026-09-27
---

# Lógica de "oferta" (`variacion_porcentual`) y ofertas falsas o eternas

## Dónde vive
- Definición de las vistas: repo backend `supleprecios-backend`, en `prisma/migrations/20260301180000_filter_is_active_in_mvs/migration.sql` (vigente) y `scripts/rebuild-views.js` (para la BD local).
- Consumo: `src/main-queries/main-queries.service.ts` lee **solo** `mv_main_query_v2`:
  - `onlydiscount` → `variacion_porcentual < 0`
  - orden por defecto → `variacion_porcentual ASC, ultimo_precio ASC`
  - `findSimilarProducts` ordena por la misma columna.
- `mv_click_stats` (productos destacados) expone `descuento = variacion_porcentual`.
- Favoritos (`favoritos.service.ts`) muestra `variacion_porcentual` desde la misma vista.
- `mv_main_query` (v1) **no la usa el backend**, pero se refresca en step 7.

## Cálculo actual
- `PrecioActual`: último registro por `id_producto_tienda`.
- `PrecioAnteriorDiferente`: registro más reciente con precio distinto, **sin límite de fecha**.
- `variacion = (actual - anterior) / anterior * 100`.

## Problemas (prod, 2026-09-27)
1. **Sin ventana de tiempo.** Una baja de hace meses sigue siendo "oferta" mientras el precio no cambie. 290 de 345 ofertas no cambian hace más de 14 días.
2. **Outlier único como referencia.** Ej.: Barrita Whey Protein 45g (Winkler): 1190 → 22170 (un run capturó el precio de la caja) → 1380 = -93,78% desde el 10/09. Gel Boost (SportNutriShop): caja 24990 → unidad 1290 = -94,84%.
3. **Precio 0 = "-100%".** 51 filas de la vista tienen `ultimo_precio = 0`. Además, `rn_mejor_precio` ordena por `precio_actual ASC`: un 0 le ganaría al precio real de otra tienda para el mismo producto (hoy no ocurre, pero la lógica lo permite).
4. **`showstockless` invertido.** `true` (valor por defecto) aplica `ultimo_precio > 0`, o sea que *oculta* los productos sin stock. El nombre dice lo contrario. Funciona por accidente; revisar el frontend antes de corregirlo.
5. **Notificaciones.** *(Corregido: el análisis inicial se hizo sobre una copia local del backend desactualizada 22 commits.)*
   - El módulo existe: `src/notificaciones/notificaciones.service.ts`, en `procesarDescuentos`.
   - Compara `MIN(precio)` de hoy vs ayer por producto, pero **no excluye precio 0 ni enlaces inactivos**.
   - Resultado: una tienda sin stock (0) dispararía un email "bajó a $0". Hoy no hay notificaciones enviadas.
6. Los duplicados por reinserción de CSV ([[byon-precios-truncados]]) inflan `historia_precios` y el gráfico de `findHistoricPrices`.

## Solución propuesta (validada solo en modo lectura contra prod)
SQL de cálculo, reemplazando los CTE `PrecioActual` / `PrecioAnteriorDiferente`:
- `diario`: un precio por día y enlace, **excluyendo precio 0** (anula duplicados y sin stock).
- `tramos`: `LAG` + suma acumulada para numerar tramos de precio constante. `desde` = inicio del tramo vigente.
- `referencia`: **mediana** de los 30 días previos a `desde`.
- La variación es 0 si:
  - `precio_actual <= 0`, o no hay referencia;
  - `desde < hoy - 14` (el cambio ya es viejo);
  - la baja es mayor a 70% o la subida mayor a 200% (sospechosa);
  - el cambio es menor a 5% (ruido).
- `rn_mejor_precio`: ordenar por `(precio_actual <= 0), precio_actual`, para que un 0 nunca gane.

Resultado en prod:

| | Vista actual | Propuesta |
|---|---|---|
| Ofertas | 387 | 44 |
| Descuentos ≤ -50% | 53 | 0 |
| Barrita Whey Winkler | -93,78% | 0 (vs mediana 1190) |
| Gel Boost | -94,84% | 0 (baja > 70%) |
| Tiempo de cálculo | | ~1,8 s |

## Migración (2026-09-27)
- Archivo: `supleprecios-backend/prisma/migrations/20260927000000_robust_variacion_porcentual/migration.sql`. **Aplicada en prod el 2026-09-27.** Se hizo con psycopg2 en una sola transacción (`lock_timeout` 10s), con registro manual en `_prisma_migrations` (checksum sha256 del archivo). **No** se usó `prisma migrate deploy`: el repo no tiene varias migraciones que sí están en prod, y podría intentar un reset.
- Recrea `mv_main_query_v2` con el cálculo nuevo, más sus dependientes `mv_active_brands`, `mv_click_stats` y `mv_active_subcategories`.
- `mv_active_subcategories` la usa `subcategorias.service.ts`, pero no existía en prod, así que ese endpoint fallaba.
- Incluye los índices FTS y trigram de `20260314_search_optimizations`. Esa migración se aplicó a mano en prod (checksum `manual`) y no está en el repo.
- `scripts/rebuild-views.js` quedó con la misma definición de v2.
- Validación en prod, solo lectura:
  - mismas 2.179 filas y mismas columnas, en el mismo orden;
  - `id_producto` único (el índice único sigue siendo válido);
  - 0,8 s de cálculo;
  - ofertas 387 → 44, mínimo -46,88%.
- Post-migración: step 7 refresca las 6 vistas OK (incluida `mv_active_subcategories`) y la búsqueda trigram usa `idx_mv_v2_trgm_producto`.
- `mv_main_query` (v1) quedó con la lógica antigua; el backend no la usa.

## Complementos en el pipeline
- [[Step 3 - Insercion Bulk]]: no insertar `precio <= 0` como precio (o marcarlo sin stock aparte).
- Limpiar en `historia_precios` los duplicados del mismo día y los 0 históricos.

Decathlon: 22 `producto_tienda` marcados `is_active=false` en prod (ver [[diagnostico-csv-vacios-2026-09]]).

Relacionado: [[Schema de Base de Datos]], [[Flujo de Datos]], [[notificaciones-precio-trigger]], [[bug-farmacia-knopp-precio-cero]]
