# Step 3: precios perdidos cuando el CSV trae varios días

Relacionado: [[etl/Step 3 - Insercion Bulk]] · [[bugs/step3-circular-url-swap-fk-violation]] · [[scraping/SharedSeenUrls]] · [[arquitectura/Flujo de Datos]]

## Síntoma

En comparafit.cl aparecían ofertas con precio de hace días (o falsas). Auditoría en producción
(2026-10-05): 87 ofertas de `mv_main_query_v2` con último precio del 2-oct aunque el enlace
figuraba activo y "visto" el 5-oct (43 AllNutrition, 41 ChileSuplementos, 3 otras). La vista
materializada estaba bien refrescada; el dato venía mal desde el step 3.

## Causa raíz

El 5-oct `latest_normalized.csv` contenía **dos días de scraping** (2-oct y 5-oct) por tienda y
el step 3 asumía uno solo:

1. **AllNutrition** (`LOWEST_PRICE_SITES`): el dedup guardaba el precio mínimo por enlace entre
   *todas* las filas, sin mirar el día. Con precios iguales o más bajos el 2-oct, el precio del
   5-oct se descartaba. Si el precio había subido, quedaba el viejo y la oferta era falsa
   (ej. 1.390 el 2-oct vs 1.590 real el 5-oct).
2. **ChileSuplementos**: el 5-oct el producto vino solo en la categoría "Ofertas"; el 2-oct, en
   su categoría real. La Fase 2 dejaba ganar la fila del 2-oct (prioridad de URL), pero la Fase 3
   no repetía esa decisión: buscaba el enlace del producto "Ofertas", no lo encontraba y saltaba
   el precio ("N precios saltados — enlace no encontrado").
3. **Desactivación** (Fase 2b): las URLs de un CSV viejo contaban como "vistas hoy", así que un
   producto que ya no existía podía seguir activo.

No se verificó por qué los CSV del 2-oct seguían en `raw_data/` el 5-oct.

## Arreglo

Lógica pura en `shared/step3_logic.py` con tests en `tests/test_step3_logic.py`
(`python -m unittest discover -s tests -v`):

- `latest_scrape_mask`: el scrapeo más reciente de cada tienda (por día) define el **estado
  actual**: productos, enlaces, URLs vistas y desactivación. Los días anteriores **solo rellenan
  `historia_precios`**.
- `dedup_links`: dedup por URL/producto (misma regla de antes) devolviendo además `url_winner`.
- `dedup_prices`: un precio por **(enlace, día)**, nunca entre días. Fase 3 envía el precio de
  una URL repetida al enlace ganador de la Fase 2 (solo en el scrapeo reciente).
- Inserción de precios **idempotente**: temp table + `NOT EXISTS (id_producto_tienda,
  fecha_precio)`, así reprocesar un CSV no duplica historia.

## Pendiente / a vigilar

- El 5-oct ChileSuplementos Part1/Part2 no capturó ~41 productos que sí salieron en Ofertas
  (Part3). Con el arreglo esos productos se reasignan temporalmente al producto "Ofertas" hasta
  que se vuelvan a ver en su categoría real. Revisar por qué Part1/Part2 los pierden.
- Una tienda cuyo único CSV es antiguo sigue contando como "vista" (comportamiento previo).
- Reparar los precios del 5-oct ya perdidos requiere escribir en Neon; si no, se corrigen solos
  en el próximo run.
