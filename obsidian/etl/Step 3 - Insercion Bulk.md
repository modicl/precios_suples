# Step 3 — Inserción Bulk en Base de Datos

**Archivo:** `local_processing_testing/step3_db_insertion.py`
**Función principal:** `insert_data_bulk(engine, df, db_name)`

---

## Contexto: Por qué se reescribió (Feb 2026)

La versión anterior usaba loops Python con `INSERT ... ON CONFLICT` fila por fila.
Con ~10.000 productos scrapeados contra Neon (latencia ~40ms por query), el paso tomaba ~15 minutos.

**Solución:** reescritura completa a `psycopg2.execute_values` con tablas temporales.
**Resultado:** ~2 segundos. Factor de mejora: ~450x.

---

## Arquitectura del Paso

### Helper central: `_bulk_execute_values(engine, query, values, page_size=1000)`
Obtiene una `raw_connection` de SQLAlchemy (necesario para usar la API nativa de psycopg2) y ejecuta un `INSERT` multi-valor en lotes de 1000 filas. Minimiza round-trips a Neon.

---

## Fases de Ejecución

### Fase 0: Construcción de Mapas de IDs

Carga en memoria:
- `cat_map`: `nombre_categoria (normalizado)` → `id_categoria`
- `subcat_map`: `nombre_subcategoria` → `id_subcategoria`
- `fallback_map`: `id_categoria` → `id_subcategoria` de "Otros" (fallback si subcategoría no existe)
- `brand_map`: `nombre_marca` → `id_marca`
- `shop_map`: `nombre_tienda` → `id_tienda`
- `db_brand_by_name`: `nombre_producto` → `id_marca` (para brand inheritance desde DB)

**Normalización de claves:** función `make_key()` — lowercase + strip + NFC unicode. Crítico para evitar mismatches silenciosos entre CSV (NFD) y DB (NFC).

---

### Identidad de producto (2026-11-11)

La identidad de un producto es **(nombre normalizado, marca)**, sin subcategoría ni tildes/espacios (`identity_key` en `shared/step3_logic.py`). Antes la clave era `(nombre, marca, subcategoría)`: el mismo producto clasificado en dos subcategorías nacía como dos filas y el [[Step 4 - Deduplicacion]] las fusionaba y borraba en cada corrida.

Ahora la Fase 1 llama a `resolve_products` **antes** de insertar:
- si la identidad ya existe, se reutiliza su id (el maestro se elige con el mismo puntaje del step 4: categoría tipo Ofertas/Packs −100, nombre no todo minúsculas +20, empate → id menor);
- dentro del lote gana la mejor subcategoría;
- un maestro atrapado en Packs/Ofertas sube a la categoría normal si el lote la trae (con guarda contra la clave única);
- Fases 2 y 3 buscan el id por identidad. Si una identidad trae dos URLs para la misma tienda, el enlace conserva la de subcategoría específica y solo esa URL aporta precio.

**Validación (copia temporal de la BD local, CSV del 10-nov):** el flujo viejo dejaba 282 grupos duplicados tras el step 3 (el step 4 los borraba: 296 productos); el nuevo deja 0 y es idempotente (segunda corrida: 0 productos nuevos). Contra "viejo + step 4": 0 productos con distinta subcategoría, 0 enlaces apuntando a otro producto, +18 fusiones por tildes/espacios, y **+85 enlaces activos**: en el flujo viejo el step 4 borraba el enlace nuevo (URL vigente) del duplicado cuando el maestro ya tenía enlace en esa tienda, y el maestro quedaba con la URL vieja desactivada.

---

### Fase 1: Productos (Bulk Upsert)

```sql
INSERT INTO productos (nombre_producto, url_imagen, url_thumb_imagen, id_marca, id_subcategoria)
VALUES %s
ON CONFLICT (nombre_producto, id_marca, id_subcategoria)
DO UPDATE SET url_imagen = CASE
    WHEN EXCLUDED.url_imagen IS NOT NULL AND EXCLUDED.url_imagen != ''
    THEN EXCLUDED.url_imagen
    ELSE productos.url_imagen
END
```

**Brand Inheritance:** Si un producto no tiene marca (`N/D`):
1. Busca en `db_brand_by_name` si ya existe en la BD con marca real → hereda.
2. Busca en el propio batch si hay un gemelo con marca → hereda (intra-batch inheritance).
Esto evita que el mismo producto aparezca duplicado con y sin marca.

---

### Fase 2: Producto_Tienda (Bulk Upsert)

```sql
INSERT INTO producto_tienda (id_producto, id_tienda, url_link, descripcion, is_active, fecha_ultima_vista)
VALUES %s
ON CONFLICT (id_producto, id_tienda) DO UPDATE SET
    url_link = EXCLUDED.url_link,
    is_active = true,
    fecha_ultima_vista = NOW()
```

**Resolución de conflictos por URL duplicada:**
- `LOW_PRIORITY_SUBCATS = {"packs", "ofertas", "promo", "bundle", "outlet"}` — si la misma URL aparece en dos categorías, gana la más específica (no-pack/no-oferta).

**URL Remap (tabla temporal):**
Cuando la tienda reutiliza una URL para un producto diferente (cambio de producto en la misma página):

- **Caso A1**: El nuevo producto NO tiene fila para esa tienda → `UPDATE id_producto` al nuevo.
- **Caso A2**: El nuevo producto YA tiene fila → reasignar `historia_precios` del huérfano al winner, luego eliminar el huérfano.

Implementado con una tabla `TEMP TABLE _batch_links ON COMMIT DROP` + CTEs para resolver en una sola ida a la BD.

**Desactivación de productos:** Al final de Fase 2, para cada tienda en el batch, todos los `producto_tienda` con URLs que NO aparecieron en este run se marcan `is_active = false`. Cuando la URL reaparece en un scrapeo posterior, el upsert la vuelve a poner `is_active = true`. Una tienda sin filas en el CSV (0 productos) no entra al bucle: sus productos no se tocan.

**Protección contra corridas parciales** (`should_skip_deactivation` en `shared/step3_logic.py`): si la corrida trae menos del 50% de los productos hoy activos de la tienda (y la tienda tiene ≥10 activos), NO se desactiva nada de esa tienda y se imprime una advertencia. Caso real: OneNutrition trajo 53 de ~395 productos el 2026-10-23 y habría desactivado ~340. Para forzar la desactivación cuando una tienda realmente redujo su catálogo: `STEP3_FORCE_DEACTIVATION=1`. Umbrales ajustables en `DEACTIVATION_MIN_RATIO` / `DEACTIVATION_MIN_BASE`. Ver [[bug-strongest-onenutrition-0-filas-2026-11]].

---

### Fase 3: Historia de Precios (Bulk Insert)

```sql
INSERT INTO historia_precios (id_producto_tienda, precio, fecha_precio)
VALUES %s
```

Sin `ON CONFLICT` — siempre inserta (serie de tiempo append-only).

**Excepción:** `LOWEST_PRICE_SITES = {"allnutrition"}` — para estas tiendas donde una misma URL puede aparecer varias veces con distintos precios, se guarda solo el precio mínimo del run.

---

## Notas de Diseño

- `safe_url()`: nunca guarda el string `"nan"` como URL, lo convierte a `None`.
- `clean_display_text()`: strip de strings; retorna `""` para NaN/no-string.
- Flujo de fallo en cascada: si Fase 1 falla → Fase 2 muestra warnings "PID no encontrado" → Fase 3 sin precios. El script no aborta, solo reporta.

---

## Referencias

- [[arquitectura/Schema de Base de Datos]]
- [[arquitectura/Flujo de Datos]]
- [[etl/Normalizacion Fuzzy]]
