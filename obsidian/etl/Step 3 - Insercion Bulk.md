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

**Desactivación de productos:** Al final de Fase 2, para cada tienda en el batch, todos los `producto_tienda` con URLs que NO aparecieron en este run se marcan `is_active = false`.

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
