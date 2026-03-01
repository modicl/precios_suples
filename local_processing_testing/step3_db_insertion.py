import os
import sys
import unicodedata
import sqlalchemy as sa
import pandas as pd
import psycopg2.extras
from datetime import datetime

# Add root to path to find tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_display_text(value):
    """Return stripped string; empty string for non-str / NaN values."""
    if isinstance(value, str):
        return value.strip()
    return ""


def make_key(value):
    """
    Lowercase + strip + NFC unicode normalization.

    NFC normalisation ensures that strings like 'Proteína' match regardless of
    whether they were stored as NFC or NFD (DB vs CSV can differ silently).
    This is the critical fix for category/subcategory lookup mismatches.
    """
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value).lower().strip()
    return ""


def safe_url(value):
    """Return URL string or None (never the string 'nan')."""
    if pd.isna(value) or not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped if stripped else None


# ---------------------------------------------------------------------------
# Shared ID resolution
# ---------------------------------------------------------------------------

def _resolve_row_ids(row, brand_map, subcat_map, cat_map, fallback_map,
                     fallback_brand_id, db_brand_by_name=None, product_name=None):
    """
    Resolve (brand_id, subcat_id, cat_id) for a single DataFrame row.

    Returns (b_id, s_id, c_id).  s_id may be None if not found (caller should
    skip the row in that case).  Warnings about missing categories/subcategories
    are NOT emitted here — callers decide how to handle None s_id.

    Brand inheritance: if the row has no brand (resolves to fallback/N/D) and
    db_brand_by_name + product_name are provided, the brand is inherited from
    an existing DB product with the same normalized name.
    """
    b_id = brand_map.get(make_key(row['brand']), fallback_brand_id)

    # Brand inheritance: productos sin marca heredan la marca de un gemelo en BD
    if db_brand_by_name is not None and product_name is not None:
        if b_id == fallback_brand_id:
            inherited = db_brand_by_name.get(make_key(product_name))
            if inherited:
                b_id = inherited

    c_id = cat_map.get(make_key(row['category']))
    s_id = subcat_map.get(make_key(row['subcategory']))

    # Fallback: if subcategory not found but category is known, use its "Otros" subcat
    if not s_id and c_id:
        s_id = fallback_map.get(c_id)

    return b_id, s_id, c_id


# ---------------------------------------------------------------------------
# Bulk insert helper (psycopg2 native — mucho más eficiente en Neon)
# ---------------------------------------------------------------------------

def _bulk_execute_values(engine, query, values, page_size=1000):
    """
    Ejecuta un INSERT masivo usando psycopg2.execute_values.
    Genera un único INSERT multi-valor por página, minimizando round-trips
    a la BD remota (Neon).  Requiere cursor nativo, por eso usa raw_connection.
    """
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, query, values, page_size=page_size)
        raw_conn.commit()
    finally:
        raw_conn.close()


# ---------------------------------------------------------------------------
# Main insertion
# ---------------------------------------------------------------------------

def insert_data_bulk(engine, df, db_name="DB"):
    print(f"\n--- [{db_name}] Insertando {len(df)} registros (MODO BATCH OPTIMIZADO) ---")

    with engine.connect() as conn:

        # ------------------------------------------------------------------ #
        # 0. BUILD ID MAPS                                                     #
        # ------------------------------------------------------------------ #
        print("Cargando mapas de referencia (Categorias, Marcas, Tiendas)...")

        # Categorias
        cat_map = {
            make_key(row.nombre_categoria): row.id_categoria
            for row in conn.execute(sa.text("SELECT id_categoria, nombre_categoria FROM categorias")).fetchall()
        }

        # Subcategorias
        all_subs = conn.execute(sa.text(
            "SELECT id_subcategoria, nombre_subcategoria, id_categoria FROM subcategorias"
        )).fetchall()

        subcat_map    = {}  # key -> id_sub
        fallback_map  = {}  # id_cat -> id_sub ("Otros")

        for row in all_subs:
            subcat_map[make_key(row.nombre_subcategoria)] = row.id_subcategoria
            if row.nombre_subcategoria.lower().startswith("otros"):
                fallback_map[row.id_categoria] = row.id_subcategoria

        # Marcas — insert new ones first
        existing_brands_set = {
            make_key(row.nombre_marca)
            for row in conn.execute(sa.text("SELECT nombre_marca FROM marcas")).fetchall()
        }
        new_brands = [
            {"nombre_marca": clean_display_text(b)}
            for b in df['brand'].dropna().unique()
            if make_key(b) not in existing_brands_set
        ]
        if new_brands:
            conn.execute(
                sa.text("INSERT INTO marcas (nombre_marca) VALUES (:nombre_marca) ON CONFLICT DO NOTHING"),
                new_brands
            )
            conn.commit()

        brand_map = {
            make_key(row.nombre_marca): row.id_marca
            for row in conn.execute(sa.text("SELECT id_marca, nombre_marca FROM marcas")).fetchall()
        }
        fallback_brand_id = brand_map.get('n/d', 14)

        # Mapa de nombre_producto (normalizado) → id_marca para productos que ya
        # tienen marca real en la BD.  Se usa para que productos nuevos sin marca
        # hereden la marca de un gemelo ya existente (brand inheritance).
        nd_id = fallback_brand_id
        db_brand_by_name = {
            make_key(row.nombre_producto): row.id_marca
            for row in conn.execute(sa.text(
                "SELECT nombre_producto, id_marca FROM productos "
                "WHERE id_marca != :nd AND id_marca != 14"
            ), {"nd": nd_id}).fetchall()
        }

        # Tiendas — insert new ones first
        existing_shops_set = {
            make_key(row.nombre_tienda)
            for row in conn.execute(sa.text("SELECT nombre_tienda FROM tiendas")).fetchall()
        }
        new_shops = [
            {"nombre_tienda": clean_display_text(s)}
            for s in df['site_name'].dropna().unique()
            if make_key(s) not in existing_shops_set
        ]
        if new_shops:
            conn.execute(
                sa.text("INSERT INTO tiendas (nombre_tienda) VALUES (:nombre_tienda) ON CONFLICT DO NOTHING"),
                new_shops
            )
            conn.commit()

        shop_map = {
            make_key(row.nombre_tienda): row.id_tienda
            for row in conn.execute(sa.text("SELECT id_tienda, nombre_tienda FROM tiendas")).fetchall()
        }

        # ------------------------------------------------------------------ #
        # FASE 1: PRODUCTOS (Bulk Upsert)                                      #
        # ------------------------------------------------------------------ #
        print("Preparando lote de Productos...")

        products_dedup = {}   # (p_key, b_id, s_id) -> row dict
        skipped_no_subcat = 0
        skipped_cats = {}     # category value -> count, for warning

        for _, row in df.iterrows():
            p_display = row.get('normalized_name', row['product_name'])
            if pd.isna(p_display):
                p_display = row['product_name']
            p_display = clean_display_text(p_display)
            if not p_display:
                continue

            b_id, s_id, c_id = _resolve_row_ids(
                row, brand_map, subcat_map, cat_map, fallback_map, fallback_brand_id,
                db_brand_by_name=db_brand_by_name, product_name=p_display
            )

            if not s_id:
                skipped_no_subcat += 1
                cat_val = str(row.get('category', ''))
                skipped_cats[cat_val] = skipped_cats.get(cat_val, 0) + 1
                continue

            key = (make_key(p_display), b_id, s_id)
            if key not in products_dedup:
                products_dedup[key] = {
                    "nombre_producto":    p_display,
                    "url_imagen":         safe_url(row.get('image_url')),
                    "url_thumb_imagen":   safe_url(row.get('thumbnail_image_url')),
                    "id_marca":           b_id,
                    "id_subcategoria":    s_id,
                }

        if skipped_no_subcat:
            print(f"[WARNING] Fase 1: {skipped_no_subcat} filas saltadas — subcategoría no encontrada en DB.")
            for cat_val, n in sorted(skipped_cats.items(), key=lambda x: -x[1]):
                print(f"  categoria='{cat_val}': {n} filas")

        # ── Intra-batch brand inheritance ───────────────────────────────────
        # Build a map of normalized_name → real brand_id from products in THIS
        # batch that already have a known brand (not fallback/N/D).
        # Then propagate to any sibling in the batch that still has fallback.
        batch_brand_by_name: dict[str, int] = {}
        for (p_key, b_id, s_id), prod in products_dedup.items():
            if b_id != fallback_brand_id:
                batch_brand_by_name[p_key] = b_id

        if batch_brand_by_name:
            inherited_count = 0
            updated_dedup: dict = {}
            for (p_key, b_id, s_id), prod in products_dedup.items():
                if b_id == fallback_brand_id and p_key in batch_brand_by_name:
                    new_b_id = batch_brand_by_name[p_key]
                    new_key  = (p_key, new_b_id, s_id)
                    prod = dict(prod, id_marca=new_b_id)
                    updated_dedup[new_key] = prod
                    inherited_count += 1
                else:
                    updated_dedup[(p_key, b_id, s_id)] = prod
            if inherited_count:
                print(f"[Brand Inheritance] {inherited_count} producto(s) heredaron marca dentro del lote.")
                products_dedup = updated_dedup
                # Extend db_brand_by_name so Phases 2 & 3 resolve consistently
                db_brand_by_name.update(batch_brand_by_name)
        # ────────────────────────────────────────────────────────────────────

        batch_products = list(products_dedup.values())
        if batch_products:
            print(f"[{db_name}] Insertando/Actualizando {len(batch_products)} productos únicos...")
            try:
                _bulk_execute_values(engine, """
                    INSERT INTO productos (nombre_producto, url_imagen, url_thumb_imagen, id_marca, id_subcategoria)
                    VALUES %s
                    ON CONFLICT (nombre_producto, id_marca, id_subcategoria)
                    DO UPDATE SET url_imagen = EXCLUDED.url_imagen
                """, [
                    (p["nombre_producto"], p["url_imagen"], p["url_thumb_imagen"], p["id_marca"], p["id_subcategoria"])
                    for p in batch_products
                ])
                print(f"[{db_name}] Productos insertados correctamente.")
            except Exception as e:
                print(f"[{db_name}] [ERROR CRITICO] Fallo en bulk insert productos: {e}")

        # Retrieve product ID map
        print("Recuperando IDs de productos...")
        all_prods = conn.execute(sa.text(
            "SELECT id_producto, nombre_producto, id_marca, id_subcategoria FROM productos"
        )).fetchall()
        prod_id_map = {
            (make_key(row.nombre_producto), row.id_marca, row.id_subcategoria): row.id_producto
            for row in all_prods
        }
        print(f"Mapa de productos cargado: {len(prod_id_map)} entradas.")

        # ------------------------------------------------------------------ #
        # FASE 2: PRODUCTO_TIENDA (Bulk Upsert)                               #
        # ------------------------------------------------------------------ #
        print("Preparando lote de Producto-Tienda...")

        links_dedup    = {}
        miss_pid_count = 0

        for _, row in df.iterrows():
            p_display = row.get('normalized_name', row['product_name'])
            if pd.isna(p_display):
                p_display = row['product_name']
            p_display = clean_display_text(p_display)
            if not p_display:
                continue

            b_id, s_id, c_id = _resolve_row_ids(
                row, brand_map, subcat_map, cat_map, fallback_map, fallback_brand_id,
                db_brand_by_name=db_brand_by_name, product_name=p_display
            )
            t_id = shop_map.get(make_key(row['site_name']))

            if not s_id or not t_id:
                continue

            p_key = make_key(p_display)
            pid   = prod_id_map.get((p_key, b_id, s_id))
            if not pid:
                miss_pid_count += 1
                if miss_pid_count <= 5:
                    print(f"  [DEBUG MISS] PID no encontrado para: '{p_display}', BrandID={b_id}, SubID={s_id}")
                continue

            key = (pid, t_id)
            links_dedup[key] = {
                "id_producto": pid,
                "id_tienda":   t_id,
                "url_link":    row['link'],
                "descripcion": row.get('description', ''),
            }

        if miss_pid_count > 0:
            print(f"[WARNING] Fase 2: {miss_pid_count} filas saltadas — producto no encontrado (fallo Fase 1 → 2).")

        batch_links = list(links_dedup.values())
        if batch_links:
            print(f"[{db_name}] Insertando/Actualizando {len(batch_links)} enlaces...")
            try:
                _bulk_execute_values(engine, """
                    INSERT INTO producto_tienda (id_producto, id_tienda, url_link, descripcion, is_active, fecha_ultima_vista)
                    VALUES %s
                    ON CONFLICT (id_producto, id_tienda) DO UPDATE SET
                        url_link           = EXCLUDED.url_link,
                        is_active          = true,
                        fecha_ultima_vista = NOW()
                """, [
                    (lnk["id_producto"], lnk["id_tienda"], lnk["url_link"], lnk["descripcion"], True, None)
                    for lnk in batch_links
                ])
                print(f"[{db_name}] Producto-Tienda insertados correctamente.")
            except Exception as e:
                print(f"[{db_name}] [ERROR] Fallo en bulk insert producto_tienda: {e}")

        # ── Fase 2b: Desactivar productos que ya no aparecen en la tienda ──────
        # Para cada tienda presente en el batch, marcar is_active=false en los
        # producto_tienda que NO fueron vistos en este run.
        if batch_links:
            product_ids_by_tienda: dict[int, list[int]] = {}
            for lnk in batch_links:
                tid = lnk["id_tienda"]
                product_ids_by_tienda.setdefault(tid, []).append(lnk["id_producto"])

            raw_conn = engine.raw_connection()
            try:
                deactivated_total = 0
                with raw_conn.cursor() as cur:
                    for tid, active_pids in product_ids_by_tienda.items():
                        cur.execute("""
                            UPDATE producto_tienda
                            SET is_active = false
                            WHERE id_tienda = %s
                              AND id_producto != ALL(%s)
                              AND is_active = true
                        """, (tid, active_pids))
                        deactivated_total += cur.rowcount
                raw_conn.commit()
            finally:
                raw_conn.close()

            if deactivated_total:
                print(f"[{db_name}] [is_active] {deactivated_total} producto(s) marcados como inactivos (no encontrados en este run).")
        # ────────────────────────────────────────────────────────────────────────

        # Retrieve link ID map
        print("Recuperando IDs de enlaces...")
        all_links = conn.execute(sa.text(
            "SELECT id_producto_tienda, id_producto, id_tienda FROM producto_tienda"
        )).fetchall()
        link_id_map = {
            (row.id_producto, row.id_tienda): row.id_producto_tienda
            for row in all_links
        }
        print(f"Mapa de enlaces cargado: {len(link_id_map)} entradas.")

        # ------------------------------------------------------------------ #
        # FASE 3: PRECIOS (Bulk Insert)                                        #
        # ------------------------------------------------------------------ #
        print("Preparando lote de Precios...")

        # Sites where duplicate URLs for the same product can exist with
        # different prices (site-side data issue). For these, keep the lowest
        # price seen across all rows for the same id_producto_tienda.
        LOWEST_PRICE_SITES = {"allnutrition"}

        # ptid -> {precio, fecha_precio}  — used only for LOWEST_PRICE_SITES
        prices_dedup: dict = {}
        prices_batch: list = []
        miss_ptid_count = 0

        for _, row in df.iterrows():
            p_display = row.get('normalized_name', row['product_name'])
            if pd.isna(p_display):
                p_display = row['product_name']
            p_display = clean_display_text(p_display)
            if not p_display:
                continue

            b_id, s_id, c_id = _resolve_row_ids(
                row, brand_map, subcat_map, cat_map, fallback_map, fallback_brand_id,
                db_brand_by_name=db_brand_by_name, product_name=p_display
            )
            t_id = shop_map.get(make_key(row['site_name']))

            if not s_id or not t_id:
                continue

            p_key = make_key(p_display)
            pid   = prod_id_map.get((p_key, b_id, s_id))
            if not pid:
                continue

            ptid = link_id_map.get((pid, t_id))
            if not ptid:
                miss_ptid_count += 1
                if miss_ptid_count <= 3:
                    print(f"  [DEBUG MISS PTID] PID={pid}, TID={t_id} no encontrado en mapa de enlaces.")
                continue

            fecha = row['date'] if pd.notna(row.get('date')) else datetime.now()
            site_key = make_key(str(row.get('site_name', '')))

            if site_key in LOWEST_PRICE_SITES:
                # Accumulate: keep only the lowest price for this ptid
                existing = prices_dedup.get(ptid)
                if existing is None or row['price'] < existing['precio']:
                    prices_dedup[ptid] = {
                        "id_producto_tienda": ptid,
                        "precio":             row['price'],
                        "fecha_precio":       fecha,
                    }
            else:
                prices_batch.append({
                    "id_producto_tienda": ptid,
                    "precio":             row['price'],
                    "fecha_precio":       fecha,
                })

        # Merge deduped lowest-price entries into the main batch
        if prices_dedup:
            print(f"  [{', '.join(LOWEST_PRICE_SITES)}] deduplicados por precio mínimo: {len(prices_dedup)} entradas.")
            prices_batch.extend(prices_dedup.values())

        if miss_ptid_count > 0:
            print(f"[WARNING] Fase 3: {miss_ptid_count} precios saltados — enlace no encontrado (fallo Fase 2 → 3).")

        if prices_batch:
            print(f"[{db_name}] Eliminando precios del día de hoy antes de reinsertar...")
            conn.execute(sa.text("DELETE FROM historia_precios WHERE fecha_precio::date = CURRENT_DATE"))
            conn.commit()

            print(f"[{db_name}] Insertando {len(prices_batch)} precios...")
            try:
                _bulk_execute_values(engine, """
                    INSERT INTO historia_precios (id_producto_tienda, precio, fecha_precio)
                    VALUES %s
                """, [
                    (hp["id_producto_tienda"], hp["precio"], hp["fecha_precio"])
                    for hp in prices_batch
                ])
                print(f"[{db_name}] Historial de precios insertado correctamente.")
            except Exception as e:
                print(f"[{db_name}] [ERROR] Fallo en bulk insert historia_precios: {e}")

    print("Proceso Batch Finalizado.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print("--- PASO 3: Inserción en Base de Datos (BATCH) ---")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_csv   = os.path.join(current_dir, "data", "2_normalized", "latest_normalized.csv")

    if not os.path.exists(input_csv):
        print(f"Error: No se encontró {input_csv}. Ejecuta el Paso 2 primero.")
        return

    df      = pd.read_csv(input_csv)
    targets = get_targets()
    print(f"Destinos de BD encontrados: {[t['name'] for t in targets]}")

    for target in targets:
        db_name = target['name']
        try:
            engine = sa.create_engine(target['url'])
            insert_data_bulk(engine, df, db_name=db_name)
        except Exception as e:
            print(f"[ERROR FATAL] {db_name}: {e}")


if __name__ == "__main__":
    main()
