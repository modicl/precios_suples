import os
import sys
import shutil
import unicodedata
import sqlalchemy as sa
import pandas as pd
import psycopg2.extras
from datetime import datetime

# Add root to path to find tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from shared.db_multiconnect import get_targets
from shared.step3_logic import (
    latest_scrape_mask, dedup_links, dedup_prices,
    should_skip_deactivation, DEACTIVATION_MIN_RATIO,
    identity_key, master_index, resolve_products,
)


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

    # El CSV puede traer varios días de scraping por tienda (runs anteriores sin
    # archivar). Estado actual (productos, enlaces, URLs vistas) = solo el scrapeo
    # más reciente de cada tienda; los días anteriores únicamente rellenan
    # historia_precios (Fase 3). Ver shared/step3_logic.py.
    df_all      = df
    latest_mask = latest_scrape_mask(df_all)
    df          = df_all[latest_mask]
    n_older     = len(df_all) - len(df)
    if n_older:
        print(f"[{db_name}] {n_older} fila(s) de scrapeos anteriores: solo rellenan historia_precios; "
              f"estado actual y desactivación usan {len(df)} fila(s) del scrapeo más reciente por tienda.")

    with engine.connect() as conn:

        # ------------------------------------------------------------------ #
        # 0. BUILD ID MAPS                                                     #
        # ------------------------------------------------------------------ #
        print("Cargando mapas de referencia (Categorias, Marcas, Tiendas)...")

        # Categorias
        cat_rows = conn.execute(sa.text("SELECT id_categoria, nombre_categoria FROM categorias")).fetchall()
        cat_map = {make_key(row.nombre_categoria): row.id_categoria for row in cat_rows}
        cat_name_by_id = {row.id_categoria: row.nombre_categoria for row in cat_rows}
        known_categories = set(cat_name_by_id.values())

        # Subcategorias
        all_subs = conn.execute(sa.text(
            "SELECT id_subcategoria, nombre_subcategoria, id_categoria FROM subcategorias"
        )).fetchall()

        subcat_map    = {}  # key -> id_sub
        fallback_map  = {}  # id_cat -> id_sub ("Otros")
        cat_name_by_sub = {row.id_subcategoria: cat_name_by_id.get(row.id_categoria) for row in all_subs}

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

        # ── Identidad (nombre normalizado, marca) SIN subcategoría ─────────────
        # Si el producto ya existe se reutiliza su id (no se crea otra fila por subcategoría);
        # dentro del lote gana la mejor subcategoría (mismo puntaje que el step 4).
        existing_rows = [
            {"id": r.id_producto, "nombre": r.nombre_producto, "id_marca": r.id_marca,
             "id_subcategoria": r.id_subcategoria, "cat_name": cat_name_by_sub.get(r.id_subcategoria)}
            for r in conn.execute(sa.text(
                "SELECT id_producto, nombre_producto, id_marca, id_subcategoria FROM productos"
            )).fetchall()
        ]
        plan = resolve_products(
            [{"nombre": prod["nombre_producto"], "id_marca": prod["id_marca"],
              "id_subcategoria": prod["id_subcategoria"],
              "cat_name": cat_name_by_sub.get(prod["id_subcategoria"]),
              "url_imagen": prod["url_imagen"], "url_thumb_imagen": prod["url_thumb_imagen"]}
             for prod in products_dedup.values()],
            existing_rows, known_categories,
        )
        batch_products = [
            {"nombre_producto": c["nombre"], "url_imagen": c["url_imagen"],
             "url_thumb_imagen": c["url_thumb_imagen"], "id_marca": c["id_marca"],
             "id_subcategoria": c["id_subcategoria"]}
            for c in plan["insert"]
        ] + [
            # maestro existente: pasa por el mismo upsert (conflicto exacto) para refrescar imágenes
            {"nombre_producto": t["nombre"], "url_imagen": t["url_imagen"],
             "url_thumb_imagen": t["url_thumb_imagen"], "id_marca": t["id_marca"],
             "id_subcategoria": t["id_subcategoria"]}
            for t in plan["touch"]
        ]
        print(f"[{db_name}] [Identidad] {len(plan['insert'])} producto(s) nuevo(s), "
              f"{len(plan['touch'])} reutilizado(s), {plan['collapsed']} fila(s) del lote colapsadas "
              f"(mismo nombre+marca en otra subcategoría), "
              f"{len(plan['subcat_updates'])} subcategoría(s) mejorada(s).")
        # ────────────────────────────────────────────────────────────────────────
        if batch_products:
            print(f"[{db_name}] Insertando/Actualizando {len(batch_products)} productos únicos...")
            try:
                _bulk_execute_values(engine, """
                    INSERT INTO productos (nombre_producto, url_imagen, url_thumb_imagen, id_marca, id_subcategoria)
                    VALUES %s
                    ON CONFLICT (nombre_producto, id_marca, id_subcategoria)
                    DO UPDATE SET
                        url_imagen = CASE
                            WHEN EXCLUDED.url_imagen LIKE '%%suplescrapper-images%%'
                                THEN EXCLUDED.url_imagen
                            WHEN (productos.url_imagen IS NULL OR productos.url_imagen = '')
                                AND EXCLUDED.url_imagen IS NOT NULL AND EXCLUDED.url_imagen != ''
                                THEN EXCLUDED.url_imagen
                            ELSE productos.url_imagen
                        END,
                        url_thumb_imagen = CASE
                            WHEN EXCLUDED.url_thumb_imagen LIKE '%%suplescrapper-images%%'
                                THEN EXCLUDED.url_thumb_imagen
                            WHEN (productos.url_thumb_imagen IS NULL OR productos.url_thumb_imagen = '')
                                AND EXCLUDED.url_thumb_imagen IS NOT NULL AND EXCLUDED.url_thumb_imagen != ''
                                THEN EXCLUDED.url_thumb_imagen
                            ELSE productos.url_thumb_imagen
                        END
                """, [
                    (p["nombre_producto"], p["url_imagen"], p["url_thumb_imagen"], p["id_marca"], p["id_subcategoria"])
                    for p in batch_products
                ])
                print(f"[{db_name}] Productos insertados correctamente.")
            except Exception as e:
                print(f"[{db_name}] [ERROR CRITICO] Fallo en bulk insert productos: {e}")

        # Sube de subcategoría a un producto atrapado en Packs/Ofertas cuando el lote lo trae en
        # una categoría normal. Protegido contra choque con la clave única (nombre, marca, subcat).
        if plan["subcat_updates"]:
            raw_sub = engine.raw_connection()
            try:
                with raw_sub.cursor() as cur:
                    for pid_up, new_sub in plan["subcat_updates"]:
                        cur.execute("""
                            UPDATE productos p SET id_subcategoria = %s
                            WHERE p.id_producto = %s
                              AND NOT EXISTS (
                                  SELECT 1 FROM productos q
                                  WHERE q.id_marca = p.id_marca
                                    AND q.nombre_producto = p.nombre_producto
                                    AND q.id_subcategoria = %s)
                        """, (new_sub, pid_up, new_sub))
                raw_sub.commit()
            finally:
                raw_sub.close()

        # Retrieve product ID map: identidad (nombre normalizado, marca) -> id del maestro
        print("Recuperando IDs de productos...")
        all_prods = [
            {"id": r.id_producto, "nombre": r.nombre_producto, "id_marca": r.id_marca,
             "id_subcategoria": r.id_subcategoria, "cat_name": cat_name_by_sub.get(r.id_subcategoria)}
            for r in conn.execute(sa.text(
                "SELECT id_producto, nombre_producto, id_marca, id_subcategoria FROM productos"
            )).fetchall()
        ]
        prod_id_by_ident = {k: r["id"] for k, r in master_index(all_prods, known_categories).items()}
        print(f"Mapa de productos cargado: {len(prod_id_by_ident)} identidades.")

        # ------------------------------------------------------------------ #
        # FASE 2: PRODUCTO_TIENDA (Bulk Upsert)                               #
        # ------------------------------------------------------------------ #
        print("Preparando lote de Producto-Tienda...")

        link_rows      = []   # filas candidatas, en orden del CSV; el dedup está en shared/step3_logic.py
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

            pid = prod_id_by_ident.get((identity_key(p_display), b_id))
            if not pid:
                miss_pid_count += 1
                if miss_pid_count <= 5:
                    print(f"  [DEBUG MISS] PID no encontrado para: '{p_display}', BrandID={b_id}, SubID={s_id}")
                continue

            link_rows.append({
                "id_producto": pid,
                "id_tienda":   t_id,
                "url_link":    row['link'],
                "descripcion": row.get('description', ''),
                "subcategory": row.get('subcategory', ''),
            })

        # ── Dedup por (tid, url) y por (pid, tid) ──
        # Misma URL en la misma tienda con distinto pid ocurre cuando el scraper recoge el
        # mismo producto desde dos categorías (ej. Proteinas y Packs): gana la categoría más
        # específica (no-pack, no-oferta); con igual prioridad queda la primera.
        # url_winner se reutiliza en la Fase 3 para que el precio vaya al enlace ganador.
        links_dedup, url_winner, url_conflict_count = dedup_links(link_rows)

        if url_conflict_count > 0:
            print(f"[WARNING] Fase 2: {url_conflict_count} URL(s) duplicada(s) en el CSV (misma URL, distinto producto) — se conservó la categoría más específica.")
        if miss_pid_count > 0:
            print(f"[WARNING] Fase 2: {miss_pid_count} filas saltadas — producto no encontrado (fallo Fase 1 → 2).")

        batch_links = list(links_dedup.values())
        if batch_links:
            print(f"[{db_name}] Insertando/Actualizando {len(batch_links)} enlaces...")

            # ── Paso A: URL remap (bulk) ─────────────────────────────────────
            # Detecta y resuelve filas en producto_tienda cuya URL coincide con
            # una del batch pero tiene distinto id_producto. Dos casos:
            #
            # A1) El producto destino NO tiene fila para esa tienda todavía:
            #     → UPDATE id_producto al nuevo (tienda reutilizó la URL).
            #
            # A2) El producto destino YA tiene fila para esa tienda:
            #     → Reasignar historia_precios al winner y eliminar el huérfano
            #       (era un pack/bundle creado por el scraper anterior).
            #
            # Implementación: tabla temporal + CTEs — una sola ida/vuelta a la BD.
            raw_conn_remap = engine.raw_connection()
            try:
                with raw_conn_remap.cursor() as cur:
                    # 1. Crear tabla temporal con los datos del batch
                    cur.execute("""
                        CREATE TEMP TABLE _batch_links (
                            id_producto  INTEGER,
                            id_tienda    INTEGER,
                            url_link     TEXT,
                            descripcion  TEXT
                        ) ON COMMIT DROP
                    """)
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO _batch_links (id_producto, id_tienda, url_link, descripcion)
                        VALUES %s
                    """, [(lnk["id_producto"], lnk["id_tienda"], lnk["url_link"], lnk["descripcion"] or "")
                          for lnk in batch_links])

                    # 2. A2: reasignar historia_precios de huérfanos cuyo destino ya existe
                    cur.execute("""
                        UPDATE historia_precios hp
                        SET id_producto_tienda = winner.id_producto_tienda
                        FROM producto_tienda orphan
                        JOIN _batch_links b
                          ON orphan.id_tienda = b.id_tienda
                         AND orphan.url_link  = b.url_link
                         AND orphan.id_producto != b.id_producto
                        JOIN producto_tienda winner
                          ON winner.id_producto = b.id_producto
                         AND winner.id_tienda   = b.id_tienda
                        WHERE hp.id_producto_tienda = orphan.id_producto_tienda
                    """)
                    hp_migrated = cur.rowcount

                    # 3a. A2: nullificar URL de huérfanos que aún tienen historia_precios
                    # (caso swap circular: la migración del paso 2 movió historia de ambos
                    # lados simultáneamente, dejando a ambos con registros. No se puede
                    # borrar sin violar la FK. Se pone url_link=NULL para liberar el slot
                    # de uq_tienda_url y que el paso B asigne la URL correcta.)
                    cur.execute("""
                        UPDATE producto_tienda orphan
                        SET url_link  = NULL,
                            is_active = false
                        FROM _batch_links b
                        JOIN producto_tienda winner
                          ON winner.id_producto = b.id_producto
                         AND winner.id_tienda   = b.id_tienda
                        WHERE orphan.id_tienda    = b.id_tienda
                          AND orphan.url_link     = b.url_link
                          AND orphan.id_producto != b.id_producto
                          AND EXISTS (
                            SELECT 1 FROM historia_precios hp
                            WHERE hp.id_producto_tienda = orphan.id_producto_tienda
                          )
                    """)
                    url_nullified = cur.rowcount

                    # 3b. A2: eliminar huérfanos que no tienen historia_precios
                    cur.execute("""
                        DELETE FROM producto_tienda orphan
                        USING _batch_links b
                        JOIN producto_tienda winner
                          ON winner.id_producto = b.id_producto
                         AND winner.id_tienda   = b.id_tienda
                        WHERE orphan.id_tienda    = b.id_tienda
                          AND orphan.url_link     = b.url_link
                          AND orphan.id_producto != b.id_producto
                          AND NOT EXISTS (
                            SELECT 1 FROM historia_precios hp
                            WHERE hp.id_producto_tienda = orphan.id_producto_tienda
                          )
                    """)
                    url_deleted = cur.rowcount

                    # 4. A1: reasignar URL al producto correcto cuando el destino NO existe aún
                    cur.execute("""
                        UPDATE producto_tienda pt
                        SET id_producto        = b.id_producto,
                            descripcion        = b.descripcion,
                            is_active          = true,
                            fecha_ultima_vista = NOW()
                        FROM _batch_links b
                        LEFT JOIN producto_tienda dest
                          ON dest.id_producto = b.id_producto
                         AND dest.id_tienda   = b.id_tienda
                        WHERE pt.id_tienda    = b.id_tienda
                          AND pt.url_link     = b.url_link
                          AND pt.id_producto != b.id_producto
                          AND dest.id_producto IS NULL
                    """)
                    url_updated = cur.rowcount

                raw_conn_remap.commit()
            finally:
                raw_conn_remap.close()

            if url_updated:
                print(f"[{db_name}] [URL remap] {url_updated} enlace(s) reasignados a nuevo producto (URL reutilizada por la tienda).")
            if url_nullified:
                print(f"[{db_name}] [URL remap] {url_nullified} enlace(s) huérfanos con historial: URL nullificada (swap circular detectado).")
            if url_deleted:
                print(f"[{db_name}] [URL remap] {url_deleted} enlace(s) huérfanos sin historial eliminados ({hp_migrated} precios reasignados al winner).")

            # ── Paso B: INSERT / UPDATE por (id_producto, id_tienda) ─────────
            # Después del remap ya no hay riesgo de colisión en uq_tienda_url.
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
        # producto_tienda que NO fueron vistos en este run (por URL).
        if batch_links:
            urls_by_tienda: dict[int, list[str]] = {}
            for lnk in batch_links:
                tid = lnk["id_tienda"]
                urls_by_tienda.setdefault(tid, []).append(lnk["url_link"])

            raw_conn = engine.raw_connection()
            try:
                deactivated_total = 0
                force_deactivation = os.getenv("STEP3_FORCE_DEACTIVATION") == "1"
                with raw_conn.cursor() as cur:
                    for tid, active_urls in urls_by_tienda.items():
                        # Protección contra corridas parciales: si esta corrida trae mucho menos
                        # de lo que la tienda tiene activo, no se desactiva nada de esa tienda.
                        cur.execute(
                            "SELECT count(*), max(t.nombre_tienda) FROM producto_tienda pt "
                            "JOIN tiendas t USING (id_tienda) "
                            "WHERE pt.id_tienda = %s AND pt.is_active = true", (tid,))
                        active_now, store_name = cur.fetchone()
                        seen_now = len(set(active_urls))
                        if not force_deactivation and should_skip_deactivation(seen_now, active_now):
                            print(f"[{db_name}] [is_active] ⚠ {store_name}: corrida parcial "
                                  f"({seen_now} URLs vs {active_now} activos, < {DEACTIVATION_MIN_RATIO:.0%}). "
                                  f"NO se desactiva nada de esta tienda. "
                                  f"(Si la tienda realmente redujo su catálogo: STEP3_FORCE_DEACTIVATION=1)")
                            continue
                        cur.execute("""
                            UPDATE producto_tienda
                            SET is_active = false
                            WHERE id_tienda = %s
                              AND url_link != ALL(%s)
                              AND is_active = true
                        """, (tid, active_urls))
                        deactivated_total += cur.rowcount
                raw_conn.commit()
            finally:
                raw_conn.close()

            if deactivated_total:
                print(f"[{db_name}] [is_active] {deactivated_total} producto(s) marcados como inactivos (URL no encontrada en este run).")
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

        # Tiendas donde la misma URL puede venir duplicada con precios distintos
        # (problema del sitio): por (enlace, día) se conserva el precio menor.
        # El dedup es por día, nunca entre días (si no, se pierde el precio más nuevo).
        LOWEST_PRICE_SITES = {"allnutrition"}

        price_rows: list = []
        miss_ptid_count = 0
        secondary_url_prices = 0

        # Se recorren TODOS los días: el scrapeo más reciente define el estado actual y
        # los anteriores rellenan historial (la inserción es idempotente, ver más abajo).
        for idx, row in df_all.iterrows():
            is_latest = bool(latest_mask.loc[idx])

            p_display = row.get('normalized_name', row['product_name'])
            if pd.isna(p_display):
                p_display = row['product_name']
            p_display = clean_display_text(p_display)
            if not p_display:
                continue

            # Marca ausente en el mapa: solo puede ocurrir en filas de días anteriores
            # (Fase 0 crea las marcas del scrapeo reciente). Se salta para no caer en N/D.
            if not is_latest and make_key(row['brand']) not in brand_map:
                continue

            b_id, s_id, c_id = _resolve_row_ids(
                row, brand_map, subcat_map, cat_map, fallback_map, fallback_brand_id,
                db_brand_by_name=db_brand_by_name, product_name=p_display
            )
            t_id = shop_map.get(make_key(row['site_name']))

            if not s_id or not t_id:
                continue

            pid = prod_id_by_ident.get((identity_key(p_display), b_id))
            if not pid:
                continue

            # Misma URL en dos categorías el mismo día: el precio va al enlace ganador
            # de la Fase 2 (si no, la fila "Ofertas" buscaba un enlace que no existe y
            # el precio se perdía). Solo para el scrapeo reciente: en días anteriores una
            # URL reutilizada por la tienda podría pertenecer a otro producto.
            if is_latest:
                winner = url_winner.get((t_id, make_key(row['link'])))
                if winner is not None:
                    pid = winner

            # Un producto ahora puede traer dos filas (misma identidad en dos subcategorías) con
            # URLs distintas para la misma tienda: solo la URL que quedó como enlace aporta precio.
            if is_latest:
                kept = links_dedup.get((pid, t_id))
                if kept is not None and make_key(kept["url_link"]) != make_key(row['link']):
                    secondary_url_prices += 1
                    continue

            ptid = link_id_map.get((pid, t_id))
            if not ptid:
                # En días anteriores es normal que el enlace ya no exista (producto retirado);
                # solo se avisa por filas del scrapeo reciente, donde sí sería un fallo Fase 2 → 3.
                if is_latest:
                    miss_ptid_count += 1
                    if miss_ptid_count <= 3:
                        print(f"  [DEBUG MISS PTID] PID={pid}, TID={t_id} no encontrado en mapa de enlaces.")
                continue

            price_rows.append({
                "id_producto_tienda": ptid,
                "precio":             row['price'],
                "fecha_precio":       row['date'] if pd.notna(row.get('date')) else datetime.now(),
                "site_key":           make_key(str(row.get('site_name', ''))),
            })

        if secondary_url_prices:
            print(f"  [Identidad] {secondary_url_prices} precio(s) de URLs secundarias omitidos (el producto conserva un solo enlace por tienda).")
        prices_batch = dedup_prices(price_rows, LOWEST_PRICE_SITES)
        if len(prices_batch) != len(price_rows):
            print(f"  Precios deduplicados por (enlace, día): {len(price_rows)} -> {len(prices_batch)}.")

        if miss_ptid_count > 0:
            print(f"[WARNING] Fase 3: {miss_ptid_count} precios saltados — enlace no encontrado (fallo Fase 2 → 3).")

        if prices_batch:
            print(f"[{db_name}] Insertando hasta {len(prices_batch)} precios (se omiten los ya existentes)...")
            try:
                raw_conn_hp = engine.raw_connection()
                try:
                    with raw_conn_hp.cursor() as cur:
                        cur.execute("""
                            CREATE TEMP TABLE _batch_prices (
                                id_producto_tienda INTEGER,
                                precio             INTEGER,
                                fecha_precio       TIMESTAMP
                            ) ON COMMIT DROP
                        """)
                        psycopg2.extras.execute_values(cur, """
                            INSERT INTO _batch_prices (id_producto_tienda, precio, fecha_precio)
                            VALUES %s
                        """, [
                            (hp["id_producto_tienda"], hp["precio"], hp["fecha_precio"])
                            for hp in prices_batch
                        ], page_size=1000)
                        # Idempotente: reprocesar un CSV (o rellenar días anteriores) no
                        # duplica filas ya guardadas para el mismo enlace y fecha.
                        cur.execute("""
                            INSERT INTO historia_precios (id_producto_tienda, precio, fecha_precio)
                            SELECT b.id_producto_tienda, b.precio, b.fecha_precio
                            FROM _batch_prices b
                            WHERE NOT EXISTS (
                                SELECT 1 FROM historia_precios h
                                WHERE h.id_producto_tienda = b.id_producto_tienda
                                  AND h.fecha_precio       = b.fecha_precio
                            )
                        """)
                        inserted_hp = cur.rowcount
                    raw_conn_hp.commit()
                finally:
                    raw_conn_hp.close()
                print(f"[{db_name}] Historial de precios insertado correctamente "
                      f"({inserted_hp} nuevos, {len(prices_batch) - inserted_hp} ya existían).")
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

    fatal_errors = []
    for target in targets:
        db_name = target['name']
        try:
            engine = sa.create_engine(target['url'])
            insert_data_bulk(engine, df, db_name=db_name)
        except Exception as e:
            print(f"[ERROR FATAL] {db_name}: {e}")
            fatal_errors.append(db_name)

    # Mover CSVs procesados a raw_data/used/ para mantener raw_data limpio.
    # Producción es el destino de referencia: si solo falla la BD local (p.ej.
    # contenedor apagado) los CSV igual se archivan. Si no se movieran, el
    # próximo run los volvería a leer y reinsertaría precios de runs anteriores.
    target_names = [t['name'] for t in targets]
    if "Production" in target_names:
        archive_ok = "Production" not in fatal_errors
    else:
        archive_ok = bool(targets) and not fatal_errors

    if archive_ok and fatal_errors:
        print(f"\n[raw_data] Aviso: {', '.join(fatal_errors)} falló, pero Producción OK — "
              f"se archivan los CSV igual (esa BD quedará desfasada).")

    if archive_ok:
        project_root = os.path.abspath(os.path.join(current_dir, ".."))
        raw_data_dir = os.path.join(project_root, "raw_data")
        used_dir     = os.path.join(raw_data_dir, "used")
        os.makedirs(used_dir, exist_ok=True)

        moved = 0
        for fname in os.listdir(raw_data_dir):
            if not fname.endswith(".csv"):
                continue
            src = os.path.join(raw_data_dir, fname)
            dst = os.path.join(used_dir, fname)
            if os.path.exists(dst):
                # Si ya existe (mismo nombre), agregar sufijo para no perderlo
                base, ext = os.path.splitext(fname)
                dst = os.path.join(used_dir, f"{base}_dup{ext}")
            shutil.move(src, dst)
            moved += 1

        if moved:
            print(f"\n[raw_data] {moved} CSV(s) movidos a raw_data/used/")
    else:
        print(f"\n[raw_data] CSVs NO movidos — hubo errores en: {', '.join(fatal_errors)}")


if __name__ == "__main__":
    main()
