"""
step4_deduplication.py — Deduplica productos en la BD.

Lógica de dedup:
  - Un "grupo de duplicados" = mismas (lower(nombre_producto), id_marca, id_subcategoria).
    Mismo nombre+marca pero distinta subcategoría = productos distintos (no se fusionan).
  - El "maestro" se elige por puntaje:
      +10  si la categoría del producto está en IMPORTANT_CATEGORIES (live desde DB)
      -100 si la categoría contiene "oferta" (p.ej. "Ofertas" siempre pierde)
      +20  si el nombre NO es todo minúsculas (preferencia por Title Case)
    En empate de puntaje: menor id_producto gana (el más antiguo).
  - Los duplicados no-maestros tienen sus:
      • historia_precios migrada al link del maestro (o eliminada si hay conflicto de tienda)
      • producto_tienda moved/deleted en consecuencia
      • click_analytics migradas al maestro
      • producto eliminado

Flags:
  --dry-run   Muestra cuántos grupos/productos se verían afectados sin tocar la BD.
"""

import os
import sys
import argparse
import sqlalchemy as sa
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

LOW_PRIORITY_KEYWORDS = [
    "oferta", "pack", "promo", "bundle", "especial",
    "outlet", "liquidacion", "cyber", "black",
]


def fetch_important_categories(conn):
    """Obtiene el conjunto de nombres de categoría desde la BD en tiempo real."""
    rows = conn.execute(
        sa.text("SELECT nombre_categoria FROM categorias")
    ).fetchall()
    return {row.nombre_categoria for row in rows}


def get_score(cat_name, prod_name, important_categories):
    score = 0
    cat_lower = (cat_name or "").lower()

    if cat_name in important_categories:
        score += 10

    for keyword in LOW_PRIORITY_KEYWORDS:
        if keyword in cat_lower:
            score -= 100
            break

    # Preferir Title Case / nombres no completamente en minúsculas
    if prod_name and not prod_name.islower():
        score += 20

    return score


def fetch_categories_bulk(conn, product_ids):
    if not product_ids:
        return {}
    chunk_size = 2000
    product_categories = {}
    pids_list = list(product_ids)

    for i in range(0, len(pids_list), chunk_size):
        chunk = tuple(pids_list[i:i + chunk_size])
        rows = conn.execute(
            sa.text("""
                SELECT p.id_producto, c.nombre_categoria
                FROM productos p
                LEFT JOIN subcategorias sc ON p.id_subcategoria = sc.id_subcategoria
                LEFT JOIN categorias c ON sc.id_categoria = c.id_categoria
                WHERE p.id_producto IN :pids
            """),
            {"pids": chunk},
        ).fetchall()
        for row in rows:
            product_categories[row.id_producto] = row.nombre_categoria or ""

    return product_categories


def fetch_links_bulk(conn, product_ids):
    if not product_ids:
        return {}
    chunk_size = 2000
    product_links = defaultdict(list)
    pids_list = list(product_ids)

    for i in range(0, len(pids_list), chunk_size):
        chunk = tuple(pids_list[i:i + chunk_size])
        rows = conn.execute(
            sa.text("""
                SELECT id_producto, id_producto_tienda, id_tienda
                FROM producto_tienda
                WHERE id_producto IN :pids
            """),
            {"pids": chunk},
        ).fetchall()
        for row in rows:
            product_links[row.id_producto].append({
                "id_producto_tienda": row.id_producto_tienda,
                "id_tienda": row.id_tienda,
            })

    return product_links


def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t["name"] == "Local"), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local' en db_multiconnect.")
        sys.exit(1)
    return sa.create_engine(local_target["url"])


def fix_duplicates(engine, dry_run=False):
    mode_label = "[DRY-RUN] " if dry_run else ""
    print(f"\n--- {mode_label}Deduplicando Productos (insensible a mayúsculas, por subcategoría) ---")

    with engine.begin() as conn:
        # Live categories for scoring
        important_categories = fetch_important_categories(conn)
        print(f"  Categorías cargadas desde BD: {len(important_categories)}")

        # 1. Buscar grupos duplicados
        #    Clave: (lower(nombre_producto), id_marca, id_subcategoria)
        #    Mismo nombre+marca en distinta subcategoría = productos distintos.
        print("  Buscando grupos de duplicados...")
        grupos = conn.execute(
            sa.text("""
                SELECT
                    lower(nombre_producto)  AS clean_name,
                    id_marca,
                    id_subcategoria,
                    array_agg(id_producto)     AS ids,
                    array_agg(nombre_producto) AS names
                FROM productos
                GROUP BY lower(nombre_producto), id_marca, id_subcategoria
                HAVING count(*) > 1
            """)
        ).fetchall()

        if not grupos:
            print("  No se encontraron duplicados.")
            return

        print(f"  Encontrados {len(grupos)} grupos de duplicados.")

        # Collect all IDs
        all_product_ids = {pid for row in grupos for pid in row.ids}

        # 2. Bulk fetch ancillary data
        categories_map = fetch_categories_bulk(conn, all_product_ids)
        links_map = fetch_links_bulk(conn, all_product_ids)

        if dry_run:
            total_dupes = sum(len(row.ids) - 1 for row in grupos)
            print(f"  [DRY-RUN] Productos que se eliminarían: {total_dupes}")
            print("  [DRY-RUN] Sin cambios en BD.")
            return

        # 3. Drop unique constraint on historia_precios if present
        try:
            conn.execute(
                sa.text("ALTER TABLE historia_precios DROP CONSTRAINT IF EXISTS uq_precio_fecha")
            )
        except Exception:
            pass

        # 4. Preparar operaciones
        ops_update_history   = []
        ops_delete_links_ids = []
        ops_move_links       = []
        ops_delete_product_ids = []
        ops_move_analytics   = []

        processed_count = 0

        for row in grupos:
            id_to_name = dict(zip(row.ids, row.names))

            candidates = [
                {
                    "id": pid,
                    "score": get_score(
                        categories_map.get(pid, ""),
                        id_to_name.get(pid, ""),
                        important_categories,
                    ),
                }
                for pid in row.ids
            ]
            # Mayor puntaje primero; en empate, id_producto menor (más antiguo) gana
            candidates.sort(key=lambda x: (-x["score"], x["id"]))

            master_id = candidates[0]["id"]
            dupes = candidates[1:]

            # Master links: {id_tienda: id_producto_tienda}
            master_links = {
                lnk["id_tienda"]: lnk["id_producto_tienda"]
                for lnk in links_map.get(master_id, [])
            }

            for d in dupes:
                dupe_id = d["id"]
                ops_delete_product_ids.append(dupe_id)
                ops_move_analytics.append({"new": master_id, "old": dupe_id})

                for lnk in links_map.get(dupe_id, []):
                    link_id_dupe = lnk["id_producto_tienda"]
                    tienda_id    = lnk["id_tienda"]

                    if tienda_id in master_links:
                        # Conflicto: el maestro ya tiene este tienda
                        # → migrar historia al link del maestro, eliminar link del dupe
                        link_id_master = master_links[tienda_id]
                        ops_update_history.append({"new_id": link_id_master, "old_id": link_id_dupe})
                        ops_delete_links_ids.append(link_id_dupe)
                    else:
                        # Sin conflicto: mover el link al maestro
                        ops_move_links.append({"master_id": master_id, "link_id": link_id_dupe})
                        master_links[tienda_id] = link_id_dupe

            processed_count += 1

        # 5. Ejecutar cambios
        print("  Ejecutando cambios en BD...")

        if ops_update_history:
            print(f"  Migrando historiales ({len(ops_update_history)} ops)...")
            conn.execute(
                sa.text("UPDATE historia_precios SET id_producto_tienda = :new_id WHERE id_producto_tienda = :old_id"),
                ops_update_history,
            )

        if ops_delete_links_ids:
            print(f"  Eliminando links redundantes ({len(ops_delete_links_ids)} ops)...")
            chunk_size = 5000
            for i in range(0, len(ops_delete_links_ids), chunk_size):
                chunk = tuple(ops_delete_links_ids[i:i + chunk_size])
                conn.execute(
                    sa.text("DELETE FROM producto_tienda WHERE id_producto_tienda IN :ids"),
                    {"ids": chunk},
                )

        if ops_move_links:
            print(f"  Moviendo links al maestro ({len(ops_move_links)} ops)...")
            conn.execute(
                sa.text("UPDATE producto_tienda SET id_producto = :master_id WHERE id_producto_tienda = :link_id"),
                ops_move_links,
            )

        if ops_move_analytics:
            print(f"  Migrando analíticas al maestro ({len(ops_move_analytics)} ops)...")
            conn.execute(
                sa.text("UPDATE click_analytics SET id_producto = :new WHERE id_producto = :old"),
                ops_move_analytics,
            )

        if ops_delete_product_ids:
            print(f"  Eliminando productos duplicados ({len(ops_delete_product_ids)} ops)...")
            chunk_size = 5000
            for i in range(0, len(ops_delete_product_ids), chunk_size):
                chunk = tuple(ops_delete_product_ids[i:i + chunk_size])
                conn.execute(
                    sa.text("DELETE FROM productos WHERE id_producto IN :ids"),
                    {"ids": chunk},
                )

        print(
            f"  Deduplicación completada. "
            f"Grupos: {processed_count}. "
            f"Productos eliminados: {len(ops_delete_product_ids)}."
        )


def main():
    parser = argparse.ArgumentParser(description="Paso 4: Deduplicación de productos")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra el impacto sin modificar la BD",
    )
    args = parser.parse_args()

    print("--- PASO 4: Deduplicación de Productos ---")
    engine = get_local_engine()
    fix_duplicates(engine, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
