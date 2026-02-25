"""
migrate_micronizada.py — Migra productos de "Micronizada" → "Creatina Monohidrato".

Lógica:
  1. Busca la subcategoría "Micronizada" (bajo categoría "Creatinas") y
     "Creatina Monohidrato".
  2. Para cada producto en "Micronizada":
     - Si NO existe otro producto con el mismo (nombre_producto, id_marca)
       en "Creatina Monohidrato": simplemente actualiza id_subcategoria.
     - Si YA existe (conflicto): el de "Creatina Monohidrato" es el maestro.
       Migra historia_precios y producto_tienda, luego elimina el duplicado.
  3. Elimina la subcategoría "Micronizada" de la BD.

Flags:
  --dry-run       Muestra el plan sin tocar la BD.
  --only-local    Solo BD local.
  --only-prod     Solo BD de producción.

Ejecutar desde la raíz del proyecto:
    python tools/migrate_micronizada.py [--dry-run] [--only-local] [--only-prod]
"""

import sys
import os
import argparse
import sqlalchemy as sa

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
sys.path.insert(0, _PROJECT_ROOT)

from tools.db_multiconnect import get_targets

SUBCAT_MICRONIZADA    = "Micronizada"
SUBCAT_MONOHIDRATO    = "Creatina Monohidrato"
CAT_CREATINAS         = "Creatinas"


def _get_subcat_id(conn, nombre_subcategoria: str, nombre_categoria: str) -> int | None:
    row = conn.execute(sa.text("""
        SELECT sc.id_subcategoria
        FROM subcategorias sc
        JOIN categorias c ON c.id_categoria = sc.id_categoria
        WHERE sc.nombre_subcategoria = :sc
          AND c.nombre_categoria     = :cat
    """), {"sc": nombre_subcategoria, "cat": nombre_categoria}).fetchone()
    return row[0] if row else None


def run_migration(engine, dry_run: bool, db_name: str):
    tag = "[DRY-RUN] " if dry_run else ""
    print(f"\n{'='*60}")
    print(f"  BD: {db_name}  {tag}")
    print(f"{'='*60}")

    with engine.begin() as conn:
        mic_id = _get_subcat_id(conn, SUBCAT_MICRONIZADA, CAT_CREATINAS)
        cm_id  = _get_subcat_id(conn, SUBCAT_MONOHIDRATO, CAT_CREATINAS)

        if mic_id is None:
            print(f"  Subcategoría '{SUBCAT_MICRONIZADA}' no encontrada. Nada que hacer.")
            return
        if cm_id is None:
            print(f"  ERROR: Subcategoría '{SUBCAT_MONOHIDRATO}' no encontrada.")
            return

        print(f"  '{SUBCAT_MICRONIZADA}' id={mic_id}  →  '{SUBCAT_MONOHIDRATO}' id={cm_id}")

        # ── Productos en Micronizada ─────────────────────────────────────────
        mic_products = conn.execute(sa.text("""
            SELECT id_producto, nombre_producto, id_marca
            FROM productos
            WHERE id_subcategoria = :mic_id
        """), {"mic_id": mic_id}).fetchall()

        if not mic_products:
            print("  No hay productos en 'Micronizada'.")
            # Still delete the subcategory if it exists
            if not dry_run:
                conn.execute(sa.text(
                    "DELETE FROM subcategorias WHERE id_subcategoria = :mid"
                ), {"mid": mic_id})
                print(f"  Subcategoría '{SUBCAT_MICRONIZADA}' eliminada.")
            return

        print(f"  Productos en '{SUBCAT_MICRONIZADA}': {len(mic_products)}")

        # ── Detectar conflictos ──────────────────────────────────────────────
        # Conflicto = mismo (nombre_producto, id_marca) ya existe en Creatina Monohidrato
        conflicts   = []  # [(mic_producto_id, cm_producto_id)]
        simple_move = []  # [mic_producto_id]

        for row in mic_products:
            mic_pid      = row.id_producto
            nombre       = row.nombre_producto
            id_marca     = row.id_marca

            existing = conn.execute(sa.text("""
                SELECT id_producto FROM productos
                WHERE nombre_producto = :n
                  AND (id_marca = :m OR (id_marca IS NULL AND :m IS NULL))
                  AND id_subcategoria = :cm_id
            """), {"n": nombre, "m": id_marca, "cm_id": cm_id}).fetchone()

            if existing:
                conflicts.append((mic_pid, existing[0]))
            else:
                simple_move.append(mic_pid)

        print(f"  Movimientos simples (no conflicto): {len(simple_move)}")
        print(f"  Conflictos (duplicado ya existe en Monohidrato): {len(conflicts)}")

        if conflicts:
            print("  Conflictos a resolver (mic_id → cm_id):")
            for mic_pid, cm_pid in conflicts:
                print(f"    producto {mic_pid} → maestro {cm_pid}")

        if dry_run:
            print(f"\n  [DRY-RUN] Sin cambios en BD.")
            return

        # ── 1. Movimientos simples: solo cambiar id_subcategoria ─────────────
        if simple_move:
            # Chunk para evitar parámetros excesivos
            chunk_size = 1000
            for i in range(0, len(simple_move), chunk_size):
                chunk = tuple(simple_move[i:i + chunk_size])
                conn.execute(sa.text("""
                    UPDATE productos
                    SET id_subcategoria = :cm_id
                    WHERE id_producto IN :ids
                """), {"cm_id": cm_id, "ids": chunk})
            print(f"  Movidos {len(simple_move)} productos a '{SUBCAT_MONOHIDRATO}'.")

        # ── 2. Conflictos: fusionar en el maestro (Creatina Monohidrato) ─────
        for mic_pid, cm_pid in conflicts:
            # Links del maestro: {id_tienda: id_producto_tienda}
            master_links_rows = conn.execute(sa.text("""
                SELECT id_producto_tienda, id_tienda
                FROM producto_tienda
                WHERE id_producto = :pid
            """), {"pid": cm_pid}).fetchall()
            master_links = {r.id_tienda: r.id_producto_tienda for r in master_links_rows}

            # Links del duplicado
            dupe_links_rows = conn.execute(sa.text("""
                SELECT id_producto_tienda, id_tienda
                FROM producto_tienda
                WHERE id_producto = :pid
            """), {"pid": mic_pid}).fetchall()

            for lnk in dupe_links_rows:
                dupe_link_id  = lnk.id_producto_tienda
                tienda_id     = lnk.id_tienda

                if tienda_id in master_links:
                    # Conflicto de tienda: migrar historia al link del maestro y eliminar el del dupe
                    master_link_id = master_links[tienda_id]
                    conn.execute(sa.text("""
                        UPDATE historia_precios
                        SET id_producto_tienda = :mid
                        WHERE id_producto_tienda = :did
                    """), {"mid": master_link_id, "did": dupe_link_id})
                    conn.execute(sa.text(
                        "DELETE FROM producto_tienda WHERE id_producto_tienda = :did"
                    ), {"did": dupe_link_id})
                else:
                    # Sin conflicto: mover el link al maestro
                    conn.execute(sa.text("""
                        UPDATE producto_tienda
                        SET id_producto = :cm_pid
                        WHERE id_producto_tienda = :lid
                    """), {"cm_pid": cm_pid, "lid": dupe_link_id})
                    master_links[tienda_id] = dupe_link_id

            # Migrar click_analytics
            conn.execute(sa.text("""
                UPDATE click_analytics
                SET id_producto = :cm_pid
                WHERE id_producto = :mic_pid
            """), {"cm_pid": cm_pid, "mic_pid": mic_pid})

            # Eliminar el producto duplicado
            conn.execute(sa.text(
                "DELETE FROM productos WHERE id_producto = :pid"
            ), {"pid": mic_pid})

            print(f"  Fusionado: producto {mic_pid} → maestro {cm_pid}")

        # ── 3. Eliminar subcategoría Micronizada ─────────────────────────────
        conn.execute(sa.text(
            "DELETE FROM subcategorias WHERE id_subcategoria = :mid"
        ), {"mid": mic_id})
        print(f"  Subcategoría '{SUBCAT_MICRONIZADA}' (id={mic_id}) eliminada.")

        total_removed = len(conflicts)
        total_moved   = len(simple_move)
        print(f"\n  Listo. {total_moved} productos migrados, {total_removed} duplicados resueltos.")


def main():
    parser = argparse.ArgumentParser(
        description="Migra subcategoría 'Micronizada' → 'Creatina Monohidrato'"
    )
    parser.add_argument("--dry-run",    action="store_true", help="Solo muestra el plan sin modificar la BD")
    parser.add_argument("--only-local", action="store_true", help="Solo BD local")
    parser.add_argument("--only-prod",  action="store_true", help="Solo BD de producción")
    args = parser.parse_args()

    targets = get_targets()
    if not targets:
        print("ERROR: No se encontraron configuraciones de BD en .env")
        sys.exit(1)

    for target in targets:
        if args.only_local and target["name"] != "Local":
            continue
        if args.only_prod and target["name"] != "Production":
            continue

        engine = sa.create_engine(target["url"])
        try:
            run_migration(engine, dry_run=args.dry_run, db_name=target["name"])
        except Exception as e:
            print(f"\n[ERROR en {target['name']}]: {e}")
            raise
        finally:
            engine.dispose()


if __name__ == "__main__":
    main()
