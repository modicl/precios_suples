"""
clean_db_brands.py — Unifica marcas duplicadas/variantes en la BD.

Por cada grupo de marcas similares (según keywords_marcas.json), reasigna todos
los productos que usen la marca "sucia" hacia la marca canónica, luego elimina
la entrada duplicada.

Casos especiales manejados:
  - Si el canónico no existe en la BD, se RENOMBRA la entrada sucia en lugar de
    reasignar + eliminar.
  - Marcas que son packs de varias marcas ("Applied Nutrition / Ostrovit") se
    asignan a una marca especial "PACK" que se crea si no existe.

Ejecutar desde la raíz del proyecto:
    python tools/clean_db_brands.py [--dry-run] [--only-local] [--only-prod]
"""

import sys
import os
import argparse
import json
import re
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, ".."))
_JSON_PATH = os.path.join(_PROJECT_ROOT, "scrapers_v2", "diccionarios", "keywords_marcas.json")

# Marcas que representan packs de múltiples fabricantes → se asignan a PACK
PACK_BRANDS = {
    "Applied Nutrition / Ostrovit",
}

# Nombre canónico especial para packs
PACK_CANONICAL = "PACK"


def _norm(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def build_kw_index(json_path: str) -> list[tuple[str, str]]:
    """
    Lee keywords_marcas.json y construye índice plano:
        [(keyword_normalizada, canonical_name), ...]
    ordenado de mayor a menor longitud de keyword.
    """
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    index: list[tuple[str, str]] = []
    for canonical, meta in data.items():
        for kw in meta.get("keywords", []):
            index.append((_norm(kw), canonical))
    index.sort(key=lambda x: len(x[0]), reverse=True)
    return index


def find_canonical(raw_brand: str, kw_index: list[tuple[str, str]]) -> str | None:
    """
    Devuelve el nombre canónico si raw_brand es una variante,
    o None si ya es canónico / no hay coincidencia.
    """
    norm = _norm(raw_brand)
    for kw, canonical in kw_index:
        pattern = r"(?<![a-z0-9])" + re.escape(kw) + r"(?![a-z0-9])"
        if re.search(pattern, norm):
            if raw_brand.strip() != canonical:
                return canonical
            return None  # ya es canónico
    return None


def delete_duplicate_products(conn, from_brand_id: int, to_brand_id: int) -> int:
    """
    For each product in from_brand that is a duplicate of a product in to_brand
    (same nombre_producto + id_subcategoria), migrate its producto_tienda rows
    to the canonical product, then delete the duplicate.
    Returns count of deleted duplicate products.
    """
    duplicates = conn.execute(sa.text("""
        SELECT p_dup.id_producto AS dup_id, p_can.id_producto AS can_id
        FROM productos p_dup
        JOIN productos p_can
          ON p_can.nombre_producto = p_dup.nombre_producto
         AND p_can.id_subcategoria  = p_dup.id_subcategoria
         AND p_can.id_marca         = :tid
        WHERE p_dup.id_marca = :fid
    """), {"fid": from_brand_id, "tid": to_brand_id}).fetchall()

    for dup_id, can_id in duplicates:
        # Move producto_tienda rows, skipping any that would duplicate on (id_producto, id_tienda)
        conn.execute(sa.text("""
            UPDATE producto_tienda
            SET id_producto = :can_id
            WHERE id_producto = :dup_id
              AND id_tienda NOT IN (
                  SELECT id_tienda FROM producto_tienda WHERE id_producto = :can_id
              )
        """), {"can_id": can_id, "dup_id": dup_id})
        # Delete leftover producto_tienda rows (exact dupes that couldn't be moved)
        conn.execute(sa.text(
            "DELETE FROM producto_tienda WHERE id_producto = :dup_id"
        ), {"dup_id": dup_id})
        # Now safe to delete the duplicate product
        conn.execute(sa.text(
            "DELETE FROM productos WHERE id_producto = :dup_id"
        ), {"dup_id": dup_id})

    return len(duplicates)


def ensure_brand(conn, nombre: str) -> int:
    """Devuelve id_marca de 'nombre', creándola si no existe."""
    row = conn.execute(
        sa.text("SELECT id_marca FROM marcas WHERE nombre_marca = :n"),
        {"n": nombre}
    ).fetchone()
    if row:
        return row[0]
    result = conn.execute(
        sa.text("INSERT INTO marcas (nombre_marca) VALUES (:n) RETURNING id_marca"),
        {"n": nombre}
    )
    new_id = result.fetchone()[0]
    print(f"  [+] Marca creada: '{nombre}' (id={new_id})")
    return new_id


def run_cleanup(engine, kw_index: list[tuple[str, str]], dry_run: bool, db_name: str):
    print(f"\n{'='*60}")
    print(f"  Base de datos: {db_name}  {'[DRY RUN]' if dry_run else '[REAL]'}")
    print(f"{'='*60}")

    with engine.connect() as conn:
        rows = conn.execute(sa.text(
            "SELECT id_marca, nombre_marca FROM marcas ORDER BY nombre_marca"
        )).fetchall()
        brands_in_db: list[tuple[int, str]] = [(r[0], r[1]) for r in rows]
        print(f"\nMarcas en BD: {len(brands_in_db)}")

        # mapa normalizado → (id, nombre_real)
        brand_lookup: dict[str, tuple[int, str]] = {
            _norm(b[1]): b for b in brands_in_db
        }

        # ------------------------------------------------------------------
        # Clasificar cada marca en una de tres acciones:
        #   PACK    → reasignar a marca especial PACK
        #   MERGE   → canónico existe en BD: reasignar productos + eliminar sucia
        #   RENAME  → canónico NO existe en BD: renombrar la fila directamente
        # ------------------------------------------------------------------
        actions: list[dict] = []

        for brand_id, brand_name in brands_in_db:
            # Caso especial: packs de múltiples marcas
            if brand_name in PACK_BRANDS:
                actions.append({
                    "type": "PACK",
                    "from_id": brand_id,
                    "from_name": brand_name,
                    "to_name": PACK_CANONICAL,
                })
                continue

            canonical = find_canonical(brand_name, kw_index)
            if canonical is None:
                continue

            canonical_entry = brand_lookup.get(_norm(canonical))

            if canonical_entry and canonical_entry[0] != brand_id:
                # Canónico existe en BD y es distinto → MERGE
                actions.append({
                    "type": "MERGE",
                    "from_id": brand_id,
                    "from_name": brand_name,
                    "to_id": canonical_entry[0],
                    "to_name": canonical_entry[1],
                })
            elif not canonical_entry:
                # Canónico no existe → RENAME
                actions.append({
                    "type": "RENAME",
                    "from_id": brand_id,
                    "from_name": brand_name,
                    "to_name": canonical,
                })

        if not actions:
            print("\n  No se encontraron marcas para unificar. La BD está limpia.")
            return

        # Agrupar por tipo para mostrar ordenado
        packs   = [a for a in actions if a["type"] == "PACK"]
        merges  = [a for a in actions if a["type"] == "MERGE"]
        renames = [a for a in actions if a["type"] == "RENAME"]

        if renames:
            print(f"\n  RENOMBRAR ({len(renames)}) — canónico no existe, se renombra la fila:")
            for a in renames:
                print(f"    [{a['from_id']}] '{a['from_name']}' → '{a['to_name']}'")

        if merges:
            print(f"\n  UNIFICAR ({len(merges)}) — canónico existe, se migran productos y elimina la variante:")
            for a in merges:
                print(f"    [{a['from_id']}] '{a['from_name']}' → [{a['to_id']}] '{a['to_name']}'")

        if packs:
            print(f"\n  PACKS ({len(packs)}) — producto de múltiples marcas, se asigna a '{PACK_CANONICAL}':")
            for a in packs:
                print(f"    [{a['from_id']}] '{a['from_name']}' → '{PACK_CANONICAL}'")

        if dry_run:
            print(f"\n  [DRY RUN] No se realizaron cambios.")
            return

        print()
        resp = input("  ¿Ejecutar los cambios? [s/N]: ").strip().lower()
        if resp not in ("s", "si", "sí", "yes", "y"):
            print("  Cancelado.")
            return

        try:
            # 1. RENAME: cambiar nombre_marca directamente
            for a in renames:
                conn.execute(sa.text(
                    "UPDATE marcas SET nombre_marca = :new WHERE id_marca = :fid"
                ), {"new": a["to_name"], "fid": a["from_id"]})
                print(f"  RENAME  [{a['from_id']}] '{a['from_name']}' → '{a['to_name']}'")

            # 2. MERGE: reasignar productos y eliminar la variante
            for a in merges:
                count = conn.execute(sa.text(
                    "SELECT COUNT(*) FROM productos WHERE id_marca = :fid"
                ), {"fid": a["from_id"]}).scalar()
                deleted = delete_duplicate_products(conn, a["from_id"], a["to_id"])
                if deleted:
                    print(f"  DEDUP   [{a['from_id']}] '{a['from_name']}': {deleted} duplicado(s) resuelto(s)")
                conn.execute(sa.text(
                    "UPDATE productos SET id_marca = :tid WHERE id_marca = :fid"
                ), {"tid": a["to_id"], "fid": a["from_id"]})
                conn.execute(sa.text(
                    "DELETE FROM marcas WHERE id_marca = :fid"
                ), {"fid": a["from_id"]})
                print(f"  MERGE   [{a['from_id']}] '{a['from_name']}' → [{a['to_id']}] '{a['to_name']}' | {count - deleted} productos migrados")

            # 3. PACK: asegurar que exista la marca PACK y reasignar
            for a in packs:
                pack_id = ensure_brand(conn, PACK_CANONICAL)
                count = conn.execute(sa.text(
                    "SELECT COUNT(*) FROM productos WHERE id_marca = :fid"
                ), {"fid": a["from_id"]}).scalar()
                deleted = delete_duplicate_products(conn, a["from_id"], pack_id)
                if deleted:
                    print(f"  DEDUP   [{a['from_id']}] '{a['from_name']}': {deleted} duplicado(s) resuelto(s)")
                conn.execute(sa.text(
                    "UPDATE productos SET id_marca = :tid WHERE id_marca = :fid"
                ), {"tid": pack_id, "fid": a["from_id"]})
                conn.execute(sa.text(
                    "DELETE FROM marcas WHERE id_marca = :fid"
                ), {"fid": a["from_id"]})
                print(f"  PACK    [{a['from_id']}] '{a['from_name']}' → [{pack_id}] '{PACK_CANONICAL}' | {count - deleted} productos migrados")

            conn.commit()
        except Exception:
            conn.rollback()
            raise

        print("\n  Limpieza completada.")


def main():
    parser = argparse.ArgumentParser(description="Unifica marcas duplicadas en la BD")
    parser.add_argument("--dry-run", action="store_true", help="Solo muestra cambios sin ejecutar")
    parser.add_argument("--only-local", action="store_true", help="Solo BD local")
    parser.add_argument("--only-prod", action="store_true", help="Solo BD producción")
    args = parser.parse_args()

    if not os.path.exists(_JSON_PATH):
        print(f"ERROR: No se encontró {_JSON_PATH}")
        sys.exit(1)

    kw_index = build_kw_index(_JSON_PATH)
    print(f"Índice cargado: {len(kw_index)} keywords")

    sys.path.insert(0, _PROJECT_ROOT)
    from tools.db_multiconnect import get_targets
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
            run_cleanup(engine, kw_index, dry_run=args.dry_run, db_name=target["name"])
        except Exception as e:
            print(f"\n[ERROR en {target['name']}]: {e}")
        finally:
            engine.dispose()


if __name__ == "__main__":
    main()
