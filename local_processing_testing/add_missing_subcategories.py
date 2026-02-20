"""
Adds 3 missing subcategories to the DB (both Local and Production).

Mismatches resolved:
  5 - Bebidas Nutricionales / "Geles Energéticos"   (not in DB)
  6 - Perdida de Grasa      / "L-Carnitina"          (not in DB)
  7 - Pre Entrenos          / "Óxido Nítrico"         (not in DB)

Run once; uses INSERT ... ON CONFLICT DO NOTHING so it is idempotent.
"""

import os
import sys
import sqlalchemy as sa

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

# (category_name, subcategory_name) pairs to add
NEW_SUBCATEGORIES = [
    ("Bebidas Nutricionales", "Geles Energéticos"),
    ("Perdida de Grasa",      "L-Carnitina"),
    ("Pre Entrenos",          "Óxido Nítrico"),
]


def add_subcategories(target):
    name = target["name"]
    engine = sa.create_engine(target["url"])

    try:
        with engine.begin() as conn:
            # Build category name → id map
            rows = conn.execute(
                sa.text("SELECT id_categoria, nombre_categoria FROM categorias")
            ).fetchall()
            cat_map = {row.nombre_categoria: row.id_categoria for row in rows}

            inserted = 0
            skipped = 0

            for cat_name, subcat_name in NEW_SUBCATEGORIES:
                cat_id = cat_map.get(cat_name)
                if cat_id is None:
                    print(f"  [{name}] WARN: category '{cat_name}' not found — skipping '{subcat_name}'")
                    skipped += 1
                    continue

                result = conn.execute(
                    sa.text("""
                        INSERT INTO subcategorias (nombre_subcategoria, id_categoria)
                        VALUES (:subcat, :cat_id)
                        ON CONFLICT (nombre_subcategoria, id_categoria) DO NOTHING
                    """),
                    {"subcat": subcat_name, "cat_id": cat_id},
                )
                if result.rowcount == 1:
                    print(f"  [{name}] INSERTED: '{subcat_name}' under '{cat_name}' (id_categoria={cat_id})")
                    inserted += 1
                else:
                    print(f"  [{name}] ALREADY EXISTS: '{subcat_name}' under '{cat_name}' — no change")
                    skipped += 1

            print(f"  [{name}] Done — inserted: {inserted}, skipped/existing: {skipped}")

    except Exception as e:
        print(f"  [{name}] ERROR: {e}")


def main():
    print("--- Agregando subcategorías faltantes ---")
    targets = get_targets()
    if not targets:
        print("Error: no DB targets found. Check .env")
        sys.exit(1)

    for target in targets:
        print(f"\nTarget: {target['name']}")
        add_subcategories(target)


if __name__ == "__main__":
    main()
