"""
review_clusters.py
------------------
Muestra los productos que fueron clusterizados exitosamente en la BD,
es decir, productos detectados en más de una tienda.

Uso:
    python local_processing_testing/debug_tools/review_clusters.py
    python local_processing_testing/debug_tools/review_clusters.py --min-tiendas 3
    python local_processing_testing/debug_tools/review_clusters.py --categoria Proteinas
    python local_processing_testing/debug_tools/review_clusters.py --buscar "iso 100"
    python local_processing_testing/debug_tools/review_clusters.py --todos          # incluye productos de 1 sola tienda
    python local_processing_testing/debug_tools/review_clusters.py --exportar       # guarda CSV en debug_tools/
"""

import argparse
import os
import sys
import psycopg2

# ---------------------------------------------------------------------------
# Setup path
# ---------------------------------------------------------------------------
script_dir   = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))
sys.path.insert(0, project_root)

from tools.db_multiconnect import get_targets


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
DIM    = "\033[2m"

def color(text, *codes):
    return "".join(codes) + str(text) + RESET


def get_connection():
    targets = get_targets()
    local = next((t for t in targets if t["name"] == "Local"), None)
    if not local:
        print("ERROR: No se encontró configuración de BD local en .env")
        sys.exit(1)
    return psycopg2.connect(local["url"])


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------
QUERY = """
SELECT
    p.id_producto,
    p.nombre_producto,
    m.nombre_marca,
    c.nombre_categoria,
    s.nombre_subcategoria,
    COUNT(DISTINCT pt.id_tienda)                        AS n_tiendas,
    MIN(hp.precio)                                      AS precio_min,
    MAX(hp.precio)                                      AS precio_max,
    ROUND(
        (MAX(hp.precio) - MIN(hp.precio))::numeric
        / NULLIF(MIN(hp.precio), 0) * 100, 1
    )                                                   AS spread_pct,
    STRING_AGG(DISTINCT t.nombre_tienda, ', '
               ORDER BY t.nombre_tienda)               AS tiendas
FROM productos p
JOIN marcas       m  ON p.id_marca        = m.id_marca
JOIN subcategorias s ON p.id_subcategoria = s.id_subcategoria
JOIN categorias    c ON s.id_categoria    = c.id_categoria
JOIN producto_tienda pt ON p.id_producto  = pt.id_producto
JOIN tiendas       t  ON pt.id_tienda     = t.id_tienda
JOIN historia_precios hp ON pt.id_producto_tienda = hp.id_producto_tienda
{where}
GROUP BY p.id_producto, p.nombre_producto, m.nombre_marca,
         c.nombre_categoria, s.nombre_subcategoria
{having}
ORDER BY n_tiendas DESC, c.nombre_categoria, p.nombre_producto
"""


def fetch_clusters(cur, min_tiendas: int, categoria: str | None, buscar: str | None):
    conditions = []
    params     = []

    if categoria:
        conditions.append("c.nombre_categoria ILIKE %s")
        params.append(f"%{categoria}%")

    if buscar:
        conditions.append("p.nombre_producto ILIKE %s")
        params.append(f"%{buscar}%")

    where  = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    having = f"HAVING COUNT(DISTINCT pt.id_tienda) >= {min_tiendas}"

    sql = QUERY.format(where=where, having=having)
    cur.execute(sql, params)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------
COL_WIDTHS = {
    "id":       6,
    "nombre":   45,
    "marca":    14,
    "cat":      14,
    "subcat":   20,
    "tiendas":  3,
    "min":      10,
    "max":      10,
    "spread":   7,
}

def fmt_price(v):
    return f"${v:,.0f}".replace(",", ".")

def fmt_spread(v):
    if v is None:
        return "  -   "
    return f"{v:>5.1f}%"

def tier_color(n):
    if n >= 5: return GREEN
    if n >= 3: return CYAN
    if n >= 2: return YELLOW
    return DIM

def print_header():
    h = (
        f"{'ID':>{COL_WIDTHS['id']}}  "
        f"{'Producto':<{COL_WIDTHS['nombre']}}  "
        f"{'Marca':<{COL_WIDTHS['marca']}}  "
        f"{'Categoría':<{COL_WIDTHS['cat']}}  "
        f"{'Subcategoría':<{COL_WIDTHS['subcat']}}  "
        f"{'#T':>{COL_WIDTHS['tiendas']}}  "
        f"{'Precio Min':>{COL_WIDTHS['min']}}  "
        f"{'Precio Max':>{COL_WIDTHS['max']}}  "
        f"{'Spread':>{COL_WIDTHS['spread']}}"
    )
    sep = "-" * len(h)
    print(color(sep, BOLD))
    print(color(h, BOLD))
    print(color(sep, BOLD))

def print_row(row, show_stores: bool = False):
    pid, nombre, marca, cat, subcat, n_tiendas, pmin, pmax, spread, tiendas = row

    nombre_trunc = nombre[:COL_WIDTHS["nombre"]]
    marca_trunc  = marca[:COL_WIDTHS["marca"]]
    cat_trunc    = cat[:COL_WIDTHS["cat"]]
    subcat_trunc = subcat[:COL_WIDTHS["subcat"]]

    tc = tier_color(n_tiendas)

    line = (
        f"{pid:>{COL_WIDTHS['id']}}  "
        f"{nombre_trunc:<{COL_WIDTHS['nombre']}}  "
        f"{marca_trunc:<{COL_WIDTHS['marca']}}  "
        f"{cat_trunc:<{COL_WIDTHS['cat']}}  "
        f"{subcat_trunc:<{COL_WIDTHS['subcat']}}  "
        f"{color(str(n_tiendas), tc, BOLD):>{COL_WIDTHS['tiendas'] + 9}}  "
        f"{fmt_price(pmin):>{COL_WIDTHS['min']}}  "
        f"{fmt_price(pmax):>{COL_WIDTHS['max']}}  "
        f"{fmt_spread(spread):>{COL_WIDTHS['spread']}}"
    )
    print(line)
    if show_stores:
        print(f"  {color('Tiendas: ' + tiendas, DIM)}")


def print_summary(rows, min_tiendas):
    total       = len(rows)
    by_n        = {}
    by_cat      = {}
    for r in rows:
        n   = r[5]
        cat = r[3]
        by_n[n]   = by_n.get(n, 0) + 1
        by_cat[cat] = by_cat.get(cat, 0) + 1

    print()
    print(color(f"{'=' * 60}", BOLD))
    print(color(f"  RESUMEN  (mínimo {min_tiendas} tienda{'s' if min_tiendas > 1 else ''})", BOLD))
    print(color(f"{'=' * 60}", BOLD))
    print(f"  Total productos clusterizados : {color(total, BOLD)}")
    print()

    print(color("  Por número de tiendas:", BOLD))
    for n in sorted(by_n.keys(), reverse=True):
        bar = color("█" * by_n[n], tier_color(n))
        print(f"    {n} tienda{'s' if n > 1 else ''} : {color(by_n[n], BOLD):>4}  {bar}")

    print()
    print(color("  Por categoría:", BOLD))
    for cat, cnt in sorted(by_cat.items(), key=lambda x: -x[1]):
        print(f"    {cat:<22} : {color(cnt, BOLD):>4}")
    print()


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
def export_csv(rows, output_dir):
    import csv
    from datetime import datetime

    os.makedirs(output_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = os.path.join(output_dir, f"clusters_{ts}.csv")

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id_producto", "nombre_producto", "marca", "categoria", "subcategoria",
            "n_tiendas", "precio_min", "precio_max", "spread_pct", "tiendas"
        ])
        writer.writerows(rows)

    print(color(f"  CSV exportado: {path}", CYAN))
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Revisa qué productos fueron clusterizados en más de una tienda."
    )
    parser.add_argument("--min-tiendas", type=int, default=2,
                        help="Mínimo de tiendas para mostrar el producto (default: 2)")
    parser.add_argument("--categoria", type=str, default=None,
                        help="Filtrar por nombre de categoría (parcial, case-insensitive)")
    parser.add_argument("--buscar", type=str, default=None,
                        help="Buscar por nombre de producto (parcial, case-insensitive)")
    parser.add_argument("--todos", action="store_true",
                        help="Incluir productos de una sola tienda (min-tiendas = 1)")
    parser.add_argument("--tiendas", action="store_true",
                        help="Mostrar las tiendas debajo de cada producto")
    parser.add_argument("--exportar", action="store_true",
                        help="Exportar resultados a CSV en debug_tools/")
    args = parser.parse_args()

    if args.todos:
        args.min_tiendas = 1

    conn = get_connection()
    cur  = conn.cursor()

    print(color("\n--- Clusters de Productos en BD ---\n", BOLD))

    rows = fetch_clusters(cur, args.min_tiendas, args.categoria, args.buscar)
    conn.close()

    if not rows:
        print(f"  No se encontraron productos con ≥{args.min_tiendas} tiendas"
              + (f" en categoría '{args.categoria}'" if args.categoria else "")
              + (f" con búsqueda '{args.buscar}'" if args.buscar else "")
              + ".")
        return

    print_header()
    for row in rows:
        print_row(row, show_stores=args.tiendas)

    print_summary(rows, args.min_tiendas)

    if args.exportar:
        export_csv(rows, script_dir)


if __name__ == "__main__":
    main()
