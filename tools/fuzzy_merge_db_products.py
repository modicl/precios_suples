"""
fuzzy_merge_db_products.py
==========================
One-shot script to detect and optionally merge near-duplicate products that are
already in the production database but were not clustered by the CSV pipeline.

Workflow:
    1. Run WITHOUT --execute to generate a report CSV for manual review.
    2. Inspect the report at tools/fuzzy_merge_report_<date>.csv
    3. Remove rows you do NOT want merged.
    4. Run WITH --execute --report <csv_file> to apply only the approved merges.

Usage:
    python tools/fuzzy_merge_db_products.py                          # generate report
    python tools/fuzzy_merge_db_products.py --execute                # apply ALL proposed merges
    python tools/fuzzy_merge_db_products.py --execute --report FILE  # apply only approved rows from CSV
"""

import os
import sys
import argparse
import csv
from collections import defaultdict
from datetime import datetime

import sqlalchemy as sa
from dotenv import load_dotenv
from rapidfuzz import fuzz

# ── Import helper functions from the v2 pipeline ──────────────────────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_processing_v2.normalize_products import (
    extract_sizes,
    detect_packaging,
    extract_pack_quantity,
    extract_flavors,
    check_critical_mismatch,
    check_percentage_mismatch,
)

# ── Thresholds (must match normalize_products.py) ─────────────────────────────
THRESHOLD_SET  = 83   # token_set_ratio  (primary scorer)
THRESHOLD_SORT = 65   # token_sort_ratio (secondary guard – AND logic)

INVALID_BRANDS = {"n/d", "nan", "none", ""}

load_dotenv()


def get_engine(use_prod: bool = True):
    if use_prod:
        url = os.getenv("DB_HOST_PROD")
        if not url:
            raise RuntimeError("DB_HOST_PROD not set in .env")
    else:
        user = os.getenv("DB_USER")
        pw   = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", "5432")
        name = os.getenv("DB_NAME")
        url  = f"postgresql://{user}:{pw}@{host}:{port}/{name}"
    return sa.create_engine(url)


def load_products(conn):
    rows = conn.execute(sa.text("""
        SELECT p.id_producto, p.nombre_producto, p.id_marca, p.id_subcategoria,
               m.nombre_marca, s.nombre_subcategoria
        FROM productos p
        LEFT JOIN marcas m ON p.id_marca = m.id_marca
        LEFT JOIN subcategorias s ON p.id_subcategoria = s.id_subcategoria
        WHERE p.nombre_producto IS NOT NULL
        ORDER BY p.id_marca, p.id_subcategoria, p.id_producto
    """)).fetchall()
    return [dict(r._mapping) for r in rows]


def is_match(p1: dict, p2: dict) -> bool:
    """
    Return True if p1 and p2 should be considered the same product.
    Mirrors the logic in normalize_products.normalize_names().
    """
    n1, n2 = p1['nombre_producto'], p2['nombre_producto']
    c1, c2 = n1.lower().strip(), n2.lower().strip()

    # Dual-scorer guard
    if fuzz.token_set_ratio(c1, c2) < THRESHOLD_SET:
        return False
    if fuzz.token_sort_ratio(c1, c2) < THRESHOLD_SORT:
        return False

    # Critical keyword mismatch
    if check_critical_mismatch(n1, n2):
        return False

    # Percentage mismatch
    if check_percentage_mismatch(n1, n2):
        return False

    # Pack quantity
    if extract_pack_quantity(n1) != extract_pack_quantity(n2):
        return False

    # Size
    if extract_sizes(n1) != extract_sizes(n2):
        return False

    # Packaging
    if detect_packaging(n1) != detect_packaging(n2):
        return False

    # Flavors
    if extract_flavors(n1) != extract_flavors(n2):
        return False

    # Brand: if both are known they must be the same brand
    b1 = (p1.get('nombre_marca') or 'N/D').lower().strip()
    b2 = (p2.get('nombre_marca') or 'N/D').lower().strip()
    if b1 not in INVALID_BRANDS and b2 not in INVALID_BRANDS:
        if fuzz.ratio(b1, b2) < 85:
            return False

    return True


def find_clusters(products: list[dict]) -> list[list[dict]]:
    """
    Group products by (id_marca, id_subcategoria) then find fuzzy duplicates
    within each group.  Returns a list of clusters (each cluster has ≥2 items).
    """
    groups: dict[tuple, list] = defaultdict(list)
    for p in products:
        groups[(p['id_marca'], p['id_subcategoria'])].append(p)

    all_clusters: list[list[dict]] = []

    for (marca, subcat), group in groups.items():
        if len(group) < 2:
            continue

        # Union-Find style clustering
        parent = {p['id_producto']: p['id_producto'] for p in group}
        prod_by_id = {p['id_producto']: p for p in group}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x, y):
            rx, ry = find(x), find(y)
            if rx != ry:
                # Keep the lower ID as root (older product = master)
                if rx < ry:
                    parent[ry] = rx
                else:
                    parent[rx] = ry

        ids = [p['id_producto'] for p in group]
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                if is_match(prod_by_id[ids[i]], prod_by_id[ids[j]]):
                    union(ids[i], ids[j])

        # Collect clusters with >1 member
        cluster_map: dict[int, list] = defaultdict(list)
        for pid in ids:
            cluster_map[find(pid)].append(prod_by_id[pid])

        for root_id, members in cluster_map.items():
            if len(members) > 1:
                members.sort(key=lambda p: p['id_producto'])
                all_clusters.append(members)

    return all_clusters


def write_report(clusters: list[list[dict]], report_path: str):
    with open(report_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow([
            'cluster_id', 'action',
            'master_id', 'master_nombre', 'master_marca', 'master_subcategoria',
            'dupe_id',   'dupe_nombre',   'dupe_marca',   'dupe_subcategoria',
            'score_set', 'score_sort',
        ])
        for cid, members in enumerate(clusters, 1):
            master = members[0]
            for dupe in members[1:]:
                n1, n2 = master['nombre_producto'], dupe['nombre_producto']
                writer.writerow([
                    cid, 'merge',
                    master['id_producto'], n1, master.get('nombre_marca'), master.get('nombre_subcategoria'),
                    dupe['id_producto'],   n2, dupe.get('nombre_marca'),   dupe.get('nombre_subcategoria'),
                    round(fuzz.token_set_ratio(n1.lower(), n2.lower()), 1),
                    round(fuzz.token_sort_ratio(n1.lower(), n2.lower()), 1),
                ])
    print(f"Reporte guardado en: {report_path}")
    print(f"Total clusters: {len(clusters)}  |  Duplicados a fusionar: {sum(len(c)-1 for c in clusters)}")


def load_approved_merges(report_path: str) -> list[tuple[int, int]]:
    """Read (master_id, dupe_id) pairs from a (possibly edited) report CSV."""
    pairs = []
    with open(report_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('action', '').strip().lower() == 'merge':
                pairs.append((int(row['master_id']), int(row['dupe_id'])))
    return pairs


def execute_merges(conn, merge_pairs: list[tuple[int, int]]):
    """
    For each (master_id, dupe_id) pair:
      - Reassign producto_tienda links from dupe to master (skip conflicts)
      - Migrate price history for conflicting links
      - Delete conflicting dupe links
      - Update click_analytics
      - Delete the dupe product
    """
    if not merge_pairs:
        print("No hay fusiones que ejecutar.")
        return

    print(f"Ejecutando {len(merge_pairs)} fusiones...")

    for master_id, dupe_id in merge_pairs:
        try:
            # Get master links {tienda_id: link_id}
            master_rows = conn.execute(sa.text(
                "SELECT id_producto_tienda, id_tienda FROM producto_tienda WHERE id_producto = :pid"
            ), {"pid": master_id}).fetchall()
            master_links = {r.id_tienda: r.id_producto_tienda for r in master_rows}

            # Get dupe links
            dupe_rows = conn.execute(sa.text(
                "SELECT id_producto_tienda, id_tienda FROM producto_tienda WHERE id_producto = :pid"
            ), {"pid": dupe_id}).fetchall()

            for dr in dupe_rows:
                if dr.id_tienda in master_links:
                    # Conflict: migrate history to master link, then delete dupe link
                    conn.execute(sa.text(
                        "UPDATE historia_precios SET id_producto_tienda = :new_id WHERE id_producto_tienda = :old_id"
                    ), {"new_id": master_links[dr.id_tienda], "old_id": dr.id_producto_tienda})
                    conn.execute(sa.text(
                        "DELETE FROM producto_tienda WHERE id_producto_tienda = :lid"
                    ), {"lid": dr.id_producto_tienda})
                else:
                    # No conflict: reassign link to master
                    conn.execute(sa.text(
                        "UPDATE producto_tienda SET id_producto = :master_id WHERE id_producto_tienda = :lid"
                    ), {"master_id": master_id, "lid": dr.id_producto_tienda})
                    master_links[dr.id_tienda] = dr.id_producto_tienda

            # Update analytics (best effort)
            try:
                conn.execute(sa.text(
                    "UPDATE click_analytics SET id_producto = :master_id WHERE id_producto = :dupe_id"
                ), {"master_id": master_id, "dupe_id": dupe_id})
            except Exception:
                pass

            # Delete the dupe product
            conn.execute(sa.text("DELETE FROM productos WHERE id_producto = :pid"), {"pid": dupe_id})
            print(f"  ✓ Fusionado: {dupe_id} → {master_id}")

        except Exception as e:
            print(f"  ✗ Error fusionando {dupe_id} → {master_id}: {e}")
            raise  # re-raise to rollback the transaction


def main():
    parser = argparse.ArgumentParser(description="Detect and merge near-duplicate products in the DB.")
    parser.add_argument('--execute', action='store_true',
                        help='Apply the merges. Without this flag only a report is generated.')
    parser.add_argument('--report', type=str, default=None,
                        help='Path to an approved report CSV. Only the rows with action=merge are applied.')
    parser.add_argument('--local', action='store_true',
                        help='Use local DB instead of production.')
    args = parser.parse_args()

    engine = get_engine(use_prod=not args.local)

    with engine.connect() as conn:
        print("Cargando productos...")
        products = load_products(conn)
        print(f"Total productos: {len(products)}")

    print("Buscando clusters...")
    clusters = find_clusters(products)

    today = datetime.now().strftime("%Y-%m-%d")
    report_path = os.path.join(
        os.path.dirname(__file__),
        f"fuzzy_merge_report_{today}.csv"
    )

    if not args.execute:
        # ── Report-only mode ──────────────────────────────────────────────────
        write_report(clusters, report_path)
        print("\nRevisa el reporte y elimina las filas que NO quieras fusionar.")
        print(f"Luego ejecuta:\n  python tools/fuzzy_merge_db_products.py --execute --report {report_path}")
    else:
        # ── Execute mode ──────────────────────────────────────────────────────
        if args.report:
            if not os.path.exists(args.report):
                print(f"Error: no se encontró el archivo {args.report}")
                sys.exit(1)
            merge_pairs = load_approved_merges(args.report)
            print(f"Cargando {len(merge_pairs)} fusiones aprobadas desde {args.report}")
        else:
            # Build merge pairs from all detected clusters
            merge_pairs = []
            for members in clusters:
                master = members[0]
                for dupe in members[1:]:
                    merge_pairs.append((master['id_producto'], dupe['id_producto']))
            print(f"Aplicando TODAS las fusiones detectadas: {len(merge_pairs)}")

        if not merge_pairs:
            print("No hay fusiones que ejecutar.")
            return

        with engine.begin() as conn:
            execute_merges(conn, merge_pairs)

        print(f"\nFusión completada. {len(merge_pairs)} duplicados eliminados.")
        # Save a final report of what was done
        write_report(clusters, report_path)


if __name__ == "__main__":
    main()
