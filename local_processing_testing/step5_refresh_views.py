"""
step5_refresh_views.py — Refresca las vistas materializadas.

Vistas:
  CONCURRENTLY (requiere unique index):
    mv_main_query, mv_main_query_v2, mv_click_stats
  NORMAL:
    mv_search_stats, mv_active_brands

Si CONCURRENTLY falla (p.ej. luego de un reset de esquema sin índice único),
cae automáticamente a REFRESH normal para esa vista.

Requiere AUTOCOMMIT — REFRESH MATERIALIZED VIEW no puede correr dentro de
una transacción normal.
"""

import os
import sys
import time
import sqlalchemy as sa

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

# (view_name, try_concurrently)
VIEWS = [
    ("mv_main_query",    True),
    ("mv_main_query_v2", True),
    ("mv_click_stats",   True),
    ("mv_search_stats",  False),
    ("mv_active_brands", False),
]


def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t["name"] == "Local"), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local' en db_multiconnect.")
        sys.exit(1)
    return sa.create_engine(local_target["url"])


def refresh_views(engine):
    print("\n--- Actualizando Vistas Materializadas ---")
    start_time = time.time()

    # REFRESH MATERIALIZED VIEW must run outside a transaction (AUTOCOMMIT).
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")

        for view, concurrent in VIEWS:
            if concurrent:
                # Try CONCURRENTLY first; fall back to normal if it fails
                # (e.g. unique index missing after a schema reset).
                try:
                    print(f"  {view} (CONCURRENTLY)...")
                    conn.execute(
                        sa.text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY public.{view};")
                    )
                    continue
                except Exception as e:
                    print(f"  {view}: CONCURRENTLY falló ({e}), reintentando sin CONCURRENTLY...")

            # Normal refresh (also the fallback path for concurrent views)
            try:
                print(f"  {view}...")
                conn.execute(
                    sa.text(f"REFRESH MATERIALIZED VIEW public.{view};")
                )
            except Exception as e:
                print(f"  {view}: ERROR — {e}")

    elapsed = time.time() - start_time
    print(f"Vistas actualizadas en {elapsed:.2f} segundos.")


def main():
    print("--- PASO 5: Refresh Materialized Views ---")
    engine = get_local_engine()
    refresh_views(engine)


if __name__ == "__main__":
    main()
