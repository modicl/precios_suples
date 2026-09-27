"""
step7_refresh_views.py — Refresca las vistas materializadas.

Vistas:
  CONCURRENTLY (requiere unique index):
    mv_main_query, mv_main_query_v2, mv_click_stats
  NORMAL:
    mv_search_stats, mv_active_brands, mv_active_subcategories

Si CONCURRENTLY falla (p.ej. luego de un reset de esquema sin índice único),
cae automáticamente a REFRESH normal para esa vista.

Requiere AUTOCOMMIT — REFRESH MATERIALIZED VIEW no puede correr dentro de
una transacción normal.

Timeouts aplicados en producción para evitar esperas infinitas por lock contention
(el backend puede tener queries activas sobre las vistas mientras se refresca):
  lock_timeout      — tiempo máximo esperando adquirir el lock
  statement_timeout — tiempo máximo de ejecución por query (safety net)
"""

import os
import sys
import time
import sqlalchemy as sa

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from shared.db_multiconnect import get_targets

# (view_name, try_concurrently)
VIEWS = [
    ("mv_main_query",          True),
    ("mv_main_query_v2",       True),
    ("mv_click_stats",         True),
    ("mv_search_stats",        False),
    ("mv_active_brands",       False),
    ("mv_active_subcategories", False),
]

# Timeouts solo para producción (Neon puede tener lock contention con el backend)
LOCK_TIMEOUT      = "30s"   # falla si no puede adquirir lock en 30s
STATEMENT_TIMEOUT = "300s"  # falla si la query tarda más de 5 min


def _unpooled_url(url: str) -> str:
    """
    Neon: convierte la URL del pooler (PgBouncer) a conexión directa.
    El pooler rechaza parámetros de startup como lock_timeout.
    REFRESH MATERIALIZED VIEW es una query larga — no apropiada para el pool.
    """
    return url.replace("-pooler.", ".", 1)


def _make_engine(url: str, is_prod: bool) -> sa.Engine:
    if is_prod:
        url = _unpooled_url(url)
        connect_args: dict = {
            "connect_timeout": 30,
            # Parámetros de startup (nivel protocolo). Funcionan en conexión directa, no en pooler.
            "options": (
                f"-c lock_timeout={LOCK_TIMEOUT} "
                f"-c statement_timeout={STATEMENT_TIMEOUT}"
            ),
            # Keepalives: detecta drops TCP silenciosos de Neon (~80s: 30 + 10×5)
            "keepalives":          1,
            "keepalives_idle":     30,
            "keepalives_interval": 10,
            "keepalives_count":    5,
        }
    else:
        connect_args = {"connect_timeout": 30}

    return sa.create_engine(url, connect_args=connect_args)


def refresh_views(engine, db_name=""):
    print(f"\n--- Actualizando Vistas Materializadas [{db_name}] ---")
    start_time = time.time()

    # engine.execution_options() es la forma correcta en SQLAlchemy 2.x
    # para garantizar AUTOCOMMIT antes de abrir la conexión.
    with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
        for view, concurrent in VIEWS:
            t0 = time.time()
            if concurrent:
                try:
                    print(f"  {view} (CONCURRENTLY)...", end=" ", flush=True)
                    conn.execute(
                        sa.text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY public.{view};")
                    )
                    print(f"OK ({time.time()-t0:.1f}s)")
                    continue
                except Exception as e:
                    err = str(e).split("\n")[0]
                    print(f"CONCURRENTLY falló ({err}), reintentando sin CONCURRENTLY...")

            try:
                print(f"  {view}...", end=" ", flush=True)
                conn.execute(
                    sa.text(f"REFRESH MATERIALIZED VIEW public.{view};")
                )
                print(f"OK ({time.time()-t0:.1f}s)")
            except Exception as e:
                err = str(e).split("\n")[0]
                print(f"ERROR — {err}")

    elapsed = time.time() - start_time
    print(f"Vistas actualizadas en {elapsed:.2f} segundos.")


def main():
    print("--- PASO 7: Refresh Materialized Views ---")
    targets = get_targets()
    print(f"Destinos de BD encontrados: {[t['name'] for t in targets]}")

    for target in targets:
        db_name = target["name"]
        is_prod = db_name.lower() == "production"
        try:
            engine = _make_engine(target["url"], is_prod=is_prod)
            refresh_views(engine, db_name=db_name)
        except Exception as e:
            print(f"[ERROR FATAL] {db_name}: {e}")


if __name__ == "__main__":
    main()
