import os
import sys
import sqlalchemy as sa
import time

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local' en db_multiconnect.")
        sys.exit(1)
    return sa.create_engine(local_target['url'])

def refresh_views(engine):
    print("\n--- Actualizando Vistas Materializadas ---")
    start_time = time.time()
    
    try:
        with engine.connect() as conn:
            # Note: CONCURRENTLY requires unique index on the view. 
            # If not present, it fails. We assume it exists or fallback.
            # For robustness in test script, we try CONCURRENTLY, if fail, try normal.
            
            views = [
                "mv_main_query", 
                "mv_main_query_v2", 
                "mv_click_stats", 
                "mv_search_stats", 
                "mv_active_brands"
            ]
            
            for view in views:
                print(f"  Actualizando {view}...")
                try:
                    conn.execute(sa.text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY public.{view};"))
                except Exception as e:
                    # print(f"  [Info] CONCURRENTLY failed for {view} (maybe no index?), trying normal refresh...")
                    # Transaction might be aborted if using same conn? 
                    # Yes, need rollback or new transaction.
                    # Since we are in autocommit or simple transaction mode, let's just let it fail or use separate transactions.
                    # Better: Just run REFRESH MATERIALIZED VIEW without concurrently if unsure, but concurrently is better for prod.
                    # Let's trust the reference script which used CONCURRENTLY for first 3 and normal for last 2.
                    pass

            # Re-reading reference:
            # mv_main_query -> CONCURRENTLY
            # mv_main_query_v2 -> CONCURRENTLY
            # mv_click_stats -> CONCURRENTLY
            # mv_search_stats -> NORMAL
            # mv_active_brands -> NORMAL
            
            # Re-doing with specific logic
            pass
            
    except Exception as e:
        print(f"Error general en vistas: {e}")

    # Retry with precise logic
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        
        try:
            print("  mv_main_query (Concurrently)...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_main_query;"))
        except Exception as e: print(f"  Error mv_main_query: {e}")

        try:
            print("  mv_main_query_v2 (Concurrently)...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_main_query_v2;"))
        except Exception as e: print(f"  Error mv_main_query_v2: {e}")

        try:
            print("  mv_click_stats (Concurrently)...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_click_stats;"))
        except Exception as e: print(f"  Error mv_click_stats: {e}")

        try:
            print("  mv_search_stats...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW public.mv_search_stats;"))
        except Exception as e: print(f"  Error mv_search_stats: {e}")

        try:
            print("  mv_active_brands...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW public.mv_active_brands;"))
        except Exception as e: print(f"  Error mv_active_brands: {e}")

    elapsed = time.time() - start_time
    print(f"Vistas actualizadas en {elapsed:.2f} segundos.")

def main():
    print("--- PASO 6: Refresh Materialized Views ---")
    engine = get_local_engine()
    refresh_views(engine)

if __name__ == "__main__":
    main()
