import os
import sys
import sqlalchemy as sa
from dotenv import load_dotenv
import time

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def refresh_views_db(engine, db_name):
    print(f"\n--- Refreshing Materialized Views for: {db_name} ---")
    start_time = time.time()
    
    try:
        with engine.connect() as conn:
            # mv_main_query
            print(f"[{db_name}] Actualizando mv_main_query...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_main_query;"))
            
            # mv_main_query_v2
            print(f"[{db_name}] Actualizando mv_main_query_v2...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_main_query_v2;"))
            
            # mv_click_stats
            print(f"[{db_name}] Actualizando mv_click_stats...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_click_stats;"))
            
            # mv_search_stats
            print(f"[{db_name}] Actualizando mv_search_stats...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW public.mv_search_stats;"))
            
            # mv_active_brands
            print(f"[{db_name}] Actualizando mv_active_brands...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW public.mv_active_brands;"))
            
            conn.commit()
            
        elapsed = time.time() - start_time
        print(f"[{db_name}] Vistas actualizadas correctamente en {elapsed:.2f} segundos.")
    except Exception as e:
        print(f"[{db_name}] Error actualizando vistas: {e}")
        # Note: If CONCURRENTLY fails (e.g. view not populated yet or no index), 
        # normally one might retry without CONCURRENTLY, but usually setup assumes it exists.

def main():
    load_dotenv()
    
    targets = get_targets()
    if not targets:
        print("Error: No database targets found.")
        return

    print(f"Found database targets: {[t['name'] for t in targets]}")

    for target in targets:
        db_name = target["name"]
        db_url = target["url"]
        
        try:
            print(f"\nConnecting to {db_name}...")
            engine = sa.create_engine(db_url)
            refresh_views_db(engine, db_name)
        except Exception as e:
            print(f"Error connecting/processing {db_name}: {e}")

if __name__ == "__main__":
    main()
