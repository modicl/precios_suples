import os
import sys
import sqlalchemy as sa
from dotenv import load_dotenv
import time

# Add project root to path to ensure modules can be imported if needed in future
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load env variables (automatically searches parent directories)
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def refresh_views():
    print("Iniciando actualización de vistas materializadas...")
    start_time = time.time()
    
    try:
        engine = sa.create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # mv_main_query
            print("Actualizando mv_main_query...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_main_query;"))
            print("mv_main_query actualizada.")

            # mv_main_query_v2
            print("Actualizando mv_main_query_v2...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_main_query_v2;"))
            print("mv_main_query_v2 actualizada.")

            # mv_click_stats
            print("Actualizando mv_click_stats...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_click_stats;"))
            print("mv_click_stats actualizada.")

            # mv_search_stats
            print("Actualizando mv_search_stats...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW public.mv_search_stats;"))
            print("mv_search_stats actualizada.")

            # mv_active_brands
            print("Actualizando mv_active_brands...")
            conn.execute(sa.text("REFRESH MATERIALIZED VIEW public.mv_active_brands;"))
            print("mv_active_brands actualizada.")
    
            conn.commit()
            
        elapsed = time.time() - start_time
        print(f"Vistas materializadas actualizadas correctamente en {elapsed:.2f} segundos.")
    except Exception as e:
        print(f"Error actualizando vistas: {e}")

if __name__ == "__main__":
    refresh_views()
