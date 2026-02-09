import os
import sys
import sqlalchemy as sa
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from tools.db_multiconnect import get_targets

def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local'.")
        sys.exit(1)
    return sa.create_engine(local_target['url'])

def reset_db():
    print("--- [PELIGRO] RESETEANDO BASE DE DATOS LOCAL ---")
    print("Manteniendo: Categorías, Subcategorías, Marcas, Tiendas.")
    print("Borrando: Historia Precios, Producto Tienda, Productos, Click Analytics.")
    
    confirm = input("¿Estás seguro? Escribe 'SI' para continuar: ")
    if confirm != 'SI':
        print("Cancelado.")
        return

    engine = get_local_engine()
    
    with engine.begin() as conn:
        print("Ejecutando TRUNCATE CASCADE...")
        # Order matters due to FKs
        # 1. historia_precios depends on producto_tienda
        # 2. producto_tienda depends on productos
        # 3. click_analytics depends on productos (usually)
        
        # TRUNCATE ... CASCADE handles dependencies automatically
        try:
            conn.execute(text("TRUNCATE TABLE historia_precios, producto_tienda, productos, click_analytics RESTART IDENTITY CASCADE"))
            print("Tablas truncadas correctamente.")
        except Exception as e:
            print(f"Error truncando (posiblemente tabla no existe): {e}")
            # Fallback one by one
            # conn.execute(text("DELETE FROM historia_precios")) ...
            
        print("Refrescando vistas (quedarán vacías)...")
        try:
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_main_query"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_main_query_v2"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_click_stats"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_search_stats"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mv_active_brands"))
        except Exception as e:
            print(f"Advertencia refrescando vistas: {e}")

    print("Base de datos reseteada y limpia.")

if __name__ == "__main__":
    reset_db()
