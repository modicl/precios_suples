import os
import sqlalchemy as sa
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "suplementos")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "password")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def query_db():
    try:
        engine = sa.create_engine(DATABASE_URL)
        print(f"--- Consultando Base de Datos: {DB_NAME} ---")
        
        with engine.connect() as conn:
            # 1. Find the product ID for the Foodtech item
            query_prod = sa.text("SELECT * FROM productos WHERE nombre_producto ILIKE '%AMINO WHEY PROTEIN 2LB - FOODTECH%'")
            products = conn.execute(query_prod).fetchall()
            
            if not products:
                print("No se encontró el producto en la tabla 'productos'.")
                return
                
            print(f"\nProductos encontrados ({len(products)}):")
            for p in products:
                print(f"ID: {p.id_producto} | Nombre: {p.nombre_producto} | ID_Marca: {p.id_marca} | ID_Sub: {p.id_subcategoria}")
                
                # Check links for this product
                pid = p.id_producto
                query_links = sa.text(f"""
                    SELECT pt.id_producto_tienda, t.nombre_tienda 
                    FROM producto_tienda pt
                    JOIN tiendas t ON pt.id_tienda = t.id_tienda
                    WHERE pt.id_producto = {pid}
                """)
                links = conn.execute(query_links).fetchall()
                print(f"  -> Vinculado a {len(links)} tiendas:")
                for l in links:
                    print(f"     - {l.nombre_tienda} (ID_PT: {l.id_producto_tienda})")
                    
                    # Check prices
                    query_prices = sa.text(f"SELECT precio, fecha_precio FROM historia_precios WHERE id_producto_tienda = {l.id_producto_tienda}")
                    prices = conn.execute(query_prices).fetchall()
                    for pr in prices:
                        print(f"       -> Precio: {pr.precio} ({pr.fecha_precio})")

    except Exception as e:
        print(f"Error consulta: {e}")

if __name__ == "__main__":
    query_db()
