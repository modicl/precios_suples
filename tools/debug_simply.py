import os
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sa.create_engine(DATABASE_URL)

with engine.connect() as conn:
    print("--- Buscando 'Simply' en productos ---")
    rows = conn.execute(sa.text("SELECT * FROM productos WHERE nombre_producto ILIKE '%Simply%'")).fetchall()
    
    if not rows:
        print("No se encontraron productos 'Simply'.")
    
    for r in rows:
        print(f"Prod: {r.id_producto} - {r.nombre_producto}")
        # Get history
        hp = conn.execute(sa.text("""
            SELECT hp.fecha_precio, hp.precio 
            FROM historia_precios hp
            JOIN producto_tienda pt ON hp.id_producto_tienda = pt.id_producto_tienda
            WHERE pt.id_producto = :pid
        """), {"pid": r.id_producto}).fetchall()
        for h in hp:
            print(f"   Fecha: {h.fecha_precio}, Precio: {h.precio}")
