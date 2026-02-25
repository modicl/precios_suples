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
    print("--- Limpiando duplicados en historia_precios ---")
    
    # Logic: Keep the one with the highest id_historia_precio for each (id_producto_tienda, fecha_precio) group
    # Delete the others.
    
    query_dedup = sa.text("""
        DELETE FROM historia_precios a USING historia_precios b
        WHERE a.id_historia_precio < b.id_historia_precio
        AND a.id_producto_tienda = b.id_producto_tienda
        AND a.fecha_precio = b.fecha_precio
    """)
    
    res = conn.execute(query_dedup)
    conn.commit()
    
    print(f"Eliminados {res.rowcount} registros duplicados.")
