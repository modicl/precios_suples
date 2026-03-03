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
    print("Agregando constraint UNIQUE a historia_precios(id_producto_tienda, fecha_precio)...")
    try:
        # Check if already exists? Postgres will error if so, which is fine.
        conn.execute(sa.text("""
            ALTER TABLE historia_precios 
            ADD CONSTRAINT uq_precio_fecha UNIQUE (id_producto_tienda, fecha_precio)
        """))
        conn.commit()
        print("Constraint agregado exitosamente.")
    except Exception as e:
        print(f"Error (o ya existe): {e}")
