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

def force_reset():
    engine = sa.create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print(f"Forcing TRUNCATE on {DB_NAME}...")
        tables = [
            "historia_precios",
            "producto_tienda",
            "productos",
            "subcategorias",
            "categorias",
            "marcas",
            "tiendas"
        ]
        conn.execute(sa.text(f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY CASCADE;"))
        conn.commit()
        
        # Verify
        count = conn.execute(sa.text("SELECT count(*) FROM historia_precios")).scalar()
        print(f"Rows in historia_precios after truncate: {count}")
        
        print("Database reset complete.")

if __name__ == "__main__":
    force_reset()
