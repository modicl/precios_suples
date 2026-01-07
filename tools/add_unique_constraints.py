import os
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "suplementos")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "password")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def add_constraints():
    try:
        engine = sa.create_engine(DATABASE_URL)
        with engine.connect() as conn:
            print("Aplicando restricciones UNIQUE...")
            
            # Categorias
            try:
                conn.execute(sa.text("ALTER TABLE categorias ADD CONSTRAINT unique_nombre_categoria UNIQUE (nombre_categoria);"))
                print("- Constraint unique_nombre_categoria agregada.")
            except Exception as e:
                print(f"- (Info) Categorias: {e}")

            # Marcas
            try:
                conn.execute(sa.text("ALTER TABLE marcas ADD CONSTRAINT unique_nombre_marca UNIQUE (nombre_marca);"))
                print("- Constraint unique_nombre_marca agregada.")
            except Exception as e:
                print(f"- (Info) Marcas: {e}")

            # Tiendas
            try:
                conn.execute(sa.text("ALTER TABLE tiendas ADD CONSTRAINT unique_nombre_tienda UNIQUE (nombre_tienda);"))
                print("- Constraint unique_nombre_tienda agregada.")
            except Exception as e:
                print(f"- (Info) Tiendas: {e}")
                
            conn.commit()
            print("Migración completada.")
            
    except Exception as e:
        print(f"Error general: {e}")

if __name__ == "__main__":
    add_constraints()
