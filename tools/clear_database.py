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

def clear_db():
    try:
        engine = sa.create_engine(DATABASE_URL)
        with engine.connect() as conn:
            print(f"Conectado a {DB_NAME}. Eliminando datos...")
            
            # TRUNCATE con CASCADE borra las tablas y sus dependientes
            # RESTART IDENTITY reinicia los contadores de ID
            query = sa.text("""
                TRUNCATE TABLE 
                    historia_precios, 
                    producto_tienda, 
                    productos, 
                    subcategorias, 
                    categorias, 
                    marcas, 
                    tiendas 
                RESTART IDENTITY CASCADE;
            """)
            
            conn.execute(query)
            conn.commit()
            print("¡Todas las tablas han sido vaciadas exitosamente!")
            
    except Exception as e:
        print(f"Error al vaciar la base de datos: {e}")

if __name__ == "__main__":
    confirm = input("¿Estás seguro que quieres BORRAR TODOS LOS DATOS? (escribe 'si' para confirmar): ")
    if confirm.lower() == 'si':
        clear_db()
    else:
        print("Operación cancelada.")
