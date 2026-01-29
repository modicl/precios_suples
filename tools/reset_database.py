import os
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

# Configuración de la BD
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "suplementos")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "password")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def reset_db_to_zero():
    """
    Borra todos los datos de las tablas y reinicia los contadores de ID.
    Mantiene la estructura (schema) intacta.
    """
    try:
        engine = sa.create_engine(DATABASE_URL)
        with engine.connect() as conn:
            print(f"⚠️  ATENCIÓN: Conectado a {DB_NAME} en {DB_HOST}.")
            print("Se procederá a vaciar TODAS las tablas y reiniciar los IDs.")
            
            # Orden importante para evitar errores de Foreign Key si no se usara CASCADE,
            # pero con CASCADE es más flexible. Listamos todas las tablas del schema.
            tables = [
                "historia_precios",
                "producto_tienda",
                "productos",
                "subcategorias",
                "categorias",
                "marcas",
                "tiendas"
            ]
            
            # Construimos la query TRUNCATE
            # TRUNCATE TABLE t1, t2, ... RESTART IDENTITY CASCADE;
            tables_str = ", ".join(tables)
            query = sa.text(f"TRUNCATE TABLE {tables_str} RESTART IDENTITY CASCADE;")
            
            print(f"Ejecutando: TRUNCATE TABLE ... RESTART IDENTITY CASCADE;")
            conn.execute(query)
            conn.commit()
            
            print("✅ ¡Operación exitosa! La base de datos ha quedado en 0.")
            
    except Exception as e:
        print(f"❌ Error al resetear la base de datos: {e}")

if __name__ == "__main__":
    print("!!! ADVERTENCIA DE SEGURIDAD !!!")
    print("Este script borrará PERMANENTEMENTE todos los datos de la base de datos.")
    print("Se recomienda ejecutar 'tools/backup_database.py' primero.")
    
    confirm = input(f"Escribe 'BORRAR {DB_NAME}' para confirmar: ")
    
    if confirm == f"BORRAR {DB_NAME}":
        reset_db_to_zero()
    else:
        print("Operación cancelada. La frase de confirmación no coincide.")
