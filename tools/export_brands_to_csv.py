import os
import sqlalchemy as sa
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "suplementos")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "password")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def export_brands():
    """
    Exports all brands from the 'marcas' table to a CSV file.
    """
    try:
        engine = sa.create_engine(DATABASE_URL)
        print(f"--- Conectando a Base de Datos: {DB_NAME} ---")

        with engine.connect() as conn:
            # Query all brands
            query = sa.text("SELECT id_marca, nombre_marca FROM marcas ORDER BY nombre_marca ASC")
            
            # Use pandas to read sql
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("No se encontraron marcas en la base de datos.")
                return

            print(f"Marcas encontradas: {len(df)}")
            
            # Define output path
            output_dir = os.path.join(os.getcwd(), ".")
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y-%m-%d")
            output_file = os.path.join(output_dir, f"marcas_dictionary.csv")
            
            # Export to CSV
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"Archivo guardado exitosamente en:\n{output_file}")
            print("\nMuestra de las primeras 5 marcas:")
            print(df.head())

    except Exception as e:
        print(f"Error durante la exportacion: {e}")

if __name__ == "__main__":
    export_brands()
