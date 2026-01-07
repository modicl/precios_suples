import os
import sqlalchemy as sa
import pandas as pd
from dotenv import load_dotenv

# 1. Cargar variables de entorno
load_dotenv() # Carga el archivo .env por defecto

# 2. Obtener variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# 3. Crear string de conexión
# Formato: postgresql://user:password@host:port/dbname
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    # 4. Crear motor
    engine = sa.create_engine(DATABASE_URL)
    
    # 5. Probar conexión
    with engine.connect() as conn:
        print(f"Conexión exitosa a la base de datos: {DB_NAME}")
        
except Exception as e:
    print(f"Error conectando a la base de datos: {e}")


with engine.connect() as conn:
    query = "select id_categoria from categorias c where nombre_categoria = 'Creatinas';"
    result = conn.execute(sa.text(query))
    for row in result:
        print(row[0])

## NOTA PARA MI : Independiente de la cantidad de columnas, el resultado es una tupla