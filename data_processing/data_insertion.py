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


# Cargamos los datos
df = pd.read_csv("processed_data/fuzzy_matched/normalized_products_2026-01-06.csv")

# 1. Insertamos las categorias si es que no existen
## Nos traemos las categorias ya existentes
with engine.connect() as conn:
    result = conn.execute(sa.text("SELECT * FROM categorias")).fetchall()
    cat_existentes = {row.nombre_categoria for row in result}

## Categorias
nombres_categorias = df["category"].unique().tolist()
nombres_categorias_json = [{"nombre_categoria": nombre} for nombre in nombres_categorias if nombre not in cat_existentes] # Filtramos las categorias que ya existen

with engine.connect() as conn:
    if len(nombres_categorias_json) > 0:
        conn.execute(sa.text("INSERT INTO categorias (nombre_categoria) VALUES(:nombre_categoria) ON CONFLICT (nombre_categoria) DO NOTHING"), nombres_categorias_json)
        conn.commit()
    else:
        print("No hay categorias nuevas para insertar.")

# 2. Insertamos las tiendas si es que no existen
## Nos traemos las tiendas ya existentes
with engine.connect() as conn:
    result = conn.execute(sa.text("SELECT * FROM tiendas")).fetchall()
    tiendas_existentes = {row.nombre_tienda for row in result}

## Filtramos las tiendas desde el df (ojo que no trae el dato de la url de la tienda, esto sera manual)
nombres_tiendas = df["site_name"].unique().tolist()
nombres_tiendas_json = [{"nombre_tienda": nombre} for nombre in nombres_tiendas if nombre not in tiendas_existentes] # Filtramos las tiendas que ya existen

with engine.connect() as conn:
    if len(nombres_tiendas_json) > 0:
        conn.execute(sa.text("INSERT INTO tiendas (nombre_tienda) VALUES(:nombre_tienda) ON CONFLICT (nombre_tienda) DO NOTHING"), nombres_tiendas_json)
        conn.commit()
    else:
        print("No hay tiendas nuevas para insertar.")

# 3. Insertamos las marcas de productos
## Nos traemos las marcas ya existentes
with engine.connect() as conn:
    result = conn.execute(sa.text("SELECT * FROM marcas")).fetchall()
    marcas_existentes = {row.nombre_marca for row in result}

## Filtramos las marcas desde el df
nombres_marcas = df["brand"][df["brand"].notna()].unique().tolist() # Eliminamos las marcas NaN
nombres_marcas_json = [{"nombre_marca": nombre} for nombre in nombres_marcas if nombre not in marcas_existentes] # Filtramos las marcas que ya existen

with engine.connect() as conn:
    if len(nombres_marcas_json) > 0:
        conn.execute(sa.text("INSERT INTO marcas (nombre_marca) VALUES(:nombre_marca) ON CONFLICT (nombre_marca) DO NOTHING"), nombres_marcas_json)
        conn.commit()
    else:
        print("No hay marcas nuevas para insertar.")


# # Insercion subcategorias + su id de categoria(si ya existen no revisamos)
# ## Subcategorias 
# nombres_subcategorias = df[["subcategory","category"]].drop_duplicates().values.tolist()
# nombres_subcategorias_json = [{"nombre_subcategoria": subcategoria , "categoria": categoria, "id_categoria": None} for subcategoria,categoria in nombres_subcategorias]

# # Para no repetir la busqueda de tupla categoria/subcategoria para obtener la id de la categoria
# vistos = set()
# # Buscamos la id de la categoria
# with engine.connect() as conn:
#     for subcategoria, categoria in nombres_subcategorias:
#         if (subcategoria , categoria) not in vistos:
#             query = f"select id_categoria from categorias c where nombre_categoria = '{categoria}';"
#             result = conn.execute(sa.text(query))
#             for row in result:
#                 nombres_subcategorias_json[nombres_subcategorias.index([subcategoria,categoria])]["id_categoria"] = row[0]
#             vistos.add((subcategoria , categoria))
#         else:
#             print(f"Combinacion categoria-subcategoria: {categoria} - {subcategoria} ya revisada en este proceso.")

#     conn.execute(sa.text("INSERT INTO subcategorias (nombre_subcategoria, id_categoria) VALUES(:nombre_subcategoria, :id_categoria) ON CONFLICT (nombre_subcategoria) DO NOTHING"), nombres_subcategorias_json)
#     conn.commit()


# ## Por arreglar, mejorar el filtrado para insercion y evitar consumo de la secuencia innecesaria



