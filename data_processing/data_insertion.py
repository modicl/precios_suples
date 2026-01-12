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
df = pd.read_csv("processed_data/fuzzy_matched/normalized_products_2026-01-12.csv")

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


# 4.Insercion subcategorias + su id de categoria(si ya existen no revisamos)

## Combinaciones ya existentes
with engine.connect() as conn:
#  Obtenemos todas las combinaciones categoria-subcategoria existentes en una query
    result = conn.execute(sa.text("SELECT sc.nombre_subcategoria, c.nombre_categoria FROM subcategorias sc JOIN categorias c ON sc.id_categoria = c.id_categoria")).fetchall()
    subcat_existentes = {(row.nombre_subcategoria, row.nombre_categoria) for row in result}
## Obtenemos todas las categorias existentes en una query
    cat_result = conn.execute(sa.text("SELECT nombre_categoria, id_categoria FROM categorias")).fetchall()
    cat_ids = {row.nombre_categoria: row.id_categoria for row in cat_result}

## Subcategorias y preparacion datos a insertar
nombres_subcategorias = df[["subcategory","category"]].drop_duplicates().values.tolist()
nombres_subcategorias_json = []

# Revisamos si la combinacion categoria-subcategoria ya existe
for subcategoria, categoria in nombres_subcategorias:
    if (subcategoria , categoria) not in subcat_existentes:
        nombres_subcategorias_json.append({
            "nombre_subcategoria": subcategoria,
            "id_categoria": cat_ids[categoria]
        })
        print(f"Nueva combinacion categoria-subcategoria: {categoria} - {subcategoria} agregada para insercion.")
    else:
        print(f"Combinacion categoria-subcategoria: {categoria} - {subcategoria} ya existe.")

# Insertamos las nuevas subcategorias
if len(nombres_subcategorias_json) > 0:
    with engine.connect() as conn:
        conn.execute(sa.text("INSERT INTO subcategorias (nombre_subcategoria, id_categoria) VALUES(:nombre_subcategoria, :id_categoria)"), nombres_subcategorias_json)
        conn.commit()

else:
    print("No hay subcategorias nuevas para insertar.")


# 5. Insercion de productos
## Primero obtenemos los ids necesarios para la insercion
with engine.connect() as conn:
    ## Obtenemos todas las subcategorias existentes en una query
    subcat_result = conn.execute(sa.text("SELECT nombre_subcategoria, id_subcategoria FROM subcategorias")).fetchall()
    subcat_ids = {row.nombre_subcategoria: row.id_subcategoria for row in subcat_result}

    ## Obtenemos las marcas exsitentes en una query
    marca_result = conn.execute(sa.text("SELECT nombre_marca, id_marca FROM marcas")).fetchall()
    marca_ids = {row.nombre_marca: row.id_marca for row in marca_result}

    ## Obtenemos los productos ya existentes
    prod_result = conn.execute(sa.text("SELECT nombre_producto, id_marca, id_subcategoria FROM productos")).fetchall()
    productos_existentes = {(row.nombre_producto, row.id_marca, row.id_subcategoria) for row in prod_result}

    # Preparamos los datos para insertar
    productos_json = []
    ## Filtramos las filas con subcategoria NaN
    df_productos = df[df["subcategory"].notna()]
    # Usamos normalized_name para el nombre del producto para agrupar variantes
    for index, row in df_productos.iterrows():
        # Usamos normalized_name si existe, si no product_name
        nombre_final = row["normalized_name"] if "normalized_name" in row and pd.notna(row["normalized_name"]) else row["product_name"]
        
        productos_json.append({"nombre_producto": nombre_final,
                        "url_imagen": row["image_url"],
                        "url_thumb_imagen": row["thumbnail_image_url"],
                        "descripcion": row["description"],
                        "id_marca" : marca_ids[row["brand"]] if pd.notna(row["brand"]) and row["brand"] in marca_ids else 14,# Si no tiene marca o no existe en la tabla, asignamos id_marca = 14 (Marca: 'Sin Marca')
                        "id_subcategoria": subcat_ids[row["subcategory"]]
                        })
    
    # Lista final a insertar
    productos_insertar_json = []
    ## Revisamos si cada producto ya existe (implemetnar logica de conflicto por nombre_producto, id_marca, id_subcategoria)
    seen_products = set()
    
    for producto in productos_json:
        nombre_producto = producto["nombre_producto"]
        id_marca = producto["id_marca"]
        id_subcategoria = producto["id_subcategoria"]
        
        # Clave unica para evitar duplicados en la lista de insercion
        key = (nombre_producto, id_marca, id_subcategoria)
        
        if key not in productos_existentes and key not in seen_products:
            productos_insertar_json.append(producto)
            seen_products.add(key)
            # print(f"El producto {nombre_producto} agregado para insercion.") # Reduce spam
    
    # Insertamos los productos
    if(len(productos_insertar_json) > 0):
        print(f"Insertando {len(productos_insertar_json)} productos nuevos...")
        conn.execute(sa.text("INSERT INTO productos (nombre_producto, url_imagen, url_thumb_imagen, descripcion, id_marca, id_subcategoria) VALUES(:nombre_producto, :url_imagen, :url_thumb_imagen, :descripcion, :id_marca, :id_subcategoria) ON CONFLICT (nombre_producto, id_marca,id_subcategoria) DO NOTHING"), productos_insertar_json)
        conn.commit()
    else:
        print("No hay productos nuevos para insertar.")

# 6. Insercion ProductoTienda

## Tiendas y sus ids

with engine.connect() as conn:
    tienda_result = conn.execute(sa.text("SELECT id_tienda, nombre_tienda FROM tiendas")).fetchall()
    tienda_ids = {row.nombre_tienda: row.id_tienda for row in tienda_result}

## Productos y sus ids
with engine.connect() as conn:
    prod_result = conn.execute(sa.text("SELECT id_producto, nombre_producto FROM productos")).fetchall()
    producto_ids = {row.nombre_producto: row.id_producto for row in prod_result}

## ProductoTienda actuales
with engine.connect() as conn:
    prod_tiend_result = conn.execute(sa.text("SELECT id_producto, id_tienda FROM producto_tienda")).fetchall()
    producto_tienda_existentes = {(row.id_producto, row.id_tienda) for row in prod_tiend_result}

## Preparamos los datos para insertar
productos_tienda_json = []
productos_tienda_vistos = set() # Evitamos la duplibacion en la primera vez
for index, row in df.iterrows():
    # Buscamos el ID usando el normalized_name que es como se guardó en la BD
    nombre_busqueda = row["normalized_name"] if "normalized_name" in row and pd.notna(row["normalized_name"]) else row["product_name"]
    id_producto = producto_ids.get(nombre_busqueda)
    
    id_tienda = tienda_ids.get(row["site_name"])
    if id_producto and id_tienda:
        if (id_producto, id_tienda) not in productos_tienda_vistos and (id_producto, id_tienda) not in producto_tienda_existentes:
            productos_tienda_json.append({
                "id_producto": id_producto,
                "id_tienda": id_tienda,
            })
            productos_tienda_vistos.add((id_producto, id_tienda))
            # print(f"Link {id_producto}-{id_tienda} agregado.")

print(f"Total combinaciones producto-tienda a insertar: {len(productos_tienda_json)}")
# print(f"Las combinaciones son : {productos_tienda_json}")


if len(productos_tienda_json) > 0:
    with engine.connect() as conn:
        conn.execute(sa.text("INSERT INTO producto_tienda (id_producto, id_tienda) VALUES(:id_producto, :id_tienda) ON CONFLICT DO NOTHING"), productos_tienda_json)
        conn.commit()



# 7. Insercion Historial Precios

## Obtenemos los ids de producto_tienda
with engine.connect() as conn:
    prod_tiend_result = conn.execute(sa.text("SELECT id_producto_tienda, id_producto, id_tienda FROM producto_tienda")).fetchall()
    producto_tienda_ids = {(row.id_producto, row.id_tienda): row.id_producto_tienda for row in prod_tiend_result}

## Preparamos los datos para insertar
historial_precios_json = []
for index, row in df.iterrows():
    # Buscamos ID producto usando normalized_name
    nombre_busqueda = row["normalized_name"] if "normalized_name" in row and pd.notna(row["normalized_name"]) else row["product_name"]
    id_producto = producto_ids.get(nombre_busqueda)
     
    id_tienda = tienda_ids.get(row["site_name"])
    id_producto_tienda = producto_tienda_ids.get((id_producto, id_tienda))
    if id_producto_tienda:
        historial_precios_json.append({
            "id_producto_tienda": id_producto_tienda,
            "precio": row["price"],
            "fecha_precio": pd.to_datetime(row["date"]).date()
        })
# Insertamos los datos
if len(historial_precios_json) > 0:
    with engine.connect() as conn:
        conn.execute(sa.text("INSERT INTO historia_precios (id_producto_tienda, precio, fecha_precio) VALUES(:id_producto_tienda, :precio, :fecha_precio)"), historial_precios_json)
        conn.commit()