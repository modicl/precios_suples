import os
import sys
import sqlalchemy as sa
import pandas as pd
import psycopg2.extras
from dotenv import load_dotenv
import glob
import shutil
from datetime import datetime

# Agregar el directorio raíz al path para importar tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def clean_text(text):
    """Normalize text for case-insensitive comparison."""
    if isinstance(text, str):
        return text.lower().strip()
    return ""

def insert_data_into_db(engine, df, db_name):
    """
    Ejecuta la lógica de inserción para una conexión de base de datos específica.
    """
    print(f"\n--- Iniciando inserción en: {db_name} ---")

    # 1. Insertamos las categorias si es que no existen
    ## Nos traemos las categorias ya existentes
    with engine.connect() as conn:
        result = conn.execute(sa.text("SELECT * FROM categorias")).fetchall()
        # Usamos un set de nombres normalizados para comparación
        cat_existentes_norm = {clean_text(row.nombre_categoria) for row in result}

    ## Categorias
    nombres_categorias = df["category"].dropna().unique().tolist()
    # Filtramos las categorias que ya existen (comparando normalizado)
    nombres_categorias_json = [{"nombre_categoria": nombre} for nombre in nombres_categorias if clean_text(nombre) not in cat_existentes_norm]

    with engine.connect() as conn:
        if len(nombres_categorias_json) > 0:
            conn.execute(sa.text("INSERT INTO categorias (nombre_categoria) VALUES(:nombre_categoria) ON CONFLICT (nombre_categoria) DO NOTHING"), nombres_categorias_json)
            conn.commit()
            print(f"[{db_name}] Categorías insertadas: {len(nombres_categorias_json)}")
        else:
            print(f"[{db_name}] No hay categorias nuevas para insertar.")

    # 2. Insertamos las tiendas si es que no existen
    ## Nos traemos las tiendas ya existentes
    with engine.connect() as conn:
        result = conn.execute(sa.text("SELECT * FROM tiendas")).fetchall()
        tiendas_existentes_norm = {clean_text(row.nombre_tienda) for row in result}

    ## Filtramos las tiendas desde el df (ojo que no trae el dato de la url de la tienda, esto sera manual)
    nombres_tiendas = df["site_name"].dropna().unique().tolist()
    # Filtramos las tiendas que ya existen
    nombres_tiendas_json = [{"nombre_tienda": nombre} for nombre in nombres_tiendas if clean_text(nombre) not in tiendas_existentes_norm] 

    with engine.connect() as conn:
        if len(nombres_tiendas_json) > 0:
            conn.execute(sa.text("INSERT INTO tiendas (nombre_tienda) VALUES(:nombre_tienda) ON CONFLICT (nombre_tienda) DO NOTHING"), nombres_tiendas_json)
            conn.commit()
            print(f"[{db_name}] Tiendas insertadas: {len(nombres_tiendas_json)}")
        else:
            print(f"[{db_name}] No hay tiendas nuevas para insertar.")

    # 3. Insertamos las marcas de productos
    ## Nos traemos las marcas ya existentes
    with engine.connect() as conn:
        result = conn.execute(sa.text("SELECT * FROM marcas")).fetchall()
        marcas_existentes_norm = {clean_text(row.nombre_marca) for row in result}


    ## Filtramos las marcas desde el df
    nombres_marcas = df["brand"][df["brand"].notna()].unique().tolist() # Eliminamos las marcas NaN
    nombres_marcas_json = [{"nombre_marca": nombre} for nombre in nombres_marcas if clean_text(nombre) not in marcas_existentes_norm] # Filtramos las marcas que ya existen

    with engine.connect() as conn:
        if len(nombres_marcas_json) > 0:
            conn.execute(sa.text("INSERT INTO marcas (nombre_marca) VALUES(:nombre_marca) ON CONFLICT (nombre_marca) DO NOTHING"), nombres_marcas_json)
            conn.commit()
            print(f"[{db_name}] Marcas insertadas: {len(nombres_marcas_json)}")

        else:
            print(f"[{db_name}] No hay marcas nuevas para insertar.")


    # 4.Insercion subcategorias + su id de categoria(si ya existen no revisamos)

    ## Combinaciones ya existentes
    with engine.connect() as conn:
    #  Obtenemos todas las combinaciones categoria-subcategoria existentes en una query
        result = conn.execute(sa.text("SELECT sc.nombre_subcategoria, c.nombre_categoria FROM subcategorias sc JOIN categorias c ON sc.id_categoria = c.id_categoria")).fetchall()
        # Guardamos combinaciones normalizadas
        subcat_existentes_norm = {(clean_text(row.nombre_subcategoria), clean_text(row.nombre_categoria)) for row in result}
        
    ## Obtenemos todas las categorias existentes en una query
        cat_result = conn.execute(sa.text("SELECT nombre_categoria, id_categoria FROM categorias")).fetchall()
        # Mapa de nombre normalizado -> id
        cat_ids_norm = {clean_text(row.nombre_categoria): row.id_categoria for row in cat_result}

    ## Subcategorias y preparacion datos a insertar
    nombres_subcategorias = df[["subcategory","category"]].drop_duplicates().values.tolist()
    nombres_subcategorias_json = []

    # Revisamos si la combinacion categoria-subcategoria ya existe
    for subcategoria, categoria in nombres_subcategorias:
        # Use normalized keys for check and lookup
        sub_norm = clean_text(subcategoria)
        cat_norm = clean_text(categoria)
        
        if (sub_norm, cat_norm) not in subcat_existentes_norm:
            # Recuperamos ID usando el nombre normalizado
            if cat_norm in cat_ids_norm:
                nombres_subcategorias_json.append({
                    "nombre_subcategoria": subcategoria, # Guardamos nombre original
                    "id_categoria": cat_ids_norm[cat_norm]
                })
                # print(f"Nueva combinacion categoria-subcategoria: {categoria} - {subcategoria} agregada para insercion.")
        else:
            # print(f"Combinacion categoria-subcategoria: {categoria} - {subcategoria} ya existe.")
            pass

    # Insertamos las nuevas subcategorias
    if len(nombres_subcategorias_json) > 0:
        with engine.connect() as conn:
            conn.execute(sa.text("INSERT INTO subcategorias (nombre_subcategoria, id_categoria) VALUES(:nombre_subcategoria, :id_categoria)"), nombres_subcategorias_json)
            conn.commit()
            print(f"[{db_name}] Subcategorías insertadas: {len(nombres_subcategorias_json)}")

    else:
        print(f"[{db_name}] No hay subcategorias nuevas para insertar.")


    # 5. Insercion de productos
    ## Primero obtenemos los ids necesarios para la insercion
    with engine.connect() as conn:
        ## Obtenemos todas las subcategorias existentes en una query
        subcat_result = conn.execute(sa.text("SELECT nombre_subcategoria, id_subcategoria FROM subcategorias")).fetchall()
        subcat_ids_norm = {clean_text(row.nombre_subcategoria): row.id_subcategoria for row in subcat_result}

        ## Obtenemos las marcas exsitentes en una query
        marca_result = conn.execute(sa.text("SELECT nombre_marca, id_marca FROM marcas")).fetchall()
        marca_ids_norm = {clean_text(row.nombre_marca): row.id_marca for row in marca_result}

        ## Obtenemos los productos ya existentes
        # prod_result = conn.execute(sa.text("SELECT nombre_producto, id_marca, id_subcategoria FROM productos")).fetchall()
        # Set con tupla normalizada: (normalized_name, id_marca, id_subcategoria)
        # Note: Not actually used for skipping in the loop below because we use INSERT ON CONFLICT, 
        # but the logic below does some local deduplication.

        # Preparamos los datos para insertar
        productos_json = []
        ## Filtramos las filas con subcategoria NaN
        df_productos = df[df["subcategory"].notna()]
        # Usamos normalized_name para el nombre del producto para agrupar variantes
        for index, row in df_productos.iterrows():
            # Usamos normalized_name si existe, si no product_name
            nombre_final = row["normalized_name"] if "normalized_name" in row and pd.notna(row["normalized_name"]) else row["product_name"]
            
            # Get normalized keys
            brand_norm = clean_text(row["brand"])
            subcat_norm = clean_text(row["subcategory"])
            
            # Look up IDs
            id_marca = marca_ids_norm.get(brand_norm, 14) # 14 is 'Sin Marca'
            id_subcategoria = subcat_ids_norm.get(subcat_norm)
            
            if id_subcategoria:
                productos_json.append({
                    "nombre_producto": nombre_final,
                    "url_imagen": row["image_url"],
                    "url_thumb_imagen": row["thumbnail_image_url"],
                    "id_marca" : id_marca,
                    "id_subcategoria": id_subcategoria
                })
        
        # Lista final a insertar
        productos_insertar_json = []
        ## Revisamos si cada producto ya existe (implemetnar logica de conflicto por nombre_producto, id_marca, id_subcategoria)
        seen_products = set()
        
        for producto in productos_json:
            nombre_producto = producto["nombre_producto"]
            id_marca = producto["id_marca"]
            id_subcategoria = producto["id_subcategoria"]
            
            # Clave normalizada para verificar existencia
            nombre_prod_norm = clean_text(nombre_producto)
            
            # Check against DB existence and local seen set
            # Key for local deduplication
            key_local = (nombre_prod_norm, id_marca, id_subcategoria)
            
            if key_local not in seen_products:
                productos_insertar_json.append(producto)
                seen_products.add(key_local)
                # print(f"El producto {nombre_producto} agregado para insercion.") # Reduce spam
        
        # Insertamos los productos con execute_values (Bulk)
        total_products = len(productos_insertar_json)
        
        if total_products > 0:
            print(f"[{db_name}] Insertando/Actualizando {total_products} productos (Bulk Optimization)...")
            
            # Prepare query and data
            query = """
            INSERT INTO productos (nombre_producto, url_imagen, url_thumb_imagen, id_marca, id_subcategoria) 
            VALUES %s
            ON CONFLICT (nombre_producto, id_marca,id_subcategoria) 
            DO UPDATE SET 
                url_imagen = CASE 
                    WHEN EXCLUDED.url_imagen LIKE '%%suplescrapper-images.s3%%' THEN EXCLUDED.url_imagen 
                    WHEN productos.url_imagen LIKE '%%suplescrapper-images.s3%%' THEN productos.url_imagen 
                    ELSE EXCLUDED.url_imagen 
                END,
                url_thumb_imagen = CASE 
                    WHEN EXCLUDED.url_thumb_imagen LIKE '%%suplescrapper-images.s3%%' THEN EXCLUDED.url_thumb_imagen 
                    WHEN productos.url_thumb_imagen LIKE '%%suplescrapper-images.s3%%' THEN productos.url_thumb_imagen 
                    ELSE EXCLUDED.url_thumb_imagen 
                END
            """
            
            # Convert list of dicts to list of tuples
            values = [
                (
                    p["nombre_producto"], 
                    p["url_imagen"], 
                    p["url_thumb_imagen"], 
                    p["id_marca"], 
                    p["id_subcategoria"]
                ) 
                for p in productos_insertar_json
            ]
            
            try:
                # Use raw connection for execute_values
                # We create a NEW raw connection or retrieve from engine.
                # Accessing raw cursor from existing SA connection can be tricky across versions. 
                # Simplest for bulk ops: detach or use raw_connection from engine.
                raw_conn = engine.raw_connection()
                try:
                    with raw_conn.cursor() as cur:
                        psycopg2.extras.execute_values(cur, query, values, page_size=1000)
                    raw_conn.commit()
                    print(f"[{db_name}] Productos insertados correctamente.")
                finally:
                    raw_conn.close()
                    
            except Exception as e:
                print(f"[{db_name}] Error en bulk insert productos: {e}")
                
        else:
            print(f"[{db_name}] No hay productos nuevos para insertar.")

    # 6. Insercion ProductoTienda

    ## Tiendas y sus ids

    with engine.connect() as conn:
        tienda_result = conn.execute(sa.text("SELECT id_tienda, nombre_tienda FROM tiendas")).fetchall()
        tienda_ids_norm = {clean_text(row.nombre_tienda): row.id_tienda for row in tienda_result}

    ## Productos y sus ids
    with engine.connect() as conn:
        prod_result = conn.execute(sa.text("SELECT id_producto, nombre_producto FROM productos")).fetchall()
        producto_ids_norm = {clean_text(row.nombre_producto): row.id_producto for row in prod_result}

    ## ProductoTienda actuales
    with engine.connect() as conn:
        prod_tiend_result = conn.execute(sa.text("SELECT id_producto, id_tienda FROM producto_tienda")).fetchall()
        # producto_tienda_existentes = {(row.id_producto, row.id_tienda) for row in prod_tiend_result}

    ## Preparamos los datos para insertar
    productos_tienda_json = []
    productos_tienda_vistos = set() # Evitamos la duplibacion en la primera vez
    for index, row in df.iterrows():
        # Buscamos el ID usando el normalized_name que es como se guardó en la BD
        nombre_busqueda = row["normalized_name"] if "normalized_name" in row and pd.notna(row["normalized_name"]) else row["product_name"]
        id_producto = producto_ids_norm.get(clean_text(nombre_busqueda))
        # Las urls
        url_producto = row["link"]
        
        id_tienda = tienda_ids_norm.get(clean_text(row["site_name"]))
        
        if id_producto and id_tienda:
            if (id_producto, id_tienda) not in productos_tienda_vistos:
                productos_tienda_json.append({
                    "id_producto": id_producto,
                    "id_tienda": id_tienda,
                    "url_link": url_producto,
                    "descripcion": row["description"]
                })
                productos_tienda_vistos.add((id_producto, id_tienda))
                # print(f"Link {id_producto}-{id_tienda} agregado.")

    print(f"[{db_name}] Total combinaciones producto-tienda a insertar: {len(productos_tienda_json)}")
    # print(f"Las combinaciones son : {productos_tienda_json}")


    if len(productos_tienda_json) > 0:
        total_items = len(productos_tienda_json)
        print(f"[{db_name}] Insertando/Actualizando {total_items} relaciones producto-tienda (Bulk Optimization)...")
        
        query = """
            INSERT INTO producto_tienda (id_producto, id_tienda, url_link, descripcion) 
            VALUES %s
            ON CONFLICT (id_producto, id_tienda) 
            DO UPDATE SET url_link = EXCLUDED.url_link, descripcion = EXCLUDED.descripcion
        """
        
        values = [
            (
                pt["id_producto"],
                pt["id_tienda"],
                pt["url_link"],
                pt["descripcion"]
            )
            for pt in productos_tienda_json
        ]

        try:
            raw_conn = engine.raw_connection()
            try:
                with raw_conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, query, values, page_size=1000)
                raw_conn.commit()
                print(f"[{db_name}] Producto-Tienda insertados correctamente.")
            finally:
                raw_conn.close()
        except Exception as e:
            print(f"[{db_name}] Error en bulk insert producto_tienda: {e}")



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
        id_producto = producto_ids_norm.get(clean_text(nombre_busqueda))
        
        id_tienda = tienda_ids_norm.get(clean_text(row["site_name"]))
        
        if id_producto and id_tienda:
            id_producto_tienda = producto_tienda_ids.get((id_producto, id_tienda))
            if id_producto_tienda:
                # Validar fecha
                fecha = row["date"]
                if pd.isna(fecha):
                    # Si es NaT, usamos la fecha y hora actual
                    fecha_final = datetime.now()
                else:
                    fecha_dt = pd.to_datetime(fecha, errors='coerce')
                    if pd.isna(fecha_dt):
                        # Fallback si conversión falla
                        fecha_final = datetime.now()
                    else:
                        # Usamos el timestamp completo, no solo .date()
                        fecha_final = fecha_dt

                historial_precios_json.append({
                    "id_producto_tienda": id_producto_tienda,
                    "precio": row["price"],
                    "fecha_precio": fecha_final
                })

    # Insertamos los datos
    if len(historial_precios_json) > 0:
        with engine.connect() as conn:
            # 1. Borrar datos de HOY para evitar duplicados/sucios antes de insertar
            print(f"[{db_name}] Eliminando registros de historia_precios del día de hoy antes de insertar...")
            conn.execute(sa.text("DELETE FROM historia_precios WHERE fecha_precio::date = CURRENT_DATE"))
            conn.commit()


            # Usamos ON CONFLICT gracias al constraint uq_precio_fecha (id_producto_tienda, fecha_precio)
            query = """
                INSERT INTO historia_precios (id_producto_tienda, precio, fecha_precio) 
                VALUES(:id_producto_tienda, :precio, :fecha_precio)
            """
            
            # Batching for price history
            total_prices = len(historial_precios_json)
            print(f"[{db_name}] Historial de precios a insertar: {total_prices} (Bulk Optimization)")
            
            query = """
                INSERT INTO historia_precios (id_producto_tienda, precio, fecha_precio) 
                VALUES %s
            """
            
            values = [
                (
                    hp["id_producto_tienda"],
                    hp["precio"],
                    hp["fecha_precio"]
                )
                for hp in historial_precios_json
            ]
            
            try:
                raw_conn = engine.raw_connection()
                try:
                    with raw_conn.cursor() as cur:
                         # Use ON CONFLICT DO NOTHING if duplicates in batch? 
                         # Actually usually duplicate check handles it, but adding it makes it robust.
                         # Original code relied on DELETE previously.
                         # execute_values expands %s.
                         psycopg2.extras.execute_values(cur, query, values, page_size=1000)
                    raw_conn.commit()
                    print(f"[{db_name}] Historial de precios insertado correctamente.")
                finally:
                    raw_conn.close()
            except Exception as e:
                print(f"[{db_name}] Error en bulk insert historia_precios: {e}")
                
    else:
         print(f"[{db_name}] No hay historial de precios para insertar.")

    print(f"[{db_name}] --- Inserción finalizada ---")


def main():
    # 1. Cargar variables de entorno
    load_dotenv() 

    # Directorios de datos
    INPUT_DIR = "processed_data/fuzzy_matched"
    BACKUP_DIR = "processed_data/inserted_data"

    # Buscar el archivo más reciente de normalized_products
    csv_files = glob.glob(os.path.join(INPUT_DIR, "normalized_products_*.csv"))

    if not csv_files:
        print(f"No se encontraron archivos normalized_products_*.csv en {INPUT_DIR}")
        exit()

    # Ordenar por fecha de modificación para tomar el más reciente
    latest_file = max(csv_files, key=os.path.getmtime)
    print(f"Procesando archivo: {latest_file}")
    
    # Cargamos el dataframe UNA vez
    df = pd.read_csv(latest_file)

    # 2. Obtener lista de bases de datos
    targets = get_targets()
    print(f"Destinos de base de datos encontrados: {[t['name'] for t in targets]}")
    
    if not targets:
        print("Error: No se encontraron configuraciones de base de datos.")
        sys.exit(1)

    # 3. Iterar sobre cada destino e insertar
    success_count = 0
    for target in targets:
        db_name = target["name"]
        db_url = target["url"]
        
        try:
            print(f"\nConectando a {db_name}...")
            engine = sa.create_engine(db_url)
            
            # Probar conexión
            with engine.connect() as conn:
                pass
                
            # Ejecutar lógica de inserción
            insert_data_into_db(engine, df, db_name)
            success_count += 1
            
        except Exception as e:
            print(f"ERROR FATAL en inserción para {db_name}: {e}")
            # No detenemos el loop, intentamos con la siguiente BD si hay

    # 4. Mover archivo a backup (Solo si al menos una inserción fue exitosa para no perder datos)
    if success_count > 0:
        print("\n--- MOVER ARCHIVO PROCESADO AL RESPALDO ---")
        try:
            if not os.path.exists(BACKUP_DIR):
                os.makedirs(BACKUP_DIR)
                print(f"Directorio creado: {BACKUP_DIR}")

            destination = os.path.join(BACKUP_DIR, os.path.basename(latest_file))
            # Usar copy + remove o move
            shutil.move(latest_file, destination)
            print(f"Archivo movido exitosamente a: {destination}")

        except Exception as e:
            print(f"Error al mover el archivo de respaldo: {e}")
    else:
        print("\n[ALERTA] No se realizó ninguna inserción exitosa. El archivo NO se moverá a respaldo.")

if __name__ == "__main__":
    main()