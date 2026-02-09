import os
import sys
import sqlalchemy as sa
from sqlalchemy import text
import pandas as pd
from datetime import datetime

# Add root to path to find tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def clean_text(text):
    if isinstance(text, str):
        return text.lower().strip()
    return ""

def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local' en db_multiconnect.")
        sys.exit(1)
    return sa.create_engine(local_target['url'])

def insert_data_bulk(engine, df):
    print(f"--- Insertando {len(df)} registros (MODO BATCH OPTIMIZADO) ---")
    
    with engine.connect() as conn:
        # --- 0. PREPARAR ID MAPS ---
        print("Cargando mapas de referencia (Categorias, Marcas, Tiendas)...")
        
        # Categorias
        existing_cats = conn.execute(sa.text("SELECT nombre_categoria FROM categorias")).fetchall()
        existing_cats_set = {clean_text(row.nombre_categoria) for row in existing_cats}
        
        # MODIFICADO: Bloquear creación de nuevas categorías
        # cats_to_insert = df['category'].dropna().unique()
        # new_cats = [{"nombre_categoria": c} for c in cats_to_insert if clean_text(c) not in existing_cats_set]
        # if new_cats:
        #     conn.execute(sa.text("INSERT INTO categorias (nombre_categoria) VALUES (:nombre_categoria) ON CONFLICT DO NOTHING"), new_cats)
        #     conn.commit()
        
        cat_map = {clean_text(row.nombre_categoria): row.id_categoria for row in conn.execute(sa.text("SELECT * FROM categorias")).fetchall()}

        # Subcategorias
        # MODIFICADO: Usar solo las existentes. Si no existe -> Fallback a 'Otros'
        existing_subs = conn.execute(sa.text("SELECT nombre_subcategoria, id_categoria FROM subcategorias")).fetchall()
        
        # Mapa estricto: clean_name -> id_subcategoria
        # Also map: clean_name -> id_categoria (to allow fallback lookup)
        # We need a way to look up the subcategory object to check its parent category, OR load a map of Fallbacks.
        
        all_subs = conn.execute(sa.text("SELECT id_subcategoria, nombre_subcategoria, id_categoria FROM subcategorias")).fetchall()
        
        subcat_map = {} # clean_sub_name -> id_sub
        cat_of_sub_map = {} # id_sub -> id_cat
        
        # Build map of category_id -> id_sub_otros
        fallback_map = {} # id_cat -> id_sub_otros
        
        for row in all_subs:
            s_name = row.nombre_subcategoria
            s_id = row.id_subcategoria
            c_id = row.id_categoria
            
            subcat_map[clean_text(s_name)] = s_id
            cat_of_sub_map[s_id] = c_id
            
            # Identify if this is the "Otros" for this category
            # We look for "Otros {CatName}" or just "Otros" belonging to this cat
            if s_name.lower().startswith("otros"):
                # Ideally check if it matches "Otros {CatName}" logic or just generic "Otros"
                # Simpler: last "Otros" seen for this cat wins as fallback
                fallback_map[c_id] = s_id

        # Bloque de inserción de subcategorías eliminado para proteger la integridad de la BD
        # subs_to_insert = [] ... (Eliminado)
        
        # Opcional: Buscar ID de 'Otros' o fallback si es necesario
        # fallback_sub_id = subcat_map.get('otros') 

        # Bloque de inserción de subcategorías eliminado para proteger la integridad de la BD
        # subs_to_insert = [] ... (Eliminado)


        # Marcas
        existing_brands = conn.execute(sa.text("SELECT nombre_marca FROM marcas")).fetchall()
        existing_brands_set = {clean_text(row.nombre_marca) for row in existing_brands}
        
        brands_to_insert = df['brand'].dropna().unique()
        new_brands = [{"nombre_marca": b} for b in brands_to_insert if clean_text(b) not in existing_brands_set]
        if new_brands:
            conn.execute(sa.text("INSERT INTO marcas (nombre_marca) VALUES (:nombre_marca) ON CONFLICT DO NOTHING"), new_brands)
            conn.commit()
        brand_map = {clean_text(row.nombre_marca): row.id_marca for row in conn.execute(sa.text("SELECT * FROM marcas")).fetchall()}

        # Tiendas
        existing_shops = conn.execute(sa.text("SELECT nombre_tienda FROM tiendas")).fetchall()
        existing_shops_set = {clean_text(row.nombre_tienda) for row in existing_shops}
        
        shops_to_insert = df['site_name'].dropna().unique()
        new_shops = [{"nombre_tienda": s} for s in shops_to_insert if clean_text(s) not in existing_shops_set]
        if new_shops:
            conn.execute(sa.text("INSERT INTO tiendas (nombre_tienda) VALUES (:nombre_tienda) ON CONFLICT DO NOTHING"), new_shops)
            conn.commit()
        shop_map = {clean_text(row.nombre_tienda): row.id_tienda for row in conn.execute(sa.text("SELECT * FROM tiendas")).fetchall()}

        # --- FASE 1: PRODUCTOS (Bulk Upsert) ---
        print("Preparando lote de Productos...")
        products_to_process = []
        # Use a dict to dedup by (name, brand, subcat) within this batch
        products_dedup = {}
        
        fallback_brand_id = brand_map.get('n/d', 14)

        for _, row in df.iterrows():
            # Use 'normalized_name' as the official product name (now derived from AI Clean Name)
            p_name = row.get('normalized_name', row['product_name'])
            if pd.isna(p_name): p_name = row['product_name']
            
            # Clean it again just in case
            p_name = clean_text(p_name)
            if not p_name: continue
            
            b_norm = clean_text(row['brand'])
            s_norm = clean_text(row['subcategory'])
            c_norm = clean_text(row['category'])
            
            b_id = brand_map.get(b_norm, fallback_brand_id)
            s_id = subcat_map.get(s_norm)
            c_id = cat_map.get(c_norm)
            
            # Fallback logic for subcategory
            if not s_id and c_id:
                # Use the 'Otros' subcategory for this specific category
                s_id = fallback_map.get(c_id)
            
            if not s_id: continue
            
            key = (p_name, b_id, s_id)
            if key not in products_dedup:
                products_dedup[key] = {
                    "nombre_producto": p_name,
                    "url_imagen": row.get('image_url', ''),
                    "url_thumb_imagen": row.get('thumbnail_image_url', ''),
                    "id_marca": b_id,
                    "id_subcategoria": s_id
                }
        
        batch_products = list(products_dedup.values())
        if batch_products:
            print(f"Insertando/Actualizando {len(batch_products)} productos únicos...")
            # Upsert
            try:
                conn.execute(sa.text("""
                    INSERT INTO productos (nombre_producto, url_imagen, url_thumb_imagen, id_marca, id_subcategoria)
                    VALUES (:nombre_producto, :url_imagen, :url_thumb_imagen, :id_marca, :id_subcategoria)
                    ON CONFLICT (nombre_producto, id_marca, id_subcategoria) 
                    DO UPDATE SET url_imagen = EXCLUDED.url_imagen
                """), batch_products)
                conn.commit()
            except Exception as e:
                print(f"[ERROR CRITICO] Fallo en bulk insert productos: {e}")
        
        # Recuperar IDs de productos
        # Select all involved products to build map
        print("Recuperando IDs de productos...")
        all_prods = conn.execute(sa.text("SELECT id_producto, nombre_producto, id_marca, id_subcategoria FROM productos")).fetchall()
        prod_id_map = {(row.nombre_producto, row.id_marca, row.id_subcategoria): row.id_producto for row in all_prods}
        print(f"Mapa de productos cargado: {len(prod_id_map)} entradas.")

        # --- FASE 2: PRODUCTO_TIENDA (Bulk Upsert) ---
        print("Preparando lote de Producto-Tienda...")
        links_dedup = {}
        
        miss_pid_count = 0
        
        for _, row in df.iterrows():
            p_name = row.get('normalized_name', row['product_name'])
            if pd.isna(p_name): p_name = row['product_name']
            
            # Clean it again just in case
            p_name = clean_text(p_name)
            if not p_name: continue
            
            b_norm = clean_text(row['brand'])
            s_norm = clean_text(row['subcategory'])
            c_norm = clean_text(row['category'])
            
            b_id = brand_map.get(b_norm, fallback_brand_id)
            s_id = subcat_map.get(s_norm)
            c_id = cat_map.get(c_norm)
            t_id = shop_map.get(clean_text(row['site_name']))
            
            # Fallback logic for subcategory
            if not s_id and c_id:
                s_id = fallback_map.get(c_id)
            
            if not s_id or not t_id: continue
            
            pid = prod_id_map.get((p_name, b_id, s_id))
            if not pid: 
                miss_pid_count += 1
                # Optional: print first few misses
                if miss_pid_count <= 3:
                    print(f"  [DEBUG MISS] PID no encontrado para: '{p_name}', BrandID {b_id}, SubID {s_id}")
                continue
            
            key = (pid, t_id)
            links_dedup[key] = {
                "id_producto": pid,
                "id_tienda": t_id,
                "url_link": row['link'],
                "descripcion": row.get('description', '')
            }
            
        if miss_pid_count > 0:
            print(f"[WARNING] {miss_pid_count} filas saltadas porque no se encontró el ID del producto (Fallo Fase 1 -> Fase 2).")
            
        batch_links = list(links_dedup.values())
        if batch_links:
            print(f"Insertando/Actualizando {len(batch_links)} enlaces...")
            conn.execute(sa.text("""
                INSERT INTO producto_tienda (id_producto, id_tienda, url_link, descripcion)
                VALUES (:id_producto, :id_tienda, :url_link, :descripcion)
                ON CONFLICT (id_producto, id_tienda) DO UPDATE SET url_link = EXCLUDED.url_link
            """), batch_links)
            conn.commit()
            
        # Recuperar IDs de enlaces
        print("Recuperando IDs de enlaces...")
        all_links = conn.execute(sa.text("SELECT id_producto_tienda, id_producto, id_tienda FROM producto_tienda")).fetchall()
        link_id_map = {(row.id_producto, row.id_tienda): row.id_producto_tienda for row in all_links}
        print(f"Mapa de enlaces cargado: {len(link_id_map)} entradas.")

        # --- FASE 3: PRECIOS (Bulk Insert) ---
        print("Preparando lote de Precios...")
        prices_batch = []
        
        miss_ptid_count = 0
        
        for _, row in df.iterrows():
            p_name = row.get('normalized_name', row['product_name'])
            if pd.isna(p_name): p_name = row['product_name']
            
            # Clean it again just in case
            p_name = clean_text(p_name)
            if not p_name: continue
            
            b_norm = clean_text(row['brand'])
            s_norm = clean_text(row['subcategory'])
            c_norm = clean_text(row['category'])
            
            b_id = brand_map.get(b_norm, fallback_brand_id)
            s_id = subcat_map.get(s_norm)
            t_id = shop_map.get(clean_text(row['site_name']))
            c_id = cat_map.get(c_norm)
            
            # Fallback logic for subcategory
            if not s_id and c_id:
                s_id = fallback_map.get(c_id)
            
            if not s_id or not t_id: continue
            
            pid = prod_id_map.get((p_name, b_id, s_id))
            if not pid: continue
            
            ptid = link_id_map.get((pid, t_id))
            if not ptid: 
                miss_ptid_count += 1
                if miss_ptid_count <= 3:
                    print(f"  [DEBUG MISS PTID] PID {pid}, TID {t_id} no encontrado en mapa de enlaces.")
                continue
            
            prices_batch.append({
                "id_producto_tienda": ptid,
                "precio": row['price'],
                "fecha_precio": row['date'] or datetime.now()
            })
            
        if miss_ptid_count > 0:
            print(f"[WARNING] {miss_ptid_count} precios saltados por falta de enlace (Fallo Fase 2 -> Fase 3).")
            
        if prices_batch:
            print(f"Insertando {len(prices_batch)} precios...")
            # Insert simple sin ON CONFLICT (según petición usuario)
            # Para evitar el error de transacción aborted si falla uno, aquí SÍ conviene batch chunks si son muchisimos,
            # pero executemany suele manejarlo bien. Si hay error, fallará todo el bloque.
            # Como pediste 'producción', lo ideal es dividir en chunks de 1000-5000
            
            chunk_size = 2000
            total_prices = len(prices_batch)
            for i in range(0, total_prices, chunk_size):
                chunk = prices_batch[i:i + chunk_size]
                try:
                    conn.execute(sa.text("""
                        INSERT INTO historia_precios (id_producto_tienda, precio, fecha_precio)
                        VALUES (:id_producto_tienda, :precio, :fecha_precio)
                    """), chunk)
                    conn.commit()
                    print(f"  Chunk precios {i}-{min(i+chunk_size, total_prices)} OK.")
                except Exception as e:
                    print(f"  Error en chunk de precios: {e}")
                    # En producción podriamos intentar fallback row-by-row para este chunk
                    
    print("Proceso Batch Finalizado.")

def main():
    print("--- PASO 4: Inserción en Base de Datos (BATCH) ---")
    # Use 'latest' pointer from Step 3 folder
    input_csv = os.path.join("local_processing_testing", "data", "3_normalized", "latest_normalized.csv")
    
    if not os.path.exists(input_csv):
        print(f"Error: No se encontró {input_csv}. Ejecuta el Paso 3 primero.")
        return
        
    df = pd.read_csv(input_csv)
    engine = get_local_engine()
    insert_data_bulk(engine, df)

if __name__ == "__main__":
    main()
