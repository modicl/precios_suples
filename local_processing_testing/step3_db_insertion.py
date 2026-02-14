import os
import sys
import sqlalchemy as sa
from sqlalchemy import text
import pandas as pd
from datetime import datetime

# Add root to path to find tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def clean_display_text(text):
    if isinstance(text, str):
        return text.strip()
    return ""

def make_key(text):
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
        existing_cats_set = {make_key(row.nombre_categoria) for row in existing_cats}
        
        cat_map = {make_key(row.nombre_categoria): row.id_categoria for row in conn.execute(sa.text("SELECT * FROM categorias")).fetchall()}

        # Subcategorias
        all_subs = conn.execute(sa.text("SELECT id_subcategoria, nombre_subcategoria, id_categoria FROM subcategorias")).fetchall()
        
        subcat_map = {} # key -> id_sub
        cat_of_sub_map = {} # id_sub -> id_cat
        fallback_map = {} # id_cat -> id_sub_otros
        
        for row in all_subs:
            s_name = row.nombre_subcategoria
            s_id = row.id_subcategoria
            c_id = row.id_categoria
            
            subcat_map[make_key(s_name)] = s_id
            cat_of_sub_map[s_id] = c_id
            
            if s_name.lower().startswith("otros"):
                fallback_map[c_id] = s_id

        # Marcas
        existing_brands = conn.execute(sa.text("SELECT nombre_marca FROM marcas")).fetchall()
        existing_brands_set = {make_key(row.nombre_marca) for row in existing_brands}
        
        brands_to_insert = df['brand'].dropna().unique()
        # Use display text for insertion, check key for existence
        new_brands = [{"nombre_marca": clean_display_text(b)} for b in brands_to_insert if make_key(b) not in existing_brands_set]
        
        if new_brands:
            conn.execute(sa.text("INSERT INTO marcas (nombre_marca) VALUES (:nombre_marca) ON CONFLICT DO NOTHING"), new_brands)
            conn.commit()
            
        brand_map = {make_key(row.nombre_marca): row.id_marca for row in conn.execute(sa.text("SELECT * FROM marcas")).fetchall()}

        # Tiendas
        existing_shops = conn.execute(sa.text("SELECT nombre_tienda FROM tiendas")).fetchall()
        existing_shops_set = {make_key(row.nombre_tienda) for row in existing_shops}
        
        shops_to_insert = df['site_name'].dropna().unique()
        new_shops = [{"nombre_tienda": clean_display_text(s)} for s in shops_to_insert if make_key(s) not in existing_shops_set]
        
        if new_shops:
            conn.execute(sa.text("INSERT INTO tiendas (nombre_tienda) VALUES (:nombre_tienda) ON CONFLICT DO NOTHING"), new_shops)
            conn.commit()
            
        shop_map = {make_key(row.nombre_tienda): row.id_tienda for row in conn.execute(sa.text("SELECT * FROM tiendas")).fetchall()}

        # --- FASE 1: PRODUCTOS (Bulk Upsert) ---
        print("Preparando lote de Productos...")
        
        # Use a dict to dedup by (name_key, brand_id, subcat_id)
        # But we store the DISPLAY name in the values
        products_dedup = {}
        
        fallback_brand_id = brand_map.get('n/d', 14)

        for _, row in df.iterrows():
            # Get generic name
            p_display = row.get('normalized_name', row['product_name'])
            if pd.isna(p_display): p_display = row['product_name']
            p_display = clean_display_text(p_display)
            if not p_display: continue
            
            # Key for lookup/dedup (LOWERCASE)
            p_key = make_key(p_display)
            
            b_key = make_key(row['brand'])
            s_key = make_key(row['subcategory'])
            c_key = make_key(row['category'])
            
            b_id = brand_map.get(b_key, fallback_brand_id)
            s_id = subcat_map.get(s_key)
            c_id = cat_map.get(c_key)
            
            # Fallback logic for subcategory
            if not s_id and c_id:
                s_id = fallback_map.get(c_id)
            
            if not s_id: continue
            
            # DEDUP KEY uses ids and LOWERCASE name
            key = (p_key, b_id, s_id)
            
            if key not in products_dedup:
                products_dedup[key] = {
                    "nombre_producto": p_display, # INSERT THIS (Title Case)
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
        # Use make_key for the product name in the map key to ensure case-insensitive lookup
        prod_id_map = {(make_key(row.nombre_producto), row.id_marca, row.id_subcategoria): row.id_producto for row in all_prods}
        print(f"Mapa de productos cargado: {len(prod_id_map)} entradas.")

        # --- FASE 2: PRODUCTO_TIENDA (Bulk Upsert) ---
        print("Preparando lote de Producto-Tienda...")
        links_dedup = {}
        
        miss_pid_count = 0
        
        for _, row in df.iterrows():
            p_display = row.get('normalized_name', row['product_name'])
            if pd.isna(p_display): p_display = row['product_name']
            
            p_display = clean_display_text(p_display)
            if not p_display: continue
            
            # Key for lookup
            p_key = make_key(p_display)
            
            b_key = make_key(row['brand'])
            s_key = make_key(row['subcategory'])
            c_key = make_key(row['category'])
            t_key = make_key(row['site_name'])
            
            b_id = brand_map.get(b_key, fallback_brand_id)
            s_id = subcat_map.get(s_key)
            c_id = cat_map.get(c_key)
            t_id = shop_map.get(t_key)
            
            # Fallback logic for subcategory
            if not s_id and c_id:
                s_id = fallback_map.get(c_id)
            
            if not s_id or not t_id: continue
            
            pid = prod_id_map.get((p_key, b_id, s_id))
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
            p_display = row.get('normalized_name', row['product_name'])
            if pd.isna(p_display): p_display = row['product_name']
            
            p_display = clean_display_text(p_display)
            if not p_display: continue
            
            p_key = make_key(p_display)
            
            b_key = make_key(row['brand'])
            s_key = make_key(row['subcategory'])
            c_key = make_key(row['category'])
            t_key = make_key(row['site_name'])
            
            b_id = brand_map.get(b_key, fallback_brand_id)
            s_id = subcat_map.get(s_key)
            t_id = shop_map.get(t_key)
            c_id = cat_map.get(c_key)
            
            # Fallback logic for subcategory
            if not s_id and c_id:
                s_id = fallback_map.get(c_id)
            
            if not s_id or not t_id: continue
            
            pid = prod_id_map.get((p_key, b_id, s_id))
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
    print("--- PASO 3: Inserción en Base de Datos (BATCH) ---")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Use 'latest' pointer from Step 2 folder
    input_csv = os.path.join(current_dir, "data", "2_normalized", "latest_normalized.csv")
    
    if not os.path.exists(input_csv):
        print(f"Error: No se encontró {input_csv}. Ejecuta el Paso 2 primero.")
        return
        
    df = pd.read_csv(input_csv)
    engine = get_local_engine()
    insert_data_bulk(engine, df)

if __name__ == "__main__":
    main()
