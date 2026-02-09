import os
import sys
import sqlalchemy as sa
import pandas as pd
import glob
from datetime import datetime
import json
import requests

# Add root to path to find tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets
from tools.categorizer import ProductCategorizer

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

def ai_categorization_step(df, engine):
    """
    Identifies products with unknown subcategories and classifies them in batches using Ollama.
    Updates the DataFrame in-place.
    Supports Resume from Checkpoint.
    """
    print("\n--- Fase AI: Clasificación por Lotes ---")
    
    checkpoint_file = os.path.join("local_processing_testing", "ai_checkpoint.csv")
    
    # 0. Check for checkpoint
    if os.path.exists(checkpoint_file):
        print(f"[RESUME] Encontrado checkpoint: {checkpoint_file}")
        try:
            # Load checkpoint into a new DF to merge/update
            df_checkpoint = pd.read_csv(checkpoint_file)
            
            # Update the main DF with values from checkpoint where available
            # Assuming 'link' + 'date' or index match? 
            # Ideally we just replace the DF with the checkpoint one if it contains everything.
            # Since process_local loads RAW csvs every time, the indices might shift if file order changes.
            # Safer to rely on link+product_name matching or just trust the rows order if consistent.
            
            # For simplicity in this local test, we assume the input files haven't changed, so row order is same.
            if len(df_checkpoint) == len(df):
                print("[RESUME] Cargando datos clasificados previamente...")
                # We specifically want the 'subcategory' column from checkpoint
                df['subcategory'] = df_checkpoint['subcategory']
                # And 'category' if updated
                if 'category' in df_checkpoint.columns:
                    df['category'] = df_checkpoint['category']
            else:
                print("[RESUME] Checkpoint size mismatch. Ignorando checkpoint (iniciando de cero).")
        except Exception as e:
            print(f"[RESUME] Error cargando checkpoint: {e}")

    # Initialize categorizer WITH AI enabled
    categorizer = ProductCategorizer(db_connection=engine.connect(), enable_ai=True)
    
    # 1. Identify rows that need classification
    # We check if the 'subcategory' in DF exists in the DB map
    # (The DB map keys are normalized lower case)
    
    unknown_indices = []
    items_to_classify = [] # List of {'index': i, 'product': p, 'context': c}
    
    for idx, row in df.iterrows():
        sub_raw = str(row['subcategory'])
        sub_norm = clean_text(sub_raw)
        
        # Check if already known
        if sub_norm not in categorizer.subcategories_map:
            # Check if it is "N/D" or empty, maybe we want to classify those too?
            # Yes, let's try to classify everything unknown.
            
            # Prepare for batch
            unknown_indices.append(idx)
            items_to_classify.append({
                'original_index': idx,
                'product': row['product_name'],
                'context': sub_raw
            })
            
    total_unknown = len(items_to_classify)
    if total_unknown == 0:
        print("Todos los productos tienen subcategorías conocidas. Saltando AI.")
        return df

    print(f"Productos pendientes de clasificar por IA: {total_unknown}")
    
    # 2. Process in Batches
    BATCH_SIZE = 50 # Adjust based on VRAM/Context limits. 50 is safe for 14B model.
    
    for i in range(0, total_unknown, BATCH_SIZE):
        batch_items = items_to_classify[i:i + BATCH_SIZE]
        print(f"Procesando lote {i+1}-{min(i+BATCH_SIZE, total_unknown)}...")
        
        # Call Categorizer Batch
        # We need to adapt the structure slightly for the tool
        tool_input = [{'product': item['product'], 'context': item['context']} for item in batch_items]
        
        results = categorizer.classify_batch(tool_input)
        
        # Update DataFrame
        for j, res in enumerate(results):
            original_idx = batch_items[j]['original_index']
            
            if res:
                # res is a dict like {'nombre_subcategoria': 'Whey Protein', ...}
                new_sub = res['nombre_subcategoria']
                
                df.at[original_idx, 'subcategory'] = new_sub
                # df.at[original_idx, 'category'] = new_cat 
                # print(f"  Fixed: {batch_items[j]['product']} -> {new_sub}")
            else:
                # Failed or no confident match.
                pass
        
        # Incremental Save (Checkpoint)
        try:
            df.to_csv(checkpoint_file, index=False)
            # print(f"  [Checkpoint] Guardado progreso en {checkpoint_file}")
        except Exception as e:
            print(f"  [Checkpoint] Error guardando backup: {e}")

    print("--- Fin Fase AI ---\n")
    return df

def insert_data(engine, df):
    print(f"--- Insertando {len(df)} registros en BD Local ---")
    
    with engine.connect() as conn:
        # 1. Categorias
        existing_cats = conn.execute(sa.text("SELECT nombre_categoria FROM categorias")).fetchall()
        existing_cats_set = {clean_text(row.nombre_categoria) for row in existing_cats}
        
        cats_to_insert = df['category'].dropna().unique()
        new_cats = [{"nombre_categoria": c} for c in cats_to_insert if clean_text(c) not in existing_cats_set]
        
        if new_cats:
            conn.execute(sa.text("INSERT INTO categorias (nombre_categoria) VALUES (:nombre_categoria) ON CONFLICT DO NOTHING"), new_cats)
            conn.commit()
            print(f"Categorías nuevas insertadas: {len(new_cats)}")
            
        # Refresh map
        cat_map = {clean_text(row.nombre_categoria): row.id_categoria for row in conn.execute(sa.text("SELECT * FROM categorias")).fetchall()}

        # 2. Subcategorias
        # Tuple (sub, cat)
        existing_subs = conn.execute(sa.text("SELECT nombre_subcategoria, id_categoria FROM subcategorias")).fetchall()
        existing_subs_set = {(clean_text(row.nombre_subcategoria), row.id_categoria) for row in existing_subs}
        
        subs_to_insert = []
        unique_subs = df[['subcategory', 'category']].dropna().drop_duplicates()
        
        for _, row in unique_subs.iterrows():
            s_norm = clean_text(row['subcategory'])
            c_norm = clean_text(row['category'])
            c_id = cat_map.get(c_norm)
            
            if c_id and (s_norm, c_id) not in existing_subs_set:
                subs_to_insert.append({"nombre_subcategoria": row['subcategory'], "id_categoria": c_id})
                existing_subs_set.add((s_norm, c_id)) # Avoid dupes in batch
        
        if subs_to_insert:
            conn.execute(sa.text("INSERT INTO subcategorias (nombre_subcategoria, id_categoria) VALUES (:nombre_subcategoria, :id_categoria) ON CONFLICT DO NOTHING"), subs_to_insert)
            conn.commit()
            print(f"Subcategorías nuevas insertadas: {len(subs_to_insert)}")

        # Refresh subcat map
        subcat_map = {clean_text(row.nombre_subcategoria): row.id_subcategoria for row in conn.execute(sa.text("SELECT * FROM subcategorias")).fetchall()}

        # 3. Marcas
        existing_brands = conn.execute(sa.text("SELECT nombre_marca FROM marcas")).fetchall()
        existing_brands_set = {clean_text(row.nombre_marca) for row in existing_brands}
        
        brands_to_insert = df['brand'].dropna().unique()
        new_brands = [{"nombre_marca": b} for b in brands_to_insert if clean_text(b) not in existing_brands_set]
        
        if new_brands:
            conn.execute(sa.text("INSERT INTO marcas (nombre_marca) VALUES (:nombre_marca) ON CONFLICT DO NOTHING"), new_brands)
            conn.commit()
            print(f"Marcas nuevas insertadas: {len(new_brands)}")
            
        brand_map = {clean_text(row.nombre_marca): row.id_marca for row in conn.execute(sa.text("SELECT * FROM marcas")).fetchall()}

        # 4. Tiendas
        existing_shops = conn.execute(sa.text("SELECT nombre_tienda FROM tiendas")).fetchall()
        existing_shops_set = {clean_text(row.nombre_tienda) for row in existing_shops}
        
        shops_to_insert = df['site_name'].dropna().unique()
        new_shops = [{"nombre_tienda": s} for s in shops_to_insert if clean_text(s) not in existing_shops_set]
        
        if new_shops:
            conn.execute(sa.text("INSERT INTO tiendas (nombre_tienda) VALUES (:nombre_tienda) ON CONFLICT DO NOTHING"), new_shops)
            conn.commit()
            print(f"Tiendas nuevas insertadas: {len(new_shops)}")
            
        shop_map = {clean_text(row.nombre_tienda): row.id_tienda for row in conn.execute(sa.text("SELECT * FROM tiendas")).fetchall()}

        # 5. Productos & Precios
        # Prepare batch
        products_batch = []
        prod_store_batch = []
        prices_batch = []
        
        # Get existing products to minimize conflict checks (optional but good)
        # For this test, we rely on ON CONFLICT
        
        print("Procesando productos...")
        for _, row in df.iterrows():
            p_name = row['product_name']
            p_norm = clean_text(p_name)
            b_norm = clean_text(row['brand'])
            s_norm = clean_text(row['subcategory'])
            
            b_id = brand_map.get(b_norm, brand_map.get('n/d', 14)) # Fallback 14
            s_id = subcat_map.get(s_norm)
            
            if not s_id:
                # Fallback: if subcategory didn't map, maybe insert into 'Otros' or skip?
                # For now skip to avoid foreign key error
                continue
                
            # Product Data
            # Note: We use product_name as is. In v1 there was normalization. 
            # Here we assume scraper title is good enough for v2 test.
            
            # Insert Product query
            # We do this one by one or small batches? 
            # Let's do a direct execute per row for simplicity in this script, or better, prepare dicts
            # But we need the product_id for the price.
            
            # STRATEGY: Insert Product -> RETURNING id.
            # Only works easily with 1-by-1 or complex CTE.
            # Let's use 1-by-1 for safety in testing script.
            
            try:
                # Upsert Product
                res_prod = conn.execute(sa.text("""
                    INSERT INTO productos (nombre_producto, url_imagen, url_thumb_imagen, id_marca, id_subcategoria)
                    VALUES (:name, :img, :thumb, :bid, :sid)
                    ON CONFLICT (nombre_producto, id_marca, id_subcategoria) 
                    DO UPDATE SET url_imagen = EXCLUDED.url_imagen
                    RETURNING id_producto
                """), {"name": p_name, "img": row.get('image_url',''), "thumb": row.get('thumbnail_image_url',''), "bid": b_id, "sid": s_id})
                
                # Fetch ID
                # If updated, it might not return ID in some PG versions/drivers depending on config, 
                # but usually RETURNING works on conflict update. 
                # If row didn't change and nothing happened, we need to select it.
                pid = res_prod.scalar()
                
                if not pid:
                    pid = conn.execute(sa.text("SELECT id_producto FROM productos WHERE nombre_producto=:name AND id_marca=:bid AND id_subcategoria=:sid"),
                                       {"name": p_name, "bid": b_id, "sid": s_id}).scalar()
                
                if pid:
                    # Producto Tienda
                    tid = shop_map.get(clean_text(row['site_name']))
                    if tid:
                        res_pt = conn.execute(sa.text("""
                            INSERT INTO producto_tienda (id_producto, id_tienda, url_link, descripcion)
                            VALUES (:pid, :tid, :link, :desc)
                            ON CONFLICT (id_producto, id_tienda) DO UPDATE SET url_link=EXCLUDED.url_link
                            RETURNING id_producto_tienda
                        """), {"pid": pid, "tid": tid, "link": row['link'], "desc": row.get('description', '')})
                        
                        ptid = res_pt.scalar()
                        if not ptid:
                             ptid = conn.execute(sa.text("SELECT id_producto_tienda FROM producto_tienda WHERE id_producto=:pid AND id_tienda=:tid"),
                                       {"pid": pid, "tid": tid}).scalar()
                        
                        # Price History
                        if ptid:
                            # Delete today's price first?
                            # conn.execute(sa.text("DELETE FROM historia_precios WHERE id_producto_tienda=:ptid AND fecha_precio::date = CURRENT_DATE"), {"ptid": ptid})
                            
                            conn.execute(sa.text("""
                                INSERT INTO historia_precios (id_producto_tienda, precio, fecha_precio)
                                VALUES (:ptid, :price, :date)
                                ON CONFLICT (id_producto_tienda, fecha_precio) DO NOTHING
                            """), {"ptid": ptid, "price": row['price'], "date": row['date'] or datetime.now()})
                            
            except Exception as e:
                print(f"Error insertando {p_name}: {e}")
        
        conn.commit()
        print("Inserción completada.")

def main():
    print("--- Iniciando Proceso V2 (Local) ---")
    
    # 1. Leer CSVs RAW
    raw_files = glob.glob("raw_data/*.csv")
    if not raw_files:
        print("No hay archivos CSV en raw_data/")
        return

    print(f"Encontrados {len(raw_files)} archivos.")
    dfs = []
    for f in raw_files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"Error leyendo {f}: {e}")
    
    if not dfs:
        return

    full_df = pd.concat(dfs, ignore_index=True)
    print(f"Total filas: {len(full_df)}")
    
    # 2. Connect DB
    engine = get_local_engine()
    
    # 3. AI Categorization (Batch)
    full_df = ai_categorization_step(full_df, engine)

    # 4. Insert
    insert_data(engine, full_df)

if __name__ == "__main__":
    main()
