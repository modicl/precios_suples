import os
import csv
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv
import re
from rapidfuzz import process, fuzz, utils
from collections import defaultdict

def load_brand_dictionary(filepath):
    """Load canonical brand names from CSV."""
    brands = []
    if not os.path.exists(filepath):
        print(f"Warning: Dictionary file not found at {filepath}")
        return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('nombre_marca'):
                    brands.append(row['nombre_marca'].strip())
    except Exception as e:
        print(f"Error loading dictionary: {e}")
        return []
    return brands

def clean_brands():
    load_dotenv()
    
    # Construct Database URL
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
         DATABASE_URL = os.getenv("DATABASE_URL")
         if not DATABASE_URL:
             print("Error: Database credentials not found.")
             return
    else:
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    print(f"Connecting to database...")
    engine = sa.create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            print("Starting smart brand cleaning...")
            
            # 1. Load Dictionary
            dic_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'marcas_dictionary.csv')
            canonical_brands = load_brand_dictionary(dic_path)
            print(f"Loaded {len(canonical_brands)} brands from dictionary.")

            # 2. Fetch all current brands
            brands_snapshot = conn.execute(text("SELECT id_marca, nombre_marca FROM marcas")).fetchall()
            print(f"Loaded {len(brands_snapshot)} brands from database.")

            # 3. Cluster Brands
            # clusters maps { 'Target Canonical Name': [ {id, current_name}, ... ] }
            clusters = defaultdict(list)

            for id_val, name in brands_snapshot:
                if not name:
                    continue
                
                target_name = None
                
                # A. Try to match with dictionary
                if canonical_brands:
                    match_result = process.extractOne(
                        name, 
                        canonical_brands, 
                        scorer=fuzz.WRatio, 
                        processor=utils.default_process
                    )
                    if match_result:
                        best_match, score, _ = match_result
                        if score > 85:
                            target_name = best_match
                
                # B. If no dictionary match (or dict empty), use own name as key (stripped)
                if not target_name:
                    target_name = name.strip()
                
                # Add to cluster
                clusters[target_name].append({'id': id_val, 'name': name})

            print(f"Grouped into {len(clusters)} unique target names.")

            # 4. Execute Merge and Rename
            fusion_count = 0
            rename_count = 0
            
            for target_name, items in clusters.items():
                
                # --- Step A: Identify Master ---
                # Logic: Prefer an item that ALREADY has the target name. 
                # If none, prefer mixed case (Title Case). 
                # Tie-breaker: Lowest ID.
                def score_candidate(item):
                    name = item['name']
                    score = 0
                    if name == target_name: score += 10 # Best: already correct
                    if name != name.lower() and name != name.upper(): score += 2 
                    if name == name.title(): score += 1
                    return (-score, item['id']) 

                sorted_items = sorted(items, key=score_candidate)
                master = sorted_items[0]
                master_id = master['id']
                
                duplicates = sorted_items[1:] # All others are slaves to be merged
                
                if duplicates:
                    fusion_count += 1
                    try:
                        print(f"Merging into '{target_name}' (Master ID {master_id}): Merging {[d['id'] for d in duplicates]}")
                    except UnicodeEncodeError:
                         pass

                    # --- Step B: Merge Logic ---
                    for dup in duplicates:
                        dup_id = dup['id']
                        
                        # Process products of duplicate brand
                        dup_products = conn.execute(
                            text("SELECT id_producto, nombre_producto, id_subcategoria FROM productos WHERE id_marca = :did"),
                            {'did': dup_id}
                        ).fetchall()
                        
                        for prod in dup_products:
                            prod_id = prod.id_producto
                            prod_name = prod.nombre_producto
                            subcat_id = prod.id_subcategoria
                            
                            # Check for collision in master brand
                            master_collision = conn.execute(
                                text("""
                                    SELECT id_producto FROM productos 
                                    WHERE id_marca = :mid AND nombre_producto = :name AND id_subcategoria = :sid
                                """),
                                {'mid': master_id, 'name': prod_name, 'sid': subcat_id}
                            ).fetchone()
                            
                            if master_collision:
                                master_prod_id = master_collision.id_producto
                                
                                # Move producto_tienda entries
                                pt_entries = conn.execute(
                                    text("SELECT id_producto_tienda, id_tienda FROM producto_tienda WHERE id_producto = :pid"),
                                    {'pid': prod_id}
                                ).fetchall()
                                
                                for pt in pt_entries:
                                    pt_id = pt.id_producto_tienda
                                    store_id = pt.id_tienda
                                    
                                    # Check if master product already has this store
                                    master_pt = conn.execute(
                                        text("SELECT id_producto_tienda FROM producto_tienda WHERE id_producto = :pid AND id_tienda = :sid"),
                                        {'pid': master_prod_id, 'sid': store_id}
                                    ).fetchone()
                                    
                                    if master_pt:
                                        master_pt_id = master_pt.id_producto_tienda
                                        # Move prices to master_pt
                                        conn.execute(
                                            text("UPDATE historia_precios SET id_producto_tienda = :target WHERE id_producto_tienda = :source"),
                                            {'target': master_pt_id, 'source': pt_id}
                                        )
                                        # Delete duplicate pt entry
                                        conn.execute(
                                            text("DELETE FROM producto_tienda WHERE id_producto_tienda = :pid"),
                                            {'pid': pt_id}
                                        )
                                    else:
                                        # Reassign pt entry to master product
                                        conn.execute(
                                            text("UPDATE producto_tienda SET id_producto = :target WHERE id_producto_tienda = :source"),
                                            {'target': master_prod_id, 'source': pt_id}
                                        )
                                
                                # Delete the duplicate product
                                conn.execute(
                                    text("DELETE FROM productos WHERE id_producto = :pid"),
                                    {'pid': prod_id}
                                )
                                
                            else:
                                # Safe to move product to master brand
                                conn.execute(
                                    text("UPDATE productos SET id_marca = :mid WHERE id_producto = :pid"),
                                    {'mid': master_id, 'pid': prod_id}
                                )
                        
                        # Delete the duplicate brand
                        conn.execute(
                            text("DELETE FROM marcas WHERE id_marca = :did"),
                            {'did': dup_id}
                        )
                
                # --- Step C: Rename Master (if needed) ---
                if master['name'] != target_name:
                    # Check safely if target_name is available (it should be, because we clustered by it)
                    # BUT: Another cluster might have claimed it? No, keys are unique.
                    # The only risk is if we are renaming to 'Space Protein' and 'Space Protein' wasn't in this cluster.
                    # But if 'Space Protein' existed in DB, it would have been put in THIS cluster.
                    # So it's safe.
                    # print(f"Renaming Master ID {master_id} from '{master['name']}' to '{target_name}'")
                    conn.execute(
                        text("UPDATE marcas SET nombre_marca = :new_name WHERE id_marca = :id"),
                        {'new_name': target_name, 'id': master_id}
                    )
                    rename_count += 1

            trans.commit()
            print(f"Brand cleaning completed. Merged {fusion_count} groups. Renamed {rename_count} masters.")
            
        except Exception as e:
            trans.rollback()
            print(f"Error during brand cleaning, rolled back: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    clean_brands()
