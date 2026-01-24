import os
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv

import re

def clean_text(text_val):
    """Normalize text for grouping (remove emojis, strip, lower)."""
    if not isinstance(text_val, str):
        return ""
    
    # 1. Remove non-standard characters (keep alphanumeric, space, hyphens, dots, apostrophes, &)
    # This effectively strips emojis like ⭐
    normalized = re.sub(r'[^\w\s\-\.\'\&]', '', text_val)
    
    # 2. Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    
    return normalized.strip().lower()

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
            print("Fetching brands...")
            raw_data = conn.execute(text("SELECT id_marca, nombre_marca FROM marcas")).fetchall()
            
            groups = {}
            for id_val, name in raw_data:
                norm = clean_text(name)
                if not norm: continue
                if norm not in groups: groups[norm] = []
                groups[norm].append({'id': id_val, 'name': name})
            
            print(f"Found {len(raw_data)} brands, grouped into {len(groups)} unique normalized names.")
            
            fusion_count = 0
            for norm_name, items in groups.items():
                if len(items) > 1:
                    fusion_count += 1
                    
                    # Heuristic for Master
                    def score_candidate(item):
                        name = item['name']
                        score = 0
                        if name != name.lower() and name != name.upper(): score += 2 
                        if name == name.title(): score += 1
                        return (-score, item['id']) 

                    sorted_items = sorted(items, key=score_candidate)
                    master = sorted_items[0]
                    master_id = master['id']
                    
                    duplicates = sorted_items[1:]
                    
                    try:
                        print(f"Merging '{norm_name}': Master='{master['name']}'({master_id}) <- {[d['id'] for d in duplicates]}")
                    except UnicodeEncodeError:
                        print(f"Merging '{norm_name.encode('utf-8', 'ignore')}'...")

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
                            # (Same name, same subcategory, but in master brand)
                            master_collision = conn.execute(
                                text("""
                                    SELECT id_producto FROM productos 
                                    WHERE id_marca = :mid AND nombre_producto = :name AND id_subcategoria = :sid
                                """),
                                {'mid': master_id, 'name': prod_name, 'sid': subcat_id}
                            ).fetchone()
                            
                            if master_collision:
                                master_prod_id = master_collision.id_producto
                                # print(f"  Collision: Prod {prod_id} (Dup) matches Prod {master_prod_id} (Master). Merging...")
                                
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
                                        # Move prices to master_pt (using 'historia_precios' table)
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
                                        # Just reassign pt entry to master product
                                        conn.execute(
                                            text("UPDATE producto_tienda SET id_producto = :target WHERE id_producto_tienda = :source"),
                                            {'target': master_prod_id, 'source': pt_id}
                                        )
                                
                                # After handling children, delete the duplicate product
                                conn.execute(
                                    text("DELETE FROM productos WHERE id_producto = :pid"),
                                    {'pid': prod_id}
                                )
                                
                            else:
                                # No collision, safe to move product to master brand
                                conn.execute(
                                    text("UPDATE productos SET id_marca = :mid WHERE id_producto = :pid"),
                                    {'mid': master_id, 'pid': prod_id}
                                )
                        
                        # Finally delete the duplicate brand
                        conn.execute(
                            text("DELETE FROM marcas WHERE id_marca = :did"),
                            {'did': dup_id}
                        )

            trans.commit()
            print(f"Brand cleaning completed. Merged {fusion_count} groups.")
            
        except Exception as e:
            trans.rollback()
            print(f"Error during brand cleaning, rolled back: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    clean_brands()
