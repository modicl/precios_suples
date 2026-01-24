import os
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv

def clean_text(text_val):
    """Normalize text for grouping."""
    if not isinstance(text_val, str):
        return ""
    return text_val.strip().lower()

def merge_categories(conn, master_id, master_name, loser_id, loser_name):
    print(f"Merging '{loser_name}' ({loser_id}) -> '{master_name}' ({master_id})")
    
    # 1. Get subcategories of loser
    loser_subs = conn.execute(
        text("SELECT id_subcategoria, nombre_subcategoria FROM subcategorias WHERE id_categoria = :cid"),
        {'cid': loser_id}
    ).fetchall()
    
    for sub in loser_subs:
        sub_id = sub.id_subcategoria
        sub_name = sub.nombre_subcategoria
        
        # Check if master has this subcategory
        master_sub = conn.execute(
            text("SELECT id_subcategoria FROM subcategorias WHERE id_categoria = :cid AND nombre_subcategoria = :name"),
            {'cid': master_id, 'name': sub_name}
        ).fetchone()
        
        if master_sub:
            master_sub_id = master_sub.id_subcategoria
            # print(f"  Subcategory collision: '{sub_name}'. Merging Sub {sub_id} -> {master_sub_id}")
            
            # Move products from loser sub to master sub
            conn.execute(
                text("UPDATE productos SET id_subcategoria = :target WHERE id_subcategoria = :source"),
                {'target': master_sub_id, 'source': sub_id}
            )
            
            # Delete loser sub
            conn.execute(
                text("DELETE FROM subcategorias WHERE id_subcategoria = :sid"),
                {'sid': sub_id}
            )
        else:
            # No collision, just move the subcategory to master category
            conn.execute(
                text("UPDATE subcategorias SET id_categoria = :target WHERE id_subcategoria = :sid"),
                {'target': master_id, 'sid': sub_id}
            )

    # 2. Delete loser category
    conn.execute(
        text("DELETE FROM categorias WHERE id_categoria = :cid"),
        {'cid': loser_id}
    )

def clean_categories():
    load_dotenv()
    
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
            # PART 1: Manual Semantic Merges
            # Mapping: Loser Name (or ID part) -> Master Name (or ID)
            # We use exact names or IDs to find them.
            
            manual_merges = [
                # (Master Name, Loser Name)
                ("Aminoacidos y BCAA", "Aminoacidos"),
                ("Snacks y Comida", "Snacks"),
                ("Ganadores de Peso", "Ganador de Masa"),
                ("Creatinas", "Creatina")
            ]
            
            print("\n--- Performing Manual Merges ---")
            for master_name, loser_name in manual_merges:
                # Find IDs
                res_m = conn.execute(text("SELECT id_categoria FROM categorias WHERE nombre_categoria ILIKE :name"), {'name': master_name}).fetchone()
                res_l = conn.execute(text("SELECT id_categoria FROM categorias WHERE nombre_categoria ILIKE :name"), {'name': loser_name}).fetchone()
                
                if res_m and res_l:
                    m_id = res_m.id_categoria
                    l_id = res_l.id_categoria
                    if m_id != l_id:
                        merge_categories(conn, m_id, master_name, l_id, loser_name)
                    else:
                        print(f"Skipping {loser_name} -> {master_name}: Same ID ({m_id})")
                else:
                    print(f"Skipping {loser_name} -> {master_name}: One or both not found.")

            # PART 2: Automatic Case-Insensitive Merges
            print("\n--- Performing Automatic Cleaning ---")
            raw_data = conn.execute(text("SELECT id_categoria, nombre_categoria FROM categorias")).fetchall()
            
            groups = {}
            for id_val, name in raw_data:
                norm = clean_text(name)
                if not norm: continue
                if norm not in groups: groups[norm] = []
                groups[norm].append({'id': id_val, 'name': name})
            
            fusion_count = 0
            for norm_name, items in groups.items():
                if len(items) > 1:
                    fusion_count += 1
                    
                    # Heuristic
                    def score_candidate(item):
                        name = item['name']
                        score = 0
                        if name != name.lower() and name != name.upper(): score += 2 
                        if name == name.title(): score += 1
                        return (-score, item['id']) 

                    sorted_items = sorted(items, key=score_candidate)
                    master = sorted_items[0]
                    
                    duplicates = sorted_items[1:]
                    
                    for dup in duplicates:
                         # Re-verify existence (it might have been merged in manual step?) 
                         # Actually manual step runs first, and fetch is fresh, so ids should be valid unless duplicates were in manual list too.
                         # But safe to just try merge.
                         merge_categories(conn, master['id'], master['name'], dup['id'], dup['name'])

            trans.commit()
            print(f"Category cleaning completed. Auto-merged {fusion_count} groups.")
            
        except Exception as e:
            trans.rollback()
            print(f"Error during category cleaning, rolled back: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    clean_categories()
