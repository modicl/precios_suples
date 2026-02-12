import os
import sys
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def clean_text(text_val):
    """Normalize text for grouping."""
    if not isinstance(text_val, str):
        return ""
    return text_val.strip().lower()

def merge_categories(conn, master_id, master_name, loser_id, loser_name, db_name=""):
    print(f"[{db_name}] Merging '{loser_name}' ({loser_id}) -> '{master_name}' ({master_id})")
    
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

def clean_categories_db(engine, db_name):
    print(f"\n--- Cleaning Categories for: {db_name} ---")
    
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # PART 1: Manual Semantic Merges
            manual_merges = [
                # (Master Name, Loser Name)
                ("Aminoacidos y BCAA", "Aminoacidos"),
                ("Snacks y Comida", "Snacks"),
                ("Ganadores de Peso", "Ganador de Masa"),
                ("Creatinas", "Creatina")
            ]
            
            print(f"[{db_name}] Performing Manual Merges...")
            for master_name, loser_name in manual_merges:
                # Find IDs
                res_m = conn.execute(text("SELECT id_categoria FROM categorias WHERE nombre_categoria ILIKE :name"), {'name': master_name}).fetchone()
                res_l = conn.execute(text("SELECT id_categoria FROM categorias WHERE nombre_categoria ILIKE :name"), {'name': loser_name}).fetchone()
                
                if res_m and res_l:
                    m_id = res_m.id_categoria
                    l_id = res_l.id_categoria
                    if m_id != l_id:
                        merge_categories(conn, m_id, master_name, l_id, loser_name, db_name)
                    else:
                        print(f"[{db_name}] Skipping {loser_name} -> {master_name}: Same ID ({m_id})")
                else:
                    # print(f"[{db_name}] Skipping {loser_name} -> {master_name}: One or both not found.")
                    pass

            # PART 2: Automatic Case-Insensitive Merges
            print(f"[{db_name}] Performing Automatic Cleaning...")
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
                         merge_categories(conn, master['id'], master['name'], dup['id'], dup['name'], db_name)

            trans.commit()
            print(f"[{db_name}] Category cleaning completed. Auto-merged {fusion_count} groups.")
            
        except Exception as e:
            trans.rollback()
            print(f"[{db_name}] Error during category cleaning, rolled back: {e}")
            import traceback
            traceback.print_exc()

def main():
    load_dotenv()
    
    targets = get_targets()
    if not targets:
        print("Error: No database targets found.")
        return

    print(f"Found database targets: {[t['name'] for t in targets]}")

    for target in targets:
        db_name = target["name"]
        db_url = target["url"]
        
        try:
            print(f"\nConnecting to {db_name}...")
            engine = sa.create_engine(db_url)
            clean_categories_db(engine, db_name)
        except Exception as e:
            print(f"Error connecting/processing {db_name}: {e}")

if __name__ == "__main__":
    main()
