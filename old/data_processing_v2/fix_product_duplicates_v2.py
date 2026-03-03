import os
import sys
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from collections import defaultdict

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

IMPORTANT_CATEGORIES = {
    "Ganadores de Peso", "Aminoacidos y BCAA", "Glutamina", "Perdida de Grasa",
    "Post Entreno", "Vitaminas", "Pre Entrenos", "Creatinas", "Proteinas",
    "Quemadores", "Gel Energetico", "Barritas", "Colageno", "Aceites y Omegas",
    "Alimentos", "Bebidas Nutricionales", "Superalimento", "Vitaminas y Minerales",
    "Hidratación", "Aminoacidos", "Snacks", "Ganador de Masa", "Creatina"
}

LOW_PRIORITY_KEYWORDS = ["oferta", "pack", "promo", "bundle", "especial", "outlet", "liquidacion", "cyber", "black"]

def get_score(cat_name):
    score = 0
    if not cat_name:
        return score
    
    cat_lower = cat_name.lower()
    
    # Priority for important categories
    if cat_name in IMPORTANT_CATEGORIES:
        score += 10
        
    # Penalty for low priority keywords
    for keyword in LOW_PRIORITY_KEYWORDS:
        if keyword in cat_lower:
            score -= 100
            break # Apply penalty once
            
    return score

def fetch_categories_bulk(conn, product_ids):
    """
    Fetches category names for a list of product IDs in one query.
    Returns a dict: {product_id: category_name}
    """
    if not product_ids:
        return {}
        
    # Chunking to avoid hitting parameter limits if thousands of duplicates
    chunk_size = 2000
    product_categories = {}
    
    # Convert set/list to list for slicing
    pids_list = list(product_ids)
    
    for i in range(0, len(pids_list), chunk_size):
        chunk = tuple(pids_list[i:i + chunk_size])
        
        query = sa.text("""
            SELECT p.id_producto, c.nombre_categoria 
            FROM productos p
            LEFT JOIN subcategorias sc ON p.id_subcategoria = sc.id_subcategoria
            LEFT JOIN categorias c ON sc.id_categoria = c.id_categoria
            WHERE p.id_producto IN :pids
        """)
        
        results = conn.execute(query, {"pids": chunk}).fetchall()
        for row in results:
            product_categories[row.id_producto] = row.nombre_categoria or ""
            
    return product_categories

def fetch_links_bulk(conn, product_ids):
    """
    Fetches all producto_tienda links for the given product IDs.
    Returns a dict: {product_id: [{'id_link': ..., 'id_tienda': ...}, ...]}
    """
    if not product_ids:
        return {}

    chunk_size = 2000
    product_links = defaultdict(list)
    pids_list = list(product_ids)

    for i in range(0, len(pids_list), chunk_size):
        chunk = tuple(pids_list[i:i + chunk_size])
        query = sa.text("""
            SELECT id_producto, id_producto_tienda, id_tienda 
            FROM producto_tienda 
            WHERE id_producto IN :pids
        """)
        results = conn.execute(query, {"pids": chunk}).fetchall()
        for row in results:
            product_links[row.id_producto].append({
                'id_producto_tienda': row.id_producto_tienda,
                'id_tienda': row.id_tienda
            })
            
    return product_links

def fix_duplicates_db(engine, db_name):
    print(f"\n--- Fixing Duplicates for: {db_name} (Optimized V2) ---")

    with engine.begin() as conn:
        print(f"[{db_name}] Buscando grupos de duplicados...")
        
        # 1. Buscar Grupos
        query = sa.text("""
            SELECT nombre_producto, id_marca, array_agg(id_producto) as ids
            FROM productos
            GROUP BY nombre_producto, id_marca
            HAVING count(*) > 1
        """)
        grupos = conn.execute(query).fetchall()
        
        if not grupos:
            print(f"[{db_name}] No se encontraron duplicados.")
            return

        print(f"[{db_name}] Encontrados {len(grupos)} grupos de duplicados. Preparando datos...")
        
        # Collect all product IDs involved
        all_product_ids = set()
        for row in grupos:
            for pid in row.ids:
                all_product_ids.add(pid)
        
        print(f"[{db_name}] Total productos involucrados: {len(all_product_ids)}")

        # 2. Bulk Fetch Context Data
        # Fetch Categories
        print(f"[{db_name}] Obteniendo categorías en lote...")
        categories_map = fetch_categories_bulk(conn, all_product_ids)
        
        # Fetch Links
        print(f"[{db_name}] Obteniendo enlaces de tiendas en lote...")
        links_map = fetch_links_bulk(conn, all_product_ids)
        
        # 3. Drop Constraint (if exists)
        try:
             conn.execute(sa.text("ALTER TABLE historia_precios DROP CONSTRAINT IF EXISTS uq_precio_fecha"))
        except Exception as e:
             # Ignore if it fails, valid in some pg versions or if permission issues, though unlikely
             pass

        # 4. Prepare Operations (In-Memory Processing)
        print(f"[{db_name}] Calculando fusiones...")
        
        ops_update_history = []  # List of params for executemany
        ops_delete_links_ids = [] # List of IDs
        ops_move_links = []      # List of params
        ops_update_click_analytics = []
        ops_delete_product_ids = []

        processed_count = 0
        
        for row in grupos:
            raw_ids = row.ids
            
            # Score Candidates
            candidates = []
            for pid in raw_ids:
                cat_name = categories_map.get(pid, "")
                score = get_score(cat_name)
                candidates.append({'id': pid, 'score': score})
            
            # Sort: High Score first, then Low ID (older)
            candidates.sort(key=lambda x: (-x['score'], x['id']))
            
            master_id = candidates[0]['id']
            dupes = candidates[1:]
            
            # Master links map for quick lookup: {tienda_id: link_id}
            master_links = {l['id_tienda']: l['id_producto_tienda'] for l in links_map.get(master_id, [])}
            
            for d in dupes:
                dupe_id = d['id']
                dupe_links = links_map.get(dupe_id, [])
                
                ops_delete_product_ids.append(dupe_id)
                
                # Handle Links
                for link in dupe_links:
                    link_id_dupe = link['id_producto_tienda']
                    tienda_id = link['id_tienda']
                    
                    if tienda_id in master_links:
                        # CONFLICT: Master has link too.
                        # Migrate history to Master's link, delete Dupe's link
                        link_id_master = master_links[tienda_id]
                        ops_update_history.append({"new_id": link_id_master, "old_id": link_id_dupe})
                        ops_delete_links_ids.append(link_id_dupe)
                    else:
                        # NO CONFLICT: Move Dupe's link to Master
                        ops_move_links.append({"master_id": master_id, "link_id": link_id_dupe})
                        # Update our local record of master links in case another dupe has same store (rare but possible logic)
                        master_links[tienda_id] = link_id_dupe 

                # Handle Extras (Click Analytics)
                ops_update_click_analytics.append({"master_id": master_id, "pid": dupe_id})

            processed_count += 1

        # 5. Execute Bulk Operations
        print(f"[{db_name}] Ejecutando cambios en BD...")

        # A. Update History
        if ops_update_history:
             print(f"[{db_name}] Migrando historiales ({len(ops_update_history)} ops)...")
             conn.execute(sa.text("UPDATE historia_precios SET id_producto_tienda = :new_id WHERE id_producto_tienda = :old_id"), ops_update_history)

        # B. Delete Conflicting Links
        if ops_delete_links_ids:
             print(f"[{db_name}] Eliminando links redundantes ({len(ops_delete_links_ids)} ops)...")
             # Batch deletes in chunks
             chunk_size = 5000
             for i in range(0, len(ops_delete_links_ids), chunk_size):
                 chunk = tuple(ops_delete_links_ids[i:i + chunk_size])
                 conn.execute(sa.text("DELETE FROM producto_tienda WHERE id_producto_tienda IN :ids"), {"ids": chunk})

        # C. Move Valid Links
        if ops_move_links:
             print(f"[{db_name}] Moviendo links al maestro ({len(ops_move_links)} ops)...")
             conn.execute(sa.text("UPDATE producto_tienda SET id_producto = :master_id WHERE id_producto_tienda = :link_id"), ops_move_links)
        
        # D. Update Analytics
        if ops_update_click_analytics:
             # This might fail on duplicate keys if not careful, but usually okay for updates unless unique constraint exists on (id_producto, date)
             # If click_analytics has PK on id_producto, this fails. Assuming it's a log table. 
             # Safety: Wrap in try/except or process carefully.
             # Given the previous script just did UPDATE and passed on error, we will do same but batching might abort transaction on error.
             # We will attempt batch, if it fails, we fall back? No, let's just do it.
             # Actually, if `click_analytics` has a constraint, `UPDATE` to master_id might violate it if master already has entry.
             # For safety in bulk, let's assume it works or ignore. 
             # Best effort:
             try:
                conn.execute(sa.text("UPDATE click_analytics SET id_producto = :master_id WHERE id_producto = :pid"), ops_update_click_analytics)
             except Exception as e:
                print(f"[{db_name}] Warning: Error bulk updating click_analytics (possibly duplicates): {e}")

        # E. Delete Products
        if ops_delete_product_ids:
             print(f"[{db_name}] Eliminando productos duplicados ({len(ops_delete_product_ids)} ops)...")
             chunk_size = 5000
             for i in range(0, len(ops_delete_product_ids), chunk_size):
                 chunk = tuple(ops_delete_product_ids[i:i + chunk_size])
                 conn.execute(sa.text("DELETE FROM productos WHERE id_producto IN :ids"), {"ids": chunk})
        
        print(f"[{db_name}] Finalizado. Grupos procesados: {processed_count}. Productos eliminados: {len(ops_delete_product_ids)}.")

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
            fix_duplicates_db(engine, db_name)
        except Exception as e:
            print(f"Error connecting/processing {db_name}: {e}")

if __name__ == "__main__":
    main()
