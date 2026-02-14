import os
import sys
import sqlalchemy as sa
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

def get_score(cat_name, prod_name):
    score = 0
    if not cat_name:
        pass # score 0
    
    cat_lower = cat_name.lower() if cat_name else ""
    
    if cat_name in IMPORTANT_CATEGORIES:
        score += 10
        
    for keyword in LOW_PRIORITY_KEYWORDS:
        if keyword in cat_lower:
            score -= 100
            break
            
    # Prefer Title Case or non-lowercase names
    if prod_name and not prod_name.islower():
        score += 20 # Strong preference for Title Case
        
    return score

def fetch_categories_bulk(conn, product_ids):
    if not product_ids: return {}
    chunk_size = 2000
    product_categories = {}
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
    if not product_ids: return {}
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

def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local' en db_multiconnect.")
        sys.exit(1)
    return sa.create_engine(local_target['url'])

def fix_duplicates(engine):
    print("\n--- Deduplicando Productos (V2 Optimized - Case Insensitive) ---")

    with engine.begin() as conn:
        print("  Buscando grupos de duplicados (insensible a mayúsculas)...")
        
        # 1. Buscar Grupos (Same Name LOWER, Same Brand)
        # Also fetch the array of names to check casing
        query = sa.text("""
            SELECT lower(nombre_producto) as clean_name, id_marca, array_agg(id_producto) as ids, array_agg(nombre_producto) as names
            FROM productos
            GROUP BY lower(nombre_producto), id_marca
            HAVING count(*) > 1
        """)
        grupos = conn.execute(query).fetchall()
        
        if not grupos:
            print("  No se encontraron duplicados.")
            return

        print(f"  Encontrados {len(grupos)} grupos de duplicados.")
        
        # Collect IDs
        all_product_ids = set()
        for row in grupos:
            for pid in row.ids:
                all_product_ids.add(pid)
        
        # 2. Bulk Fetch
        categories_map = fetch_categories_bulk(conn, all_product_ids)
        links_map = fetch_links_bulk(conn, all_product_ids)
        
        # 3. Drop Constraint (if exists) for history update
        try:
             conn.execute(sa.text("ALTER TABLE historia_precios DROP CONSTRAINT IF EXISTS uq_precio_fecha"))
        except: pass

        # 4. Prepare Operations
        ops_update_history = [] 
        ops_delete_links_ids = []
        ops_move_links = []      
        ops_delete_product_ids = []
        ops_move_analytics = [] # New list for click_analytics

        processed_count = 0
        
        for row in grupos:
            raw_ids = row.ids
            raw_names = row.names
            
            # Create map of id -> name
            id_to_name = dict(zip(raw_ids, raw_names))
            
            # Score Candidates
            candidates = []
            for pid in raw_ids:
                cat_name = categories_map.get(pid, "")
                p_name = id_to_name.get(pid, "")
                score = get_score(cat_name, p_name)
                candidates.append({'id': pid, 'score': score})
            
            # Sort: High Score first, then Low ID (older is master usually, BUT here we want Title Case master)
            # If Title Case is new (Highest ID), it has High Score, so it comes first.
            candidates.sort(key=lambda x: (-x['score'], x['id']))
            
            master_id = candidates[0]['id']
            dupes = candidates[1:]
            
            # Master links map: {tienda_id: link_id}
            master_links = {l['id_tienda']: l['id_producto_tienda'] for l in links_map.get(master_id, [])}
            
            for d in dupes:
                dupe_id = d['id']
                dupe_links = links_map.get(dupe_id, [])
                
                ops_delete_product_ids.append(dupe_id)
                ops_move_analytics.append({"new": master_id, "old": dupe_id}) # Capture for analytics
                
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
                        master_links[tienda_id] = link_id_dupe 

            processed_count += 1

        # 5. Execute
        print("  Ejecutando cambios en BD...")

        # A. Update History
        if ops_update_history:
             print(f"  Migrando historiales ({len(ops_update_history)} ops)...")
             conn.execute(sa.text("UPDATE historia_precios SET id_producto_tienda = :new_id WHERE id_producto_tienda = :old_id"), ops_update_history)

        # B. Delete Conflicting Links
        if ops_delete_links_ids:
             print(f"  Eliminando links redundantes ({len(ops_delete_links_ids)} ops)...")
             chunk_size = 5000
             for i in range(0, len(ops_delete_links_ids), chunk_size):
                 chunk = tuple(ops_delete_links_ids[i:i + chunk_size])
                 conn.execute(sa.text("DELETE FROM producto_tienda WHERE id_producto_tienda IN :ids"), {"ids": chunk})

        # C. Move Valid Links
        if ops_move_links:
             print(f"  Moviendo links al maestro ({len(ops_move_links)} ops)...")
             conn.execute(sa.text("UPDATE producto_tienda SET id_producto = :master_id WHERE id_producto_tienda = :link_id"), ops_move_links)
        
        # D. Move Analytics
        if ops_move_analytics:
             print(f"  Migrando analíticas al maestro ({len(ops_move_analytics)} ops)...")
             # click_analytics FK is on id_producto
             conn.execute(sa.text("UPDATE click_analytics SET id_producto = :new WHERE id_producto = :old"), ops_move_analytics)

        # E. Delete Products
        if ops_delete_product_ids:
             print(f"  Eliminando productos duplicados ({len(ops_delete_product_ids)} ops)...")
             chunk_size = 5000
             for i in range(0, len(ops_delete_product_ids), chunk_size):
                 chunk = tuple(ops_delete_product_ids[i:i + chunk_size])
                 conn.execute(sa.text("DELETE FROM productos WHERE id_producto IN :ids"), {"ids": chunk})

        
        print(f"  Deduplicación completada. Grupos: {processed_count}. Productos eliminados: {len(ops_delete_product_ids)}.")

def main():
    print("--- PASO 4: Deduplicación de Productos ---")
    engine = get_local_engine()
    fix_duplicates(engine)

if __name__ == "__main__":
    main()
