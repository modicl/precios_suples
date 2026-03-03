
import sys
import os
import sqlalchemy as sa

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def get_taxonomy():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No Local DB target found.")
        return

    try:
        engine = sa.create_engine(local_target['url'])
        with engine.connect() as conn:
            # Get Categories
            cats = conn.execute(sa.text("SELECT id_categoria, nombre_categoria FROM categorias")).fetchall()
            cat_map = {row.id_categoria: row.nombre_categoria for row in cats}
            
            # Get Subcategories
            result = conn.execute(sa.text("SELECT nombre_subcategoria, id_categoria FROM subcategorias ORDER BY nombre_subcategoria"))
            
            print("--- VALID SUBCATEGORIES ---")
            for row in result:
                parent = cat_map.get(row.id_categoria, "Unknown")
                print(f"Sub: '{row.nombre_subcategoria}' -> Cat: '{parent}'")
                
    except Exception as e:
        print(f"Error connecting to DB: {e}")

if __name__ == "__main__":
    get_taxonomy()
