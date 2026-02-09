import os
import sys
import sqlalchemy as sa
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from tools.db_multiconnect import get_targets

def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local' en db_multiconnect.")
        sys.exit(1)
    return sa.create_engine(local_target['url'])

def ensure_others():
    print("--- Asegurando Subcategoría 'Otros' por Categoría ---")
    engine = get_local_engine()
    
    with engine.connect() as conn:
        # Get all categories
        cats = conn.execute(text("SELECT id_categoria, nombre_categoria FROM categorias")).fetchall()
        
        created_count = 0
        
        for c in cats:
            cat_id = c.id_categoria
            cat_name = c.nombre_categoria
            
            # Check if "Otros" exists for this category
            # We look for "Otros" exactly, or maybe "Otros {CatName}" if you prefer specific names
            # Let's stick to simple "Otros" first, or specific if unique constraint requires unique names globally?
            # Usually subcategory names are not unique globally, only unique per category? 
            # Check constraint: subcategorias_nombre_subcategoria_key usually implies global uniqueness if simple constraint.
            # Let's check constraint definition first or assume names must be unique.
            # If names must be unique, we must use "Otros {CatName}".
            
            # Let's try "Otros {CatName}" to be safe and clear.
            target_name = f"Otros {cat_name}"
            
            # Check existence
            exists = conn.execute(
                text("SELECT 1 FROM subcategorias WHERE nombre_subcategoria = :name"),
                {'name': target_name}
            ).fetchone()
            
            if not exists:
                print(f"  Creando '{target_name}' para categoría '{cat_name}'...")
                conn.execute(
                    text("INSERT INTO subcategorias (nombre_subcategoria, id_categoria) VALUES (:name, :cid)"),
                    {'name': target_name, 'cid': cat_id}
                )
                created_count += 1
            else:
                # Check if it belongs to correct category, if not, name collision?
                # Assuming unique names, so it must be ours or collision.
                pass
                
        conn.commit()
        print(f"Finalizado. Creadas {created_count} subcategorías 'Otros'.")

if __name__ == "__main__":
    ensure_others()
