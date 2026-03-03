import sys
import os
import sqlalchemy as sa
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def fix_encoding():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No Local DB config found.")
        return

    print(f"Connecting to Local DB: {local_target['url']}")
    engine = sa.create_engine(local_target['url'])

    # Replacement map for known issues
    # Note: We match the broken string pattern. 
    # Python might read '' differently depending on encoding, 
    # but let's try to match by partial string or SQL UPDATE with LIKE.
    
    # Actually, let's fetch, fix in python, and update by ID. Safer.
    
    replacements = {
        "Protena": "Proteína",
        "Prote?na": "Proteína",
        "Casena": "Caseína",
        "Case?na": "Caseína",
        "Multivitamnicos": "Multivitamínicos",
        "Multivitam?nicos": "Multivitamínicos",
        "Probiticos": "Probióticos",
        "Probi?ticos": "Probióticos",
        "Energa": "Energía",
        "Energ?a": "Energía",
        "Caf": "Café",
        "Caf?": "Café",
        "Colgeno": "Colágeno",
        "Col?geno": "Colágeno",
        "Hidratacin": "Hidratación",
        "Hidrataci?n": "Hidratación",
        "Multivitamínicos": "Multivitamínicos", # Ensure standard
    }

    with engine.connect() as conn:
        # Fetch all subcategories
        subs = conn.execute(text("SELECT id_subcategoria, nombre_subcategoria FROM subcategorias")).fetchall()
        
        print(f"Checking {len(subs)} subcategories...")
        
        for row in subs:
            original_name = row.nombre_subcategoria
            new_name = original_name
            
            # 1. Bruteforce replacement of the specific replacement character if present
            # The character might be \ufffd (replacement char) or similar.
            
            changed = False
            
            # Fix specific words
            if "Protena" in new_name or "Prote?na" in new_name:
                new_name = new_name.replace("Protena", "Proteína").replace("Prote?na", "Proteína")
                changed = True
            if "Casena" in new_name or "Case?na" in new_name:
                new_name = new_name.replace("Casena", "Caseína").replace("Case?na", "Caseína")
                changed = True
            if "Multivitamnicos" in new_name or "Multivitam?nicos" in new_name:
                new_name = new_name.replace("Multivitamnicos", "Multivitamínicos").replace("Multivitam?nicos", "Multivitamínicos")
                changed = True
            if "Probiticos" in new_name or "Probi?ticos" in new_name:
                new_name = new_name.replace("Probiticos", "Probióticos").replace("Probi?ticos", "Probióticos")
                changed = True
            if "Energa" in new_name or "Energ?a" in new_name:
                new_name = new_name.replace("Energa", "Energía").replace("Energ?a", "Energía")
                changed = True
            if "Caf" in new_name or "Caf? " in new_name: # Caf? might match Cafe
                new_name = new_name.replace("Caf", "Café")
                changed = True
            if "Colgeno" in new_name or "Col?geno" in new_name:
                new_name = new_name.replace("Colgeno", "Colágeno").replace("Col?geno", "Colágeno")
                changed = True
            if "Hidratacin" in new_name:
                new_name = new_name.replace("Hidratacin", "Hidratación")
                changed = True
                
            # Generic  fixer if logic above missed something but we can guess?
            # Better manual control.
            
            if changed:
                print(f"Fixing ID {row.id_subcategoria}: '{original_name}' -> '{new_name}'")
                conn.execute(
                    text("UPDATE subcategorias SET nombre_subcategoria = :name WHERE id_subcategoria = :id"),
                    {"name": new_name, "id": row.id_subcategoria}
                )
                conn.commit()

if __name__ == "__main__":
    fix_encoding()
