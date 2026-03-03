import sys
import os
import sqlalchemy as sa
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def fix_encoding_sql():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No Local DB config found.")
        return

    print(f"Connecting to Local DB: {local_target['url']}")
    engine = sa.create_engine(local_target['url'])

    with engine.connect() as conn:
        print("Executing SQL fixes...")
        
        # List of fix tuples: (Target Pattern, Replacement)
        # We use REPLACE() in SQL to handle occurrences
        
        # Common encoding errors observed:
        # "Protena" or "Protena" -> "Proteína"
        # "Casena" -> "Caseína"
        
        updates = [
            ("Protena", "Proteína"),
            ("Protena", "Proteína"),
            ("Prote?na", "Proteína"),
            
            ("Casena", "Caseína"),
            ("Casena", "Caseína"),
            ("Case?na", "Caseína"),
            
            ("Multivitamnicos", "Multivitamínicos"),
            ("Multivitamnicos", "Multivitamínicos"),
            
            ("Probiticos", "Probióticos"),
            ("Probiticos", "Probióticos"),
            
            ("Colgeno", "Colágeno"),
            ("Colgeno", "Colágeno"),
            
            ("Hidratacin", "Hidratación"),
            ("Hidratacin", "Hidratación"),
            
            ("Energa", "Energía"),
            ("Energa", "Energía"),
            
            ("Caf", "Café"),
            ("Caf?", "Café")
        ]
        
        for bad, good in updates:
            # We assume 'bad' might be part of the string
            # Postgres REPLACE(string, from, to)
            
            query = text(f"""
                UPDATE subcategorias 
                SET nombre_subcategoria = REPLACE(nombre_subcategoria, :bad, :good)
                WHERE nombre_subcategoria LIKE '%' || :bad || '%'
            """)
            
            result = conn.execute(query, {"bad": bad, "good": good})
            if result.rowcount > 0:
                print(f"Fixed '{bad}' -> '{good}': {result.rowcount} rows updated.")
                
        conn.commit()
        
        print("\n--- Current Subcategories ---")
        rows = conn.execute(text("SELECT nombre_subcategoria FROM subcategorias ORDER BY nombre_subcategoria")).fetchall()
        for r in rows:
            print(r[0])

if __name__ == "__main__":
    fix_encoding_sql()
