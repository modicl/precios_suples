import os
import sys
import sqlalchemy as sa
from sqlalchemy import text

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from tools.db_multiconnect import get_targets

def check_wild():
    targets = get_targets()
    url = next((t['url'] for t in targets if t['name'] == 'Local'), None)
    engine = sa.create_engine(url)
    
    with engine.connect() as conn:
        print("--- Productos Wild Protein en BD ---")
        rows = conn.execute(text("""
            SELECT p.id_producto, p.nombre_producto, length(p.nombre_producto), m.nombre_marca, s.nombre_subcategoria 
            FROM productos p
            JOIN marcas m ON p.id_marca = m.id_marca
            JOIN subcategorias s ON p.id_subcategoria = s.id_subcategoria
            WHERE p.nombre_producto ILIKE 'Wild Protein%'
        """)).fetchall()
        
        for r in rows:
            print(f"ID: {r.id_producto} | Name: '{r.nombre_producto}' (Len: {r[2]}) | Brand: {r.nombre_marca} | Sub: {r.nombre_subcategoria}")

if __name__ == "__main__":
    check_wild()
