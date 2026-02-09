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
        print("Error: No se encontró la configuración para 'Local'.")
        sys.exit(1)
    return sa.create_engine(local_target['url'])

def clean_subcategories_safe():
    print("--- Limpieza Profunda de Subcategorías (Python Safe Mode) ---")
    engine = get_local_engine()
    
    with engine.connect() as conn:
        # 1. Definir Maestros (Subcategoria -> ID Categoria Padre)
        # Ajusta los IDs de categoría padre según tu BD real si difieren
        masters = [
            # Proteinas (ID 1)
            ('Proteína Aislada', 1), ('Proteína Hidrolizada', 1), ('Proteína Vegana', 1),
            ('Proteína de Carne', 1), ('Caseína', 1), ('Barras y Snacks', 1), ('Proteína Concentrada', 1),
            
            # Vitaminas (ID 21 - Asumiendo fusion de vitaminas aqui)
            ('Multivitamínicos', 21), ('Vitamina C', 21), ('Vitamina B / Complejo B', 21),
            ('Vitamina D', 21), ('Vitamina E', 21), ('Minerales (Magnesio/ZMA)', 21),
            ('Omega 3 y Aceites', 21), ('Colágeno', 21), ('Probióticos', 21), ('Bienestar General', 21),
            
            # Creatinas (ID 2)
            ('Creatina Monohidrato', 2),
            
            # Pre-Entrenos (ID 3)
            ('Pre-Entreno con Estimulantes', 3), ('Pre-Entreno Sin Estimulantes', 3), ('Energía (Geles/Café)', 3),
            
            # Aminoacidos (ID 8)
            ('BCAAs', 8), ('EAAs (Esenciales)', 8), ('Glutamina', 8)
        ]
        
        # 1.1 Asegurar que existan los Maestros
        print("Asegurando subcategorías maestras...")
        for name, cat_id in masters:
            # Check existence
            exists = conn.execute(text("SELECT id_subcategoria FROM subcategorias WHERE nombre_subcategoria = :name"), {"name": name}).fetchone()
            if not exists:
                try:
                    conn.execute(text("INSERT INTO subcategorias (nombre_subcategoria, id_categoria) VALUES (:name, :cid)"), {"name": name, "cid": cat_id})
                    conn.commit()
                    print(f"  [Creada] {name}")
                except Exception as e:
                    print(f"  [Error Creando] {name}: {e}")
        
        # 1.2 Mapeo de fusiones (Origen -> Destino)
        # Key: Nombre 'Sucio' (Case insensitive en logica abajo) -> Value: Nombre 'Limpio'
        moves = {
            # Proteinas
            'Whey Isolate': 'Proteína Aislada', 'Clear Whey Isolate': 'Proteína Aislada', 'Proteina Isolate': 'Proteína Aislada',
            'Isolate Aislada': 'Proteína Aislada', 'Proteinas Isoladas': 'Proteína Aislada', 'Isolate Protein': 'Proteína Aislada',
            'Whey Protein': 'Proteína Concentrada', 'Proteina Whey': 'Proteína Concentrada', 'Concentradas': 'Proteína Concentrada', 'Proteinas': 'Proteína Concentrada',
            'Hidrolizada': 'Proteína Hidrolizada', 'Proteinas Hidrolizadas': 'Proteína Hidrolizada',
            'Proteina Vegana': 'Proteína Vegana', 'Proteinas Veganas': 'Proteína Vegana', 'Proteinas Vegetarianas': 'Proteína Vegana',
            'Proteina De Carne': 'Proteína de Carne', 'Proteinas De Carne': 'Proteína de Carne',
            'Caseina': 'Caseína', 'Proteinas Caseinas': 'Caseína',
            'Barras Proteicas': 'Barras y Snacks', 'Snack Proteico': 'Barras y Snacks', 'Barritas Proteicas': 'Barras y Snacks',
            
            # Vitaminas
            'Multivitaminicos': 'Multivitamínicos', 'Multivitaminicos Y Energia': 'Multivitamínicos',
            'Vitamina B': 'Vitamina B / Complejo B', 'B Complex': 'Vitamina B / Complejo B', 'Acido Folico': 'Vitamina B / Complejo B',
            'Magnesio': 'Minerales (Magnesio/ZMA)', 'Zma': 'Minerales (Magnesio/ZMA)', 'Zinc': 'Minerales (Magnesio/ZMA)',
            'Omega': 'Omega 3 y Aceites', 'Omega 3': 'Omega 3 y Aceites', 'Aceites y Omegas': 'Omega 3 y Aceites',
            'Colageno': 'Colágeno', 'Colagenos 1': 'Colágeno',
            'Probioticos': 'Probióticos', 'Probioticos Y Prebioticos': 'Probióticos',
            'Antioxidantes': 'Bienestar General', 'Coenzima Q10': 'Bienestar General', 'Super Alimentos': 'Bienestar General',
            
            # Creatinas
            'Monohidratada': 'Creatina Monohidrato', 'Creatinas': 'Creatina Monohidrato', 'Micronizada': 'Creatina Monohidrato',
            
            # Pre-Entrenos
            'Pre Entrenos': 'Pre-Entreno con Estimulantes', 'Pre Workout': 'Pre-Entreno con Estimulantes', 'Cafeina': 'Pre-Entreno con Estimulantes',
            'Libre De Estimulantes': 'Pre-Entreno Sin Estimulantes',
            'Shots Y Geles': 'Energía (Geles/Café)', 'Energeticas': 'Energía (Geles/Café)',
            
            # Aminos
            'BCAA': 'BCAAs', 'Bcaa': 'BCAAs', 'Aminoacidos y BCAA': 'BCAAs',
            'Eaa': 'EAAs (Esenciales)', 'Aminoacidos': 'EAAs (Esenciales)'
        }
        
        print("Migrando productos...")
        count_moved = 0
        
        # Get all subcategories map: Name -> ID
        all_subs = conn.execute(text("SELECT id_subcategoria, nombre_subcategoria FROM subcategorias")).fetchall()
        name_to_id = {row.nombre_subcategoria.lower().strip(): row.id_subcategoria for row in all_subs}
        # Reverse map for display
        id_to_name = {row.id_subcategoria: row.nombre_subcategoria for row in all_subs}
        
        for dirty, clean in moves.items():
            dirty_norm = dirty.lower().strip()
            clean_norm = clean.lower().strip()
            
            target_id = name_to_id.get(clean_norm)
            source_id = name_to_id.get(dirty_norm)
            
            if source_id and target_id and source_id != target_id:
                # Move products
                res = conn.execute(
                    text("UPDATE productos SET id_subcategoria = :tid WHERE id_subcategoria = :sid"),
                    {"tid": target_id, "sid": source_id}
                )
                if res.rowcount > 0:
                    print(f"  Movidos {res.rowcount} productos de '{dirty}' -> '{clean}'")
                    count_moved += res.rowcount
                conn.commit()
                
        print(f"Total productos movidos: {count_moved}")
        
        # 3. Eliminar subcategorías vacías (Excepto Maestros)
        print("Eliminando subcategorías vacías...")
        
        # Safe list of IDs to KEEP (Masters)
        keep_ids = []
        for name, _ in masters:
            nid = name_to_id.get(name.lower().strip())
            if nid: keep_ids.append(nid)
            
        # Add 'Otros' categories to keep list
        for name, nid in name_to_id.items():
            if name.startswith('otros '):
                keep_ids.append(nid)
                
        if keep_ids:
            query = text(f"""
                DELETE FROM subcategorias 
                WHERE id_subcategoria NOT IN (SELECT DISTINCT id_subcategoria FROM productos)
                AND id_subcategoria NOT IN :kids
            """)
            # Need to pass tuple for IN clause
            res = conn.execute(query, {"kids": tuple(keep_ids)})
            conn.commit()
            print(f"Eliminadas {res.rowcount} subcategorías vacías.")
        
    print("Limpieza completada.")

if __name__ == "__main__":
    clean_subcategories_safe()
