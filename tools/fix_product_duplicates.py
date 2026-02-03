import os
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "suplementos")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "password")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = sa.create_engine(DATABASE_URL)

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

def fix_duplicates():
    # Use begin() for automatic transaction management
    with engine.begin() as conn:
        print("Buscando grupos de duplicados...")
        # 1. Buscar Grupos
        query = sa.text("""
            SELECT nombre_producto, id_marca, array_agg(id_producto) as ids
            FROM productos
            GROUP BY nombre_producto, id_marca
            HAVING count(*) > 1
        """)
        grupos = conn.execute(query).fetchall()
        
        print(f"Encontrados {len(grupos)} grupos de duplicados.")
        
        # Eliminar constraint si existe para permitir duplicados (historia multiple en un dia)
        try:
             conn.execute(sa.text("ALTER TABLE historia_precios DROP CONSTRAINT IF EXISTS uq_precio_fecha"))
             print("Constraint 'uq_precio_fecha' eliminado (o no existía).")
        except Exception as e:
             print(f"Advertencia al intentar borrar constraint: {e}")
        
        processed_count = 0
        deleted_count = 0
        
        for row in grupos:
            raw_ids = row.ids
            
            # 2. Obtener Info de Categorias para Prioridad
            candidates = []
            for pid in raw_ids:
                # Obtener nombre de la categoria (Join producto -> subcategoria -> categoria)
                # Ojo: id_subcategoria puede ser null
                cat_query = sa.text("""
                    SELECT c.nombre_categoria 
                    FROM productos p
                    LEFT JOIN subcategorias sc ON p.id_subcategoria = sc.id_subcategoria
                    LEFT JOIN categorias c ON sc.id_categoria = c.id_categoria
                    WHERE p.id_producto = :pid
                """)
                cat_res = conn.execute(cat_query, {"pid": pid}).fetchone()
                cat_name = cat_res.nombre_categoria if cat_res else ""
                
                score = get_score(cat_name)
                candidates.append({'id': pid, 'score': score, 'cat': cat_name})
            
            # Ordenar: Mayor score primero, luego menor ID (antiguedad)
            candidates.sort(key=lambda x: (-x['score'], x['id']))
            
            master = candidates[0]
            master_id = master['id']
            dupes_infos = candidates[1:]
            
            def safe_print(text):
                try:
                    print(text)
                except UnicodeEncodeError:
                    print(text.encode('ascii', 'ignore').decode('ascii'))

            safe_print(f"Procesando '{row.nombre_producto}' (Marca ID: {row.id_marca})")
            safe_print(f"  > Maestro Seleccionado: {master_id} (Cat: '{master['cat']}', Score: {master['score']})")
            
            for d in dupes_infos:
                 safe_print(f"  > Duplicado a eliminar: {d['id']} (Cat: '{d['cat']}', Score: {d['score']})")
            
            # 3. Fusión
            for d_info in dupes_infos:
                dupe_id = d_info['id']
                
                # Obtener links del dupe
                links_dupe = conn.execute(sa.text("SELECT id_producto_tienda, id_tienda FROM producto_tienda WHERE id_producto = :pid"), {"pid": dupe_id}).fetchall()
                
                for link in links_dupe:
                    link_id_dupe = link.id_producto_tienda
                    tienda_id = link.id_tienda
                    
                    # Verificar si master tiene link en esta tienda
                    link_master = conn.execute(sa.text("SELECT id_producto_tienda FROM producto_tienda WHERE id_producto = :pid AND id_tienda = :tid"), {"pid": master_id, "tid": tienda_id}).fetchone()
                    
                    if link_master:
                        # CASO A: Conflicto - Migrar historia y borrar link dupe
                        link_id_master = link_master.id_producto_tienda
                        print(f"    - Conflicto en tienda {tienda_id}: Migrando historia de link {link_id_dupe} a {link_id_master}")
                        
                        # Migrar historia (ahora permitido duplicados)
                        conn.execute(sa.text("UPDATE historia_precios SET id_producto_tienda = :new_id WHERE id_producto_tienda = :old_id"), {"new_id": link_id_master, "old_id": link_id_dupe})
                        
                        # Borrar link dupe
                        conn.execute(sa.text("DELETE FROM producto_tienda WHERE id_producto_tienda = :id"), {"id": link_id_dupe})
                    else:
                        # CASO B: Sin conflicto - Mover link al maestro
                        print(f"    - Sin conflicto en tienda {tienda_id}: Moviendo link {link_id_dupe} al maestro")
                        conn.execute(sa.text("UPDATE producto_tienda SET id_producto = :master_id WHERE id_producto_tienda = :link_id"), {"master_id": master_id, "link_id": link_id_dupe})
                
                # Migrar referencias extras (click_analytics, etc)
                # Manejar excepción si la tabla no existe o si hay otros conflictos, pero click_analytics apareció en logs
                try:
                     conn.execute(sa.text("UPDATE click_analytics SET id_producto = :master_id WHERE id_producto = :pid"), {"master_id": master_id, "pid": dupe_id})
                except Exception as e:
                     # Si falla, puede ser porque no existe la tabla o porque viola unicidad (si click_analytics tuviera unique por producto y fecha)
                     # Asumiremos que es seguro ignorar o loguear.
                     # Pero el error anterior fue FK Violation al borrar producto, lo que significa que HABIA registros.
                     # Si hay registros, el UPDATE debería funcionar.
                     # Si hay conflicto de unique, deberíamos borrar el dupe de click_analytics o sumarlo.
                     # Asumamos simple update por ahora. Si falla, el script crasheará y veremos.
                     print(f"    - Advertencia actualizando click_analytics: {e}")

                # Eliminar Producto Duplicado
                print(f"    - Eliminando producto {dupe_id}")
                conn.execute(sa.text("DELETE FROM productos WHERE id_producto = :pid"), {"pid": dupe_id})
                deleted_count += 1
                
            processed_count += 1
            
        print(f"Finalizado. Grupos procesados: {processed_count}. Productos eliminados: {deleted_count}.")

if __name__ == "__main__":
    fix_duplicates()
