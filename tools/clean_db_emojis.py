import os
import re
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "suplementos")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "password")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def clean_text(text: str) -> str:
    if not text:
        return ""
    # Regex para eliminar emojis y símbolos gráficos, preservando acentos
    # \w, \s, acentos, guion, punto, coma
    return re.sub(r'[^\w\s\u00C0-\u00FF\u002D\u002E\u002C]', '', text).strip()

def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        # Encode to ascii (or cp1252) with replacement to strip/replace unprintable chars
        # Then decode back to string for print()
        try:
            print(msg.encode('ascii', errors='replace').decode('ascii'))
        except:
            print("  [Printing Error: Message contains unprintable characters]")

def clean_table(conn, table_name, id_col, name_col, ref_tables=[]):
    """
    Limpia una tabla (id, nombre) y maneja duplicados fusionando registros.
    ref_tables: Lista de tuplas (tabla_referencia, col_referencia) que apuntan a esta tabla via FK.
    """
    safe_print(f"\n--- Procesando tabla: {table_name} ---")
    
    # 1. Obtener todos los registros
    query = sa.text(f"SELECT {id_col}, {name_col} FROM {table_name}")
    rows = conn.execute(query).fetchall()
    
    # Mapa: nombre_limpio -> lista de IDs que tienen ese nombre (o similar)
    # Primero detectamos qué cambios hay que hacer
    
    # clean_map: { clean_name: [ {id: ..., original: ...}, ... ] }
    clean_map = {}
    
    for row in rows:
        original_id = getattr(row, id_col)
        original_name = getattr(row, name_col)
        
        if original_name is None:
            continue
            
        cleaned = clean_text(original_name)
        
        # Agrupar por nombre limpio
        if cleaned not in clean_map:
            clean_map[cleaned] = []
        clean_map[cleaned].append({'id': original_id, 'original': original_name})
        
    # Iterar sobre los grupos
    updates = 0
    merges = 0
    
    for cleaned_name, items in clean_map.items():
        # Si el nombre limpio es vacío, ¿qué hacemos? 
        # Idealmente no borrar, pero quizás dejarlo vacío o 'N/D'. 
        # Por ahora asumimos que si queda vacío es porque era todo emoji.
        if not cleaned_name:
            safe_print(f"  [WARN] Nombre '{items[0]['original']}' se limpió a vacío. ID: {items[0]['id']}")
            continue

        # Ordenar items: preferir el que ya sea igual al limpio, o el ID más bajo
        # Prioridad: 1. Exact match, 2. Lower ID
        items.sort(key=lambda x: (x['original'] != cleaned_name, x['id']))
        
        target_item = items[0] # El que sobrevivirá
        target_id = target_item['id']
        
        # 1. Actualizar el nombre del target si es necesario
        if target_item['original'] != cleaned_name:
            # Verificar si existe conflicto UNIQUE con un registro que NO estaba en este grupo (raro pero posible si DB inconsistente)
            # Pero como iteramos sobre TODOS los registros y agrupamos por nombre limpio, 
            # todos los conflictos de nombre limpio están en 'items'.
            
            # Sin embargo, la DB puede tener ya el nombre limpio en otro ID que acabamos de procesar?
            # No, porque estamos iterando el mapa completo.
            
            # Solo actualizamos.
            safe_print(f"  [UPDATE] ID {target_id}: '{target_item['original']}' -> '{cleaned_name}'")
            conn.execute(sa.text(f"UPDATE {table_name} SET {name_col} = :name WHERE {id_col} = :id"), {"name": cleaned_name, "id": target_id})
            updates += 1
            
        # 2. Fusionar duplicados (otros items en el grupo)
        for other_item in items[1:]:
            other_id = other_item['id']
            safe_print(f"  [MERGE] ID {other_id} ('{other_item['original']}') -> ID {target_id} ('{cleaned_name}')")
            
            # Reasignar FKs en tablas referenciadas
            for ref_table, ref_col in ref_tables:
                safe_print(f"    -> Actualizando referencias en {ref_table}...")
                conn.execute(sa.text(f"UPDATE {ref_table} SET {ref_col} = :target_id WHERE {ref_col} = :other_id"), {"target_id": target_id, "other_id": other_id})
            
            # Borrar el registro antiguo
            safe_print(f"    -> Borrando ID {other_id} de {table_name}")
            conn.execute(sa.text(f"DELETE FROM {table_name} WHERE {id_col} = :id"), {"id": other_id})
            merges += 1

    safe_print(f"Resumen {table_name}: {updates} actualizaciones, {merges} fusiones.")

def clean_products_names(conn):
    safe_print("\n--- Procesando tabla: productos (nombres) ---")
    query = sa.text("SELECT id_producto, nombre_producto FROM productos")
    rows = conn.execute(query).fetchall()
    
    updates = 0
    for row in rows:
        pid = row.id_producto
        original = row.nombre_producto
        if not original: continue
        
        cleaned = clean_text(original)
        if cleaned != original:
            # safe_print(f"  [PROD] ID {pid}: '{original}' -> '{cleaned}'") # Verbose
            conn.execute(sa.text("UPDATE productos SET nombre_producto = :name WHERE id_producto = :id"), {"name": cleaned, "id": pid})
            updates += 1
    safe_print(f"Resumen productos: {updates} nombres limpiados.")

def main():
    try:
        engine = sa.create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # Desactivar constraints temporales si fuera necesario, pero mejor manejarlo via lógica
            # Iniciamos transacción
            trans = conn.begin() # Start transaction explicitly or use context manager? 
            # engine.connect() gives a connection. .begin() starts a transaction.
            # However, connection autocommit behavior depends on driver. SQLAlchemy usually starts transaction automatically.
            # But let's be explicit.
            
            try:
                # 1. Marcas
                # Referenciada por: productos.id_marca
                clean_table(conn, "marcas", "id_marca", "nombre_marca", [("productos", "id_marca")])
                
                # 2. Categorias
                # Referenciada por: subcategorias.id_categoria
                clean_table(conn, "categorias", "id_categoria", "nombre_categoria", [("subcategorias", "id_categoria")])
                
                # 3. Subcategorias
                # Referenciada por: productos.id_subcategoria
                clean_table(conn, "subcategorias", "id_subcategoria", "nombre_subcategoria", [("productos", "id_subcategoria")])
                
                # 4. Tiendas
                # Referenciada por: producto_tienda.id_tienda
                clean_table(conn, "tiendas", "id_tienda", "nombre_tienda", [("producto_tienda", "id_tienda")])
                
                # 5. Nombres de Productos (sin merge, solo clean)
                clean_products_names(conn)
                
                trans.commit()
                safe_print("\n[SUCCESS] Limpieza de base de datos completada.")
                
            except Exception as e:
                trans.rollback()
                safe_print(f"\n[ERROR] Rollback ejecutado: {e}")
                # Re-raise to see full traceback if needed, or just exit
                # raise e
                
    except Exception as e:
        safe_print(f"Error conexión: {e}")

if __name__ == "__main__":
    main()
