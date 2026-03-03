import os
import sys
import sqlalchemy as sa
from sqlalchemy import text

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from tools.db_multiconnect import get_targets

def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local'.")
        sys.exit(1)
    return sa.create_engine(local_target['url'])

def unify_brands():
    print("--- Unificando Marcas (WILD / WILD FOODS / WILD PROTEIN) ---")
    engine = get_local_engine()
    
    # 1. Definir Maestro y Esclavos
    # Master Name: WILD (o Wild Foods) -> Let's use 'WILD' based on user preference in dict
    master_name = "WILD"
    slaves_names = ["Wild Foods", "Wild Protein", "Wild Fit"]
    
    with engine.connect() as conn:
        # Get Master ID
        res = conn.execute(text("SELECT id_marca FROM marcas WHERE nombre_marca ILIKE :name"), {"name": master_name}).fetchone()
        if not res:
            print(f"Marca maestra '{master_name}' no encontrada. Creándola...")
            # Create master if not exists (unlikely given previous steps, but safe)
            conn.execute(text("INSERT INTO marcas (nombre_marca) VALUES (:name)"), {"name": master_name})
            conn.commit()
            res = conn.execute(text("SELECT id_marca FROM marcas WHERE nombre_marca ILIKE :name"), {"name": master_name}).fetchone()
            
        master_id = res.id_marca
        print(f"Master Brand: '{master_name}' (ID {master_id})")
        
        count_moved = 0
        
        for s_name in slaves_names:
            slave_res = conn.execute(text("SELECT id_marca FROM marcas WHERE nombre_marca ILIKE :name"), {"name": s_name}).fetchone()
            if slave_res:
                slave_id = slave_res.id_marca
                if slave_id != master_id:
                    print(f"Procesando slave '{s_name}' (ID {slave_id})...")
                    
                    # Iterate products of slave to handle collisions
                    slave_products = conn.execute(
                        text("SELECT id_producto, nombre_producto, id_subcategoria FROM productos WHERE id_marca = :sid"),
                        {"sid": slave_id}
                    ).fetchall()
                    
                    for p in slave_products:
                        pid = p.id_producto
                        pname = p.nombre_producto
                        subid = p.id_subcategoria
                        
                        # Check collision in Master
                        master_prod = conn.execute(
                            text("SELECT id_producto FROM productos WHERE id_marca = :mid AND nombre_producto = :name AND id_subcategoria = :sub"),
                            {"mid": master_id, "name": pname, "sub": subid}
                        ).fetchone()
                        
                        if master_prod:
                            # COLLISION: Merge Slave -> Master
                            master_pid = master_prod.id_producto
                            print(f"  Fusionando producto '{pname}': {pid} -> {master_pid}")
                            
                            # Move links (with conflict handling for links too)
                            # Links conflict on (id_producto, id_tienda)
                            slave_links = conn.execute(
                                text("SELECT id_producto_tienda, id_tienda FROM producto_tienda WHERE id_producto = :pid"),
                                {"pid": pid}
                            ).fetchall()
                            
                            for link in slave_links:
                                link_id = link.id_producto_tienda
                                tid = link.id_tienda
                                
                                # Check if master has this link
                                master_link = conn.execute(
                                    text("SELECT id_producto_tienda FROM producto_tienda WHERE id_producto = :pid AND id_tienda = :tid"),
                                    {"pid": master_pid, "tid": tid}
                                ).fetchone()
                                
                                if master_link:
                                    # Link Collision: Merge History -> Master Link, Delete Slave Link
                                    mlink_id = master_link.id_producto_tienda
                                    conn.execute(text("UPDATE historia_precios SET id_producto_tienda = :new WHERE id_producto_tienda = :old"), {"new": mlink_id, "old": link_id})
                                    conn.execute(text("DELETE FROM producto_tienda WHERE id_producto_tienda = :old"), {"old": link_id})
                                else:
                                    # No Link Collision: Move Link -> Master Product
                                    conn.execute(text("UPDATE producto_tienda SET id_producto = :new WHERE id_producto_tienda = :old"), {"new": master_pid, "old": link_id})
                            
                            # Move Analytics (clicks) to Master
                            conn.execute(text("UPDATE click_analytics SET id_producto = :new WHERE id_producto = :old"), {"new": master_pid, "old": pid})

                            # Finally delete slave product
                            conn.execute(text("DELETE FROM productos WHERE id_producto = :pid"), {"pid": pid})
                            count_moved += 1
                            
                        else:
                            # NO COLLISION: Just move product to Master Brand
                            conn.execute(text("UPDATE productos SET id_marca = :mid WHERE id_producto = :pid"), {"mid": master_id, "pid": pid})
                            count_moved += 1
                    
                    conn.commit()
                    
                    # Delete slave brand if empty
                    try:
                        conn.execute(text("DELETE FROM marcas WHERE id_marca = :sid"), {"sid": slave_id})
                        conn.commit()
                        print(f"  Marca eliminada: '{s_name}'")
                    except Exception as e:
                        print(f"  No se pudo eliminar marca '{s_name}': {e}")
            else:
                print(f"  Slave '{s_name}' no encontrado en BD. Skip.")

        print(f"Total productos unificados: {count_moved}")

if __name__ == "__main__":
    unify_brands()
