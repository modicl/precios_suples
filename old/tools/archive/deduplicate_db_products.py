import os
import sys
import sqlalchemy as sa
from sqlalchemy.sql import text
from rapidfuzz import process, fuzz
from rich import print
from rich.console import Console
from dotenv import load_dotenv
import argparse

# Import functions directly from normalize_products
sys.path.append(os.getcwd())
try:
    from data_processing.normalize_products import (
        extract_sizes, detect_packaging, extract_pack_quantity, 
        extract_flavors, check_critical_mismatch, check_percentage_mismatch
    )
except ImportError:
    print("[red]Error: No se pudo importar data_processing.normalize_products. Verifica estar en la raíz del proyecto.[/red]")
    sys.exit(1)

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "suplementos")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "password")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

console = Console()

def get_db_connection():
    return sa.create_engine(DATABASE_URL)

def find_clusters(conn):
    """
    Reuses logic from evaluate_db_clusters to find clusters.
    Returns a list of clusters, where each cluster is a list of product dicts.
    """
    console.print("[blue]Buscando clusters de productos...[/blue]")
    query = text("""
        SELECT 
            p.id_producto, 
            p.nombre_producto, 
            p.id_marca, 
            p.id_subcategoria
        FROM productos p
        WHERE p.nombre_producto IS NOT NULL
    """)
    rows = conn.execute(query).fetchall()
    products = [dict(row._mapping) for row in rows]
    
    # Group by Brand + Subcategory
    groups = {}
    for p in products:
        key = (p['id_marca'], p['id_subcategoria'])
        if key not in groups:
            groups[key] = []
        groups[key].append(p)
        
    clusters = []
    
    for key, group_products in groups.items():
        if len(group_products) < 2:
            continue
            
        normalized_reps = [] 
        cluster_map = {} # rep_id -> list of products
        rep_names = []
        
        for product in group_products:
            name = product['nombre_producto']
            clean_name = name.lower().strip()
            
            # Extract features
            sizes_candidate = extract_sizes(name)
            pack_candidate = detect_packaging(name)
            Nx_candidate = extract_pack_quantity(name)
            flavors_candidate = extract_flavors(name)
            
            best_match_idx = -1
            
            if rep_names:
                matches = process.extract(
                    clean_name, 
                    rep_names, 
                    scorer=fuzz.token_sort_ratio, 
                    limit=5, 
                    score_cutoff=87
                )
                
                for match_tuple in matches:
                    candidate_name, score, idx = match_tuple
                    candidate_rep_prod = normalized_reps[idx]
                    candidate_rep_name = candidate_rep_prod['nombre_producto']
                    
                    if check_critical_mismatch(name, candidate_rep_name): continue
                    if check_percentage_mismatch(name, candidate_rep_name): continue
                    
                    Nx_rep = extract_pack_quantity(candidate_rep_name)
                    if Nx_candidate != Nx_rep: continue
                        
                    sizes_rep = extract_sizes(candidate_rep_name)
                    if sizes_candidate != sizes_rep: continue
                        
                    pack_rep = detect_packaging(candidate_rep_name)
                    if pack_candidate != pack_rep: continue
                        
                    flavors_rep = extract_flavors(candidate_rep_name)
                    if flavors_candidate != flavors_rep: continue
                        
                    best_match_idx = idx
                    break
            
            if best_match_idx != -1:
                rep_prod = normalized_reps[best_match_idx]
                rep_id = rep_prod['id_producto']
                if rep_id not in cluster_map:
                    cluster_map[rep_id] = [rep_prod]
                cluster_map[rep_id].append(product)
            else:
                normalized_reps.append(product)
                rep_names.append(clean_name)
                cluster_map[product['id_producto']] = [product]
        
        for rep_id, members in cluster_map.items():
            if len(members) > 1:
                clusters.append(members)
                
    return clusters

def merge_cluster(conn, cluster, dry_run=True):
    """
    Merges a cluster of products into a single master product.
    cluster: list of product dicts
    """
    # 1. Choose Master (Lowest ID)
    cluster.sort(key=lambda x: x['id_producto'])
    master = cluster[0]
    duplicates = cluster[1:]
    
    master_id = master['id_producto']
    
    console.print(f"[bold white]Procesando Cluster Master ID {master_id} ({master['nombre_producto']})[/bold white]")
    if dry_run:
        console.print(f"  [yellow][DRY-RUN][/yellow] Se fusionarían {len(duplicates)} duplicados: {[d['id_producto'] for d in duplicates]}")
        return

    # Transaction is handled by caller or context
    
    for dup in duplicates:
        dup_id = dup['id_producto']
        console.print(f"  [cyan]Fusionando ID {dup_id} -> Master {master_id}[/cyan]")
        
        # 2. Handle producto_tienda (PT)
        # Get PT entries for duplicate
        pt_dup_rows = conn.execute(text("SELECT id_producto_tienda, id_tienda FROM producto_tienda WHERE id_producto = :pid"), {"pid": dup_id}).fetchall()
        
        for pt_dup in pt_dup_rows:
            pt_dup_id = pt_dup.id_producto_tienda
            tienda_id = pt_dup.id_tienda
            
            # Check if Master already has entry for this store
            pt_master_row = conn.execute(
                text("SELECT id_producto_tienda FROM producto_tienda WHERE id_producto = :mid AND id_tienda = :tid"),
                {"mid": master_id, "tid": tienda_id}
            ).fetchone()
            
            if pt_master_row:
                # Case B: Conflict. Master already has this store.
                pt_master_id = pt_master_row.id_producto_tienda
                console.print(f"    -> Conflicto tienda {tienda_id}: Moviendo historial de PT {pt_dup_id} a PT {pt_master_id}")
                
                # Move history intelligently to avoid Unique Constraint violations (if (id_pt, date) is unique)
                # 1. Update records that don't conflict on date
                conn.execute(text("""
                    UPDATE historia_precios 
                    SET id_producto_tienda = :new_ptid 
                    WHERE id_producto_tienda = :old_ptid 
                    AND fecha_precio NOT IN (
                        SELECT fecha_precio 
                        FROM historia_precios 
                        WHERE id_producto_tienda = :new_ptid
                    )
                """), {"new_ptid": pt_master_id, "old_ptid": pt_dup_id})
                
                # 2. Delete remaining records in old_ptid (duplicates/conflicts)
                conn.execute(
                    text("DELETE FROM historia_precios WHERE id_producto_tienda = :old_ptid"),
                    {"old_ptid": pt_dup_id}
                )
                
                # Delete duplicate PT
                conn.execute(text("DELETE FROM producto_tienda WHERE id_producto_tienda = :ptid"), {"ptid": pt_dup_id})
                
            else:
                # Case A: No conflict. Just reassign product ID.
                console.print(f"    -> Reasignando PT {pt_dup_id} a Master {master_id}")
                conn.execute(
                    text("UPDATE producto_tienda SET id_producto = :mid WHERE id_producto_tienda = :ptid"),
                    {"mid": master_id, "ptid": pt_dup_id}
                )
                
        # 3. Handle other FKs (click_analytics) if they exist
        # Schema inspection showed click_analytics(id_producto)
        conn.execute(text("UPDATE click_analytics SET id_producto = :mid WHERE id_producto = :did"), {"mid": master_id, "did": dup_id})
        
        # 4. Delete Duplicate Product
        console.print(f"    -> Eliminando producto ID {dup_id}")
        conn.execute(text("DELETE FROM productos WHERE id_producto = :pid"), {"pid": dup_id})

def run_deduplication(dry_run=True):
    engine = get_db_connection()
    console.print(f"[bold red]Iniciando Deduplicación (DRY-RUN={dry_run})[/bold red]")
    
    with engine.connect() as conn:
        clusters = find_clusters(conn)
        console.print(f"[green]Total Clusters a procesar: {len(clusters)}[/green]")
        
        if not clusters:
            console.print("No hay duplicados para procesar.")
            return

        # Start Transaction
        # First rollback any implicit transaction from find_clusters (read-only)
        conn.rollback()
        
        trans = conn.begin()
        
        try:
            for cluster in clusters:
                merge_cluster(conn, cluster, dry_run=dry_run)
                
            if dry_run:
                console.print("[yellow]Modo Dry-Run: Rollback automático (no se aplicaron cambios).[/yellow]")
                trans.rollback()
            else:
                console.print("[bold green]Aplicando cambios (COMMIT)...[/bold green]")
                trans.commit()
                console.print("[bold green]Deduplicación completada con éxito.[/bold green]")
                
        except Exception as e:
            console.print(f"[bold red]Error Crítico: {e}. Haciendo Rollback.[/bold red]")
            trans.rollback()
            raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deduplicar productos en base de datos")
    parser.add_argument("--execute", action="store_true", help="Ejecutar cambios reales en la BD (sin flag es dry-run)")
    args = parser.parse_args()
    
    # Dry run is True by default unless --execute is passed
    is_dry_run = not args.execute
    
    run_deduplication(dry_run=is_dry_run)
