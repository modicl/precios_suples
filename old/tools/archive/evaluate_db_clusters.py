import os
import sqlalchemy as sa
from rapidfuzz import process, fuzz
from rich import print
from rich.table import Table
from rich.console import Console
from dotenv import load_dotenv
import re
import sys

# Import functions directly from normalize_products
# Using sys.path to ensure we can import from sibling directory if needed
sys.path.append(os.getcwd())
try:
    from data_processing.normalize_products import (
        extract_sizes, detect_packaging, extract_pack_quantity, 
        extract_flavors, check_critical_mismatch, check_percentage_mismatch
    )
except ImportError as e:
    print(f"[red]Error: No se pudo importar data_processing.normalize_products. Detalles: {e}[/red]")
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

def evaluate_clusters():
    engine = get_db_connection()
    console.print(f"[bold blue]Conectando a BD: {DB_NAME}...[/bold blue]")
    
    with engine.connect() as conn:
        # 1. Load Data
        query = sa.text("""
            SELECT 
                p.id_producto, 
                p.nombre_producto, 
                p.id_marca, 
                p.id_subcategoria,
                m.nombre_marca,
                s.nombre_subcategoria
            FROM productos p
            LEFT JOIN marcas m ON p.id_marca = m.id_marca
            LEFT JOIN subcategorias s ON p.id_subcategoria = s.id_subcategoria
            WHERE p.nombre_producto IS NOT NULL
        """)
        rows = conn.execute(query).fetchall()
        
    products = [dict(row._mapping) for row in rows]
    total_products = len(products)
    console.print(f"[green]Cargados {total_products} productos.[/green]")
    
    # Group by Brand + Subcategory to reduce search space
    groups = {}
    for p in products:
        # Use IDs for grouping key to be exact
        key = (p['id_marca'], p['id_subcategoria'])
        if key not in groups:
            groups[key] = []
        groups[key].append(p)
        
    clusters = [] # List of lists of product dicts
    
    processed_count = 0
    
    console.print("[bold cyan]Iniciando Clustering...[/bold cyan]")
    
    for key, group_products in groups.items():
        if len(group_products) < 2:
            processed_count += len(group_products)
            continue
            
        # Local clustering within the group
        # normalized_reps: list of representative product dicts
        normalized_reps = [] 
        # cluster_map: rep_name -> list of product dicts
        cluster_map = {}
        
        # We need a clean list of rep names for rapidfuzz
        rep_names = []
        
        for product in group_products:
            name = product['nombre_producto']
            clean_name = name.lower().strip()
            
            # Extract features
            sizes_candidate = extract_sizes(name)
            pack_candidate = detect_packaging(name)
            Nx_candidate = extract_pack_quantity(name)
            flavors_candidate = extract_flavors(name)
            
            # Find match among existing representatives in this group
            # Note: normalized_reps contains FULL product dicts, but we match against names
            
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
                    candidate_rep_prod = normalized_reps[idx] # Get the full product dict
                    candidate_rep_name = candidate_rep_prod['nombre_producto']
                    
                    # 1. Critical Keywords
                    if check_critical_mismatch(name, candidate_rep_name):
                        continue
                    
                    # 1.1 Percentage
                    if check_percentage_mismatch(name, candidate_rep_name):
                        continue
                        
                    # 2. Pack Quantity
                    Nx_rep = extract_pack_quantity(candidate_rep_name)
                    if Nx_candidate != Nx_rep:
                        continue
                        
                    # 3. Sizes
                    sizes_rep = extract_sizes(candidate_rep_name)
                    if sizes_candidate != sizes_rep:
                        continue
                        
                    # 4. Packaging
                    pack_rep = detect_packaging(candidate_rep_name)
                    if pack_candidate != pack_rep:
                        continue
                        
                    # 5. Flavors
                    flavors_rep = extract_flavors(candidate_rep_name)
                    if flavors_candidate != flavors_rep:
                        continue
                        
                    # Brand Check? We are already grouping by Brand ID, so implicitly same brand.
                    # Unless Brand ID is None?
                    # If ID is None, we might have mixed brands?
                    # But the group key handles that.
                    
                    best_match_idx = idx
                    break
            
            if best_match_idx != -1:
                # Add to existing cluster
                rep_prod = normalized_reps[best_match_idx]
                rep_key = rep_prod['id_producto'] # Use ID as unique key for map
                if rep_key not in cluster_map:
                    cluster_map[rep_key] = [rep_prod]
                cluster_map[rep_key].append(product)
            else:
                # Create new cluster rep
                normalized_reps.append(product)
                rep_names.append(clean_name)
                # Initialize list in map just in case it becomes a cluster later
                # actually, we only care about clusters > 1
                cluster_map[product['id_producto']] = [product]
        
        # Collect valid clusters
        for rep_id, members in cluster_map.items():
            if len(members) > 1:
                clusters.append(members)
        
        processed_count += len(group_products)
        if processed_count % 500 == 0:
            print(f"  Procesado {processed_count}/{total_products}...")

    # Reporting
    console.print("\n[bold green]--- Reporte de Clusters Encontrados ---[/bold green]")
    
    table = Table(title="Clusters Propuestos (Muestra)")
    table.add_column("ID Rep", style="cyan", no_wrap=True)
    table.add_column("Producto Representante", style="magenta")
    table.add_column("Duplicados (IDs)", style="white")
    table.add_column("Total", justify="right")
    
    total_clusters = len(clusters)
    products_to_merge = 0
    
    for cluster in clusters:
        # Sort by ID to pick a consistent "representative" (lowest ID usually implies older)
        cluster.sort(key=lambda x: x['id_producto'])
        rep = cluster[0]
        dupes = cluster[1:]
        products_to_merge += len(dupes)
        
        dupe_ids = ", ".join([str(d['id_producto']) for d in dupes])
        # Show first 5 names of dupes if verbose? Just IDs for compact table
        
        table.add_row(
            str(rep['id_producto']),
            rep['nombre_producto'],
            dupe_ids,
            str(len(cluster))
        )

    console.print(table)
    
    # Stats
    reduction = 0
    if total_products > 0:
        reduction = (products_to_merge / total_products) * 100
        
    console.print(f"\n[bold]Estadísticas Finales:[/bold]")
    console.print(f"Total Productos Analizados: {total_products}")
    console.print(f"Clusters Encontrados: {total_clusters}")
    console.print(f"Productos a Eliminar (Fusión): {products_to_merge}")
    console.print(f"Reducción Potencial: [green]{reduction:.2f}%[/green]")

if __name__ == "__main__":
    evaluate_clusters()
