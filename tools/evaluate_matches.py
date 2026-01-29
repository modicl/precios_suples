import pandas as pd
import os
import glob
from rich import print

def evaluate_matches():
    processed_dir = "processed_data"
    # fuzzy_dir = os.path.join(processed_dir, "fuzzy_matched")
    # inserted_dir = os.path.join(processed_dir, "inserted_data")
    
    # files = glob.glob(os.path.join(inserted_dir, "normalized_products_*.csv"))
    
    target_dir = os.path.join(processed_dir, "fuzzy_matched")
    files = glob.glob(os.path.join(target_dir, "normalized_products_*.csv"))
    if not files:
        print("[red]No hay archivos normalizados para evaluar.[/red]")
        return
        
    files.sort()
    latest = files[-1]
    print(f"[blue]Evaluando: {os.path.basename(latest)}[/blue]")
    
    df = pd.read_csv(latest)
    
    if 'normalized_name' not in df.columns:
        print("[red]El archivo no tiene la columna 'normalized_name'.[/red]")
        return

    # Group by normalized_name and get list of unique original product_names
    # We only care about names, so let's simplify
    grouped = df.groupby('normalized_name')['product_name'].unique()
    
    total_clusters = len(grouped)
    merged_clusters = [ (name, originals) for name, originals in grouped.items() if len(originals) > 1 ]
    
    print(f"\n[bold]Estadísticas:[/bold]")
    print(f"  Total de Productos (filas): {len(df)}")
    print(f"  Nombres Originales Únicos: {df['product_name'].nunique()}")
    print(f"  Nombres Normalizados (Clusters): {total_clusters}")
    print(f"  Clusters fusionados (más de 1 nombre original): {len(merged_clusters)}")
    print(f"  Reducción: {100 - (total_clusters / df['product_name'].nunique() * 100):.2f}%")
    
    if merged_clusters:
        print(f"\n[bold green]Detalle de fusiones ({len(merged_clusters)}):[/bold green]")
        for normalized, originals in merged_clusters:
            print(f"\n[yellow]Cluster: '{normalized}'[/yellow]")
            for orig in originals:
                # Find sites and brands for this original name
                rows = df[df['product_name'] == orig]
                sites = rows['site_name'].unique()
                brands = rows['brand'].unique()
                
                sites_str = ", ".join(sites)
                brands_str = ", ".join([str(b) for b in brands])
                
                print(f"  - '{orig}' ({sites_str}) [Brand: {brands_str}]")
    else:
        print("\n[yellow]No se encontraron fusiones. Todos los productos tienen nombres únicos o el umbral es muy estricto.[/yellow]")

if __name__ == "__main__":
    evaluate_matches()
