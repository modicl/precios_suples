import sys
import os
import pandas as pd
from rich.console import Console
from rich.table import Table

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, 'scrapers'))

from scrapers.SuplementosMayoristasScraper import SuplementosMayoristasScraper

console = Console()

def analyze_results(csv_path):
    if not os.path.exists(csv_path):
        console.print(f"[red]No se encontró el archivo {csv_path}[/red]")
        return
    
    try:
        df = pd.read_csv(csv_path)
        total = len(df)
        
        # Check brand column
        if 'brand' not in df.columns:
            console.print("[red]Columna 'brand' no encontrada en el CSV.[/red]")
            return
            
        # Count "N/D" or empty
        nd_count = df[df['brand'].isin(['N/D', '', 'NaN', None]) | df['brand'].isna()].shape[0]
        found_count = total - nd_count
        
        # Get distribution of brands
        brand_counts = df['brand'].value_counts().reset_index()
        brand_counts.columns = ['Brand', 'Count']
        
        console.print(f"\n[bold]Resumen de Detección de Marcas[/bold]")
        console.print(f"Total Productos: {total}")
        console.print(f"Marcas Detectadas: [green]{found_count}[/green]")
        console.print(f"Marcas No Detectadas (N/D): [red]{nd_count}[/red]")
        
        table = Table(title="Top 20 Marcas Detectadas")
        table.add_column("Marca", style="cyan")
        table.add_column("Cantidad", justify="right", style="magenta")
        
        for _, row in brand_counts.head(20).iterrows():
            table.add_row(str(row['Brand']), str(row['Count']))
            
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error analizando CSV: {e}[/red]")

if __name__ == "__main__":
    # Configure to scrape a larger category: "Whey Protein"
    scraper = SuplementosMayoristasScraper(base_url="https://www.suplementosmayoristas.cl", headless=True)
    
    scraper.categories_config = {
        "Proteinas": [
             ("Whey Protein", "https://www.suplementosmayoristas.cl/proteinas/whey-protein")
        ]
    }
    
    # Run scraper
    # Note: The scraper writes to raw_data/productos_suplementosmayoristas_YYYY-MM-DD.csv
    # We need to capture that filename or predict it
    
    try:
        scraper.run()
        
        # Predict filename based on scraper logic
        from datetime import datetime
        csv_filename = f"productos_suplementosmayoristas_{datetime.now().strftime('%Y-%m-%d')}.csv"
        csv_path = os.path.join(project_root, "raw_data", csv_filename)
        
        analyze_results(csv_path)
        
    except KeyboardInterrupt:
        console.print("[yellow]Scraping interrumpido manualmente. Analizando lo que se haya guardado...[/yellow]")
        from datetime import datetime
        csv_filename = f"productos_suplementosmayoristas_{datetime.now().strftime('%Y-%m-%d')}.csv"
        csv_path = os.path.join(project_root, "raw_data", csv_filename)
        analyze_results(csv_path)
