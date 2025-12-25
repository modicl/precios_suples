import pandas as pd
import os
import glob
import shutil
from rich import print
from datetime import datetime

def process_data():
    raw_dir = "raw_data"
    used_dir = os.path.join(raw_dir, "used")
    processed_dir = "processed_data"
    
    # Create directories if needed
    if not os.path.exists(used_dir):
        os.makedirs(used_dir)
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    all_data = []

    # 1. Load History (Last all_products_*.csv)
    # Find files matching all_products_*.csv
    history_files = glob.glob(os.path.join(processed_dir, "all_products_*.csv"))
    if history_files:
        history_files.sort() # Sort by name/date
        latest_history = history_files[-1]
        print(f"[blue]Cargando historial desde: {os.path.basename(latest_history)}[/blue]")
        try:
            df_history = pd.read_csv(latest_history)
            all_data.append(df_history)
        except Exception as e:
            print(f"[red]Error cargando historial: {e}[/red]")
    else:
        print("[yellow]No se encontró historial previo en processed_data.[/yellow]")

    # 2. Load New Raw Data
    raw_files = glob.glob(os.path.join(raw_dir, "*.csv"))
    print(f"[blue]Encontrados {len(raw_files)} archivos nuevos en {raw_dir}[/blue]")

    new_files_processed = []

    for f in raw_files:
        try:
            df = pd.read_csv(f)
            all_data.append(df)
            new_files_processed.append(f)
        except Exception as e:
            print(f"[red]Error leyendo archivo {f}: {e}[/red]")

    if not all_data:
        print("[red]No hay datos para procesar.[/red]")
        return

    # 3. Concatenate
    full_df = pd.concat(all_data, ignore_index=True)
    print(f"Total registros (Historial + Nuevos): {len(full_df)}")

    # 4. Standardize Date
    if 'date' in full_df.columns:
        full_df['date'] = pd.to_datetime(full_df['date'], errors='coerce')
        # Sort by date desc
        full_df = full_df.sort_values(by='date', ascending=False)
    
    # 5. Deduplicate
    # Policy: Keep unique (Link + Date). 
    # If same link appears multiple times for SAME date, keep first (latest processed).
    # If same link appears for DIFFERENT dates, keep BOTH (history).
    subset = ['link', 'date'] if 'link' in full_df.columns and 'date' in full_df.columns else None
    
    if subset:
        dedup_df = full_df.drop_duplicates(subset=subset, keep='first')
    else:
        # Fallback
        dedup_df = full_df.drop_duplicates(keep='first')

    print(f"Registros después de eliminar duplicados exactos (Link+Fecha): {len(dedup_df)}")

    # 6. Save Output
    today_str = datetime.now().strftime("%Y-%m-%d")
    output_file = os.path.join(processed_dir, f"all_products_{today_str}.csv")
    
    dedup_df.to_csv(output_file, index=False)
    print(f"[green]Archivo consolidado guardado en: [bold]{output_file}[/bold][/green]")

    # 7. Move raw files to 'used'
    print("[blue]Moviendo archivos procesados a raw_data/used...[/blue]")
    for f in new_files_processed:
        filename = os.path.basename(f)
        dest = os.path.join(used_dir, filename)
        try:
            # Overwrite if exists in used
            if os.path.exists(dest):
                os.remove(dest)
            shutil.move(f, dest)
            print(f"  -> Movido: {filename}")
        except Exception as e:
            print(f"[red]Error moviendo {filename}: {e}[/red]")

if __name__ == "__main__":
    process_data()
