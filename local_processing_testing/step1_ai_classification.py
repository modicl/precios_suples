import os
import sys
import pandas as pd
import glob
from datetime import datetime
import json
import requests
import sqlalchemy as sa
import time # Added import

# Add root to path to find tools
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets
from tools.categorizer import ProductCategorizer

def clean_text(text):
    if isinstance(text, str):
        return text.lower().strip()
    return ""

def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local' en db_multiconnect.")
        sys.exit(1)
    return sa.create_engine(local_target['url'])

def ai_categorization_step(df, engine):
    """
    Identifies products with unknown subcategories and classifies them in batches using Ollama.
    Updates the DataFrame in-place.
    Supports Resume from Checkpoint.
    """
    print("\n--- Fase AI: Clasificación por Lotes ---")
    
    checkpoint_file = os.path.join("local_processing_testing", "ai_checkpoint.csv")
    
    # 0. Check for checkpoint
    if os.path.exists(checkpoint_file):
        print(f"[RESUME] Encontrado checkpoint: {checkpoint_file}")
        try:
            # Load checkpoint into a new DF to merge/update
            df_checkpoint = pd.read_csv(checkpoint_file)
            
            # Update the main DF with values from checkpoint where available
            # Assuming row order is consistent because we load the same raw files in the same order
            if len(df_checkpoint) == len(df):
                print("[RESUME] Cargando datos clasificados previamente...")
                # We specifically want the 'subcategory' column from checkpoint
                df['subcategory'] = df_checkpoint['subcategory']
                if 'category' in df_checkpoint.columns:
                    df['category'] = df_checkpoint['category']
            else:
                print("[RESUME] Checkpoint size mismatch. Ignorando checkpoint (iniciando de cero).")
        except Exception as e:
            print(f"[RESUME] Error cargando checkpoint: {e}")

    # Initialize categorizer WITH AI enabled
    # Pass DB connection to load existing known categories
    categorizer = ProductCategorizer(db_connection=engine.connect(), enable_ai=True)
    
    # 1. Identify rows that need classification
    unknown_indices = []
    items_to_classify = [] 
    
    for idx, row in df.iterrows():
        sub_raw = str(row['subcategory'])
        sub_norm = clean_text(sub_raw)
        
        # Check if already known in DB
        if sub_norm not in categorizer.subcategories_map:
            # Prepare for batch
            unknown_indices.append(idx)
            items_to_classify.append({
                'original_index': idx,
                'product': row['product_name'],
                'brand': str(row['brand']), # Pass brand to Categorizer
                'context': sub_raw
            })
            
    total_unknown = len(items_to_classify)
    if total_unknown == 0:
        print("Todos los productos tienen subcategorías conocidas. Saltando AI.")
        return df

    print(f"Productos pendientes de clasificar por IA: {total_unknown}")
    
    # 2. Process in Batches
    # Configurar Batch Size y Sleep según Proveedor
    BATCH_SIZE = 50
    SLEEP_TIME = 0
    
    if hasattr(categorizer, 'provider') and categorizer.provider == "google":
        BATCH_SIZE = 100
        SLEEP_TIME = 4 # Seconds to respect 15 RPM limit (1 req / 4s)
        print(f"[Config] Provider: Google | Batch: {BATCH_SIZE} | Sleep: {SLEEP_TIME}s")
    else:
        print(f"[Config] Provider: Ollama/Default | Batch: {BATCH_SIZE}")
    
    for i in range(0, total_unknown, BATCH_SIZE):
        batch_items = items_to_classify[i:i + BATCH_SIZE]
        print(f"Procesando lote {i+1}-{min(i+BATCH_SIZE, total_unknown)}...")
        
        start_time = time.time()
        
        # Call Categorizer Batch
        tool_input = [{'product': item['product'], 'brand': item['brand'], 'context': item['context']} for item in batch_items]
        results = categorizer.classify_batch(tool_input)
        
        # Update DataFrame
        for j, res in enumerate(results):
            original_idx = batch_items[j]['original_index']
            if res:
                df.at[original_idx, 'subcategory'] = res.get('nombre_subcategoria', '')
                if 'nombre_categoria' in res:
                    df.at[original_idx, 'category'] = res['nombre_categoria']
                if 'ai_clean_name' in res and res['ai_clean_name']:
                    df.at[original_idx, 'ai_clean_name'] = res['ai_clean_name']
                else:
                    df.at[original_idx, 'ai_clean_name'] = df.at[original_idx, 'product_name']
        
        # Incremental Save (Checkpoint)
        try:
            df.to_csv(checkpoint_file, index=False)
        except Exception as e:
            print(f"  [Checkpoint] Error guardando backup: {e}")
            
        # Rate Limiting
        elapsed = time.time() - start_time
        if SLEEP_TIME > 0:
            wait = max(0, SLEEP_TIME - elapsed)
            if wait > 0:
                time.sleep(wait)

    print("--- Fin Fase AI ---\n")
    
    # POST-PROCESSING: Enforce Category Consistency
    # Ensure all rows have the correct Parent Category for their assigned Subcategory
    # This fixes any discrepancies from old checkpoints or manual scrapers
    print("Validando consistencia Categoría <-> Subcategoría...")
    count_fixed = 0
    for idx, row in df.iterrows():
        sub_name = str(row['subcategory'])
        clean_sub = clean_text(sub_name)
        
        if clean_sub in categorizer.subcategories_map:
            correct_cat = categorizer.subcategories_map[clean_sub]['nombre_categoria']
            current_cat = str(row['category'])
            
            if clean_text(current_cat) != clean_text(correct_cat):
                df.at[idx, 'category'] = correct_cat
                count_fixed += 1
                
    print(f"Corregidas {count_fixed} categorías para coincidir con la base de datos.")
    
    return df

def main():
    print("--- PASO 1: Clasificación con IA ---")
    
    # Removed local import of sa as it's already imported globally at the top
    
    # 1. Leer CSVs RAW
    raw_files = glob.glob("raw_data/*.csv")
    if not raw_files:
        print("No hay archivos CSV en raw_data/")
        return

    print(f"Encontrados {len(raw_files)} archivos raw.")
    dfs = []
    for f in raw_files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"Error leyendo {f}: {e}")
    
    if not dfs:
        return

    full_df = pd.concat(dfs, ignore_index=True)
    print(f"Total filas cargadas: {len(full_df)}")
    
    # 2. Connect DB (needed for Categorizer to know what exists)
    engine = get_local_engine()
    
    # 3. AI Categorization
    full_df = ai_categorization_step(full_df, engine)
    
    # 4. Save Result with Date
    today_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join("local_processing_testing", "data", "1_classified")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_path = os.path.join(output_dir, f"classified_{today_str}.csv")
    full_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"[EXITO] Datos clasificados guardados en: {output_path}")
    
    # Save a 'latest' copy inside the specific folder
    latest_path = os.path.join(output_dir, "latest_classified.csv")
    full_df.to_csv(latest_path, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    main()
