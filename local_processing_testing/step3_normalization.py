import pandas as pd
import os
from rapidfuzz import process, fuzz
import re
from datetime import datetime

# Import normalization helpers
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_processing.normalize_products import (
    extract_sizes, 
    detect_packaging, 
    extract_pack_quantity, 
    extract_flavors, 
    check_critical_mismatch, 
    check_percentage_mismatch
)

def cleaner(text):
    if not isinstance(text, str): return ""
    return text.lower().strip()

def normalize_step(input_file, output_file):
    print(f"[blue]Cargando datos de: {input_file}[/blue]")
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"[red]Error leyendo {input_file}: {e}[/red]")
        return

    unique_names = df['product_name'].dropna().unique()
    mapping = {}
    normalized_names = [] 
    
    # Brand map for checking
    brand_map = df.drop_duplicates('product_name').set_index('product_name')['brand'].astype(str).to_dict()
    
    print("[blue]Iniciando normalización (Fuzzy Matching)...[/blue]")
    
    threshold = 87
    count = 0
    total = len(unique_names)
    
    for name in unique_names:
        count += 1
        if count % 100 == 0:
            print(f"  Procesando {count}/{total}...")
            
        # Use AI Clean Name if available in the row corresponding to this product name
        # We need to look up the ai_clean_name for this product_name
        # Since unique_names comes from df['product_name'], we can find one row.
        # Ideally, we should iterate over unique (product_name, ai_clean_name) tuples if they vary, 
        # but usually 1 product name = 1 clean name in this batch.
        
        # Optimization: Create a lookup map at start
        # clean_lookup = df.drop_duplicates('product_name').set_index('product_name')['ai_clean_name'].to_dict()
        # BUT wait, step2 is refactored below.
        pass

    # REFACTORING MAIN LOOP TO USE AI CLEAN NAME
    # We need to process unique combinations of (original_name, ai_clean_name)
    # Actually, we want to normalize the 'product_name' column using 'ai_clean_name' as the key feature.
    
    # Let's rebuild the logic slightly.
    
    # 1. Get unique rows of (product_name, ai_clean_name, brand)
    unique_products = df[['product_name', 'ai_clean_name', 'brand']].drop_duplicates()
    
    # If ai_clean_name is missing (NaN), fill with product_name
    unique_products['ai_clean_name'] = unique_products['ai_clean_name'].fillna(unique_products['product_name'])
    
    mapping = {} # product_name -> normalized_name (cleanest version)
    normalized_groups = [] # List of {clean_name, representative_original_name}
    
    count = 0
    total = len(unique_products)
    
    print("[blue]Iniciando clustering con IA Clean Names...[/blue]")

    for _, row in unique_products.iterrows():
        orig_name = row['product_name']
        ai_name = row['ai_clean_name']
        brand = str(row['brand'])
        
        count += 1
        if count % 100 == 0: print(f"  Procesando {count}/{total}...")

        # Comparison Logic uses AI NAME
        target_clean = cleaner(ai_name)
        
        # IMPROVEMENT: If normalized names are very short (e.g. "Iso 100"), 
        # TokenSortRatio might be too strict if one has "Hydrolyzed".
        # Try Partial Ratio for very high confidence matches?
        
        # Features extraction from AI Name (it's cleaner!)
        sizes_candidate = extract_sizes(ai_name) 
        pack_candidate = detect_packaging(ai_name)
        Nx_candidate = extract_pack_quantity(ai_name)
        flavors_candidate = extract_flavors(ai_name)
        
        # Match against existing groups
        # We need a list of strings to fuzz against. Using 'target_clean' of groups.
        candidates_pool = [g['clean_key'] for g in normalized_groups]
        
        matches = process.extract(
            target_clean, 
            candidates_pool, 
            scorer=fuzz.token_sort_ratio, 
            limit=5, 
            score_cutoff=threshold
        )
        
        best_match_rep = None
        
        for match_tuple in matches:
            # match_tuple = (matched_string, score, index)
            match_idx = match_tuple[2]
            candidate_group = normalized_groups[match_idx]
            candidate_ai_name = candidate_group['ai_name']
            
            # Critical Checks (using AI names for precision)
            if check_critical_mismatch(ai_name, candidate_ai_name): continue
            if check_percentage_mismatch(ai_name, candidate_ai_name): continue
            
            Nx_rep = extract_pack_quantity(candidate_ai_name)
            if Nx_candidate != Nx_rep: continue

            sizes_rep = extract_sizes(candidate_ai_name)
            if sizes_candidate != sizes_rep: continue

            pack_rep = detect_packaging(candidate_ai_name)
            if pack_candidate != pack_rep: continue
            
            flavors_rep = extract_flavors(candidate_ai_name)
            if flavors_candidate != flavors_rep: continue
            
            # Brand Check
            b1 = brand.lower()
            b2 = candidate_group['brand'].lower()
            invalid_brands = ["n/d", "nan", "none", ""]
            
            if b1 not in invalid_brands and b2 not in invalid_brands:
                if fuzz.ratio(b1, b2) < 85: continue
            
            best_match_rep = candidate_group['rep_name']
            break
        
        if best_match_rep:
            mapping[orig_name] = best_match_rep
        else:
            # New Group
            # We use the AI Clean Name as the "Normalized Name" because it's better!
            # Wait, user wants to group products. 
            # If we group "Gold Standard" and "100% Whey Gold", what name do we save?
            # Ideally the AI Clean Name "Gold Standard 100% Whey".
            
            # Let's map orig_name -> ai_name
            mapping[orig_name] = ai_name
            
            normalized_groups.append({
                'clean_key': target_clean,
                'ai_name': ai_name,
                'rep_name': ai_name, # This will be the value in 'normalized_name' column
                'brand': brand
            })

    print(f"Clustering terminado. {len(unique_products)} -> {len(normalized_groups)} grupos.")
    
    df['normalized_name'] = df['product_name'].map(mapping)
    
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"[green]Archivo normalizado guardado en: [bold]{output_file}[/bold][/green]")

def main():
    print("--- PASO 3: Normalización de Productos ---")
    
    # Use 'latest' pointer from Step 2 folder
    input_csv = os.path.join("local_processing_testing", "data", "2_cleaned", "latest_cleaned.csv")
    
    # Output logic
    today_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join("local_processing_testing", "data", "3_normalized")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_csv = os.path.join(output_dir, f"normalized_{today_str}.csv")
    
    # Also save a 'latest' pointer for Step 4
    latest_norm_path = os.path.join(output_dir, "latest_normalized.csv")
    
    if not os.path.exists(input_csv):
        print(f"Error: No se encontró {input_csv}. Ejecuta el Paso 2 primero.")
        return
        
    normalize_step(input_csv, output_csv)
    
    # Copy to latest
    try:
        df = pd.read_csv(output_csv)
        df.to_csv(latest_norm_path, index=False, encoding='utf-8-sig')
    except: pass

if __name__ == "__main__":
    from rich import print
    main()
