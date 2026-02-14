import pandas as pd
import os
import re
from datetime import datetime

def load_brands(dict_path):
    if not os.path.exists(dict_path):
        print(f"[WARNING] No se encontró el diccionario de marcas en {dict_path}")
        return []
    
    try:
        df = pd.read_csv(dict_path)
        # Assuming column 'nombre_marca' exists based on user info
        if 'nombre_marca' in df.columns:
            brands = df['nombre_marca'].dropna().unique().tolist()
            # Sort by length descending to match longest first (e.g. "Muscle Tech" before "Muscle")
            brands.sort(key=len, reverse=True)
            return brands
    except Exception as e:
        print(f"[ERROR] Leyendo diccionario de marcas: {e}")
        return []
    return []

def clean_name_logic(name, brands_pattern):
    if not isinstance(name, str) or not name:
        return ""
    
    # Check for WILD brand BEFORE removing it
    is_wild = name.lower().startswith("wild")
    
    # 1. Remove Brands (Case Insensitive)
    cleaned = brands_pattern.sub('', name)
    
    # 2. Cleanup Punctuation and leftovers
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip(" -.,:;|/()%")
    cleaned = cleaned.replace(" - ", " ")
    
    # 3. Aggressive cleaning for WILD brands only
    # We apply this to 'cleaned' (the version without the brand string)
    # BUT wait, if the brand "Wild" was removed, 'cleaned' starts with "Protein..."
    # So we use the 'is_wild' flag detected earlier.
    
    if is_wild:
        # Remove generic "Format" words
        cleaned = re.sub(r'\b(Barra|Barrita)s? (de )?Prote[íi]nas?\b', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\b(Barra|Barrita)s?\b', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\bProtein Bars?\b', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\bOriginal\b', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\bSabor\b', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\bVegan[oa]s?\b', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\b\d+\s*g(r)?\b', '', cleaned, flags=re.IGNORECASE)
        
        # If the result became empty (e.g. "Wild Protein" -> remove "Wild" -> "Protein" -> remove "Protein"?)
        # Wait, "Wild Protein" -> remove "Wild" -> "Protein". "Protein" is not removed by above regexes unless "Protein Bars".
        # But if the brand dictionary has "Wild Protein", then "Wild Protein" is removed completely!
        # Ah, we removed "Wild Protein" from dictionary. Good.
        # So "Wild Protein" -> remove "WILD" -> " Protein" -> "Protein".
        # "Wild Protein Vegana" -> remove "WILD" -> " Protein Vegana" -> remove "Vegana" -> "Protein".
        # So they group under "Protein".
        
        # Re-add "Wild" prefix if it was stripped by brand removal but we want to keep identity?
        # User wants "Wild Protein", "Wild Protein Vegana" -> "Wild Protein".
        # If we remove "Wild" (brand), we are left with "Protein".
        # Is "Protein" a good product name? Maybe too generic?
        # But if all Wild products map to "Protein" and brand is "Wild Foods", then (Name="Protein", Brand="Wild Foods") is unique enough.
        pass

    # Remove ", wild" at the end (flexible spacing)
    cleaned = re.sub(r',\s*wild\s*$', '', cleaned, flags=re.IGNORECASE)
    
    # 4. Title Case
    # Check if 'cleaned' is empty/None to avoid errors
    if not cleaned:
        return ""
    cleaned = cleaned.title()
    
    return cleaned.strip()

def standardize_units(name):
    if not isinstance(name, str): return ""
    
    # Standardize weights/volumes
    # 5 lbs, 5lbs, 5 lb -> 5lb
    name = re.sub(r'\b(\d+(\.\d+)?)\s*(lbs?|libras?)\b', r'\1lb', name, flags=re.IGNORECASE)
    # 2 kg, 2kg -> 2kg
    name = re.sub(r'\b(\d+(\.\d+)?)\s*(kgs?|kilos?)\b', r'\1kg', name, flags=re.IGNORECASE)
    # 1000 mg -> 1000mg
    name = re.sub(r'\b(\d+)\s*(mg|mcg|g)\b', r'\1\2', name, flags=re.IGNORECASE)
    # 60 caps -> 60caps
    name = re.sub(r'\b(\d+)\s*(caps?|capsulas?|softgels?|tabletas?|servicios?|servs?)\b', r'\1\2', name, flags=re.IGNORECASE)
    
    # Normalize "Whey Protein" variations
    name = re.sub(r'\b(100%|100 %)\s*Whey\b', 'Whey', name, flags=re.IGNORECASE)
    
    # Extra cleaning for "Nutrition" leftovers
    name = re.sub(r'\b(Nutrition|Supplements|Labs?|Pharm|Pharma)\b', '', name, flags=re.IGNORECASE)
    
    # Clean Promotional/Bundle leftovers (Regalo, Gratis, Shaker)
    # Strategy: If "Regalo", "Gratis", "Shaker" is found, truncate everything after it?
    # Or just remove the word? Truncating is safer for " ... + Shaker Dymatize"
    
    # Remove "+ Shaker..." or " con Shaker..."
    name = re.sub(r'(\+|con|incluye)?\s*\b(Shaker|Vaso|Regalo|Gratis)\b.*', '', name, flags=re.IGNORECASE)
    
    # Remove " (Unidad)" or " Unidad" at end
    name = re.sub(r'[\(\s]Unidad[\)]?$', '', name, flags=re.IGNORECASE)
    
    # NEW: Aggressive cleaning for WILD brands only
    # We check if the original brand is WILD-related (passed implicitly via brand dictionary logic? No, regex is generic)
    # But we can check if the name STARTS with "Wild" (since brand wasn't removed for WILD)
    
    if name.lower().startswith("wild"):
        # Remove generic "Format" words that break grouping
        # "Barra de proteina", "Protein Bar", "Barrita", "45 g", "Original", "Sabor"
        name = re.sub(r'\b(Barra|Barrita)s? (de )?Proteinas?\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\bProtein Bars?\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\bOriginal\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\bSabor\b', '', name, flags=re.IGNORECASE)
        # Remove weight for Wild Bars to group flavors regardless of "45g" vs "45 g"
        # Be careful not to remove "2lb" for proteins. Only small grams for bars?
        # Let's remove "45\s*g(r)?" specifically or generic grams if small?
        # Safer: "45\s*gr?"
        name = re.sub(r'\b45\s*gr?\b', '', name, flags=re.IGNORECASE)
    
    # 4. Stopword Guard (Prevent removing brand leaving only "Unidad", "Pack", "Caja")
    # If the remaining name is just a generic word, we probably stripped too much.
    stopwords = {"unidad", "unidades", "pack", "caja", "display", "promo", "oferta", "barra", "barras", "sachet"}
    if name.lower().strip() in stopwords:
        # Revert logic? We need original name.
        # But this function only takes 'name' (which is effectively 'cleaned' so far)
        # We can return None to signal "revert to original" in the caller.
        return None 

    return name.strip()

def process_cleaning():
    print("--- PASO 1: Limpieza Determinista de Nombres ---")
    
    # Determine Project Root (parent of 'local_processing_testing')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    
    # Read from raw_data (absolute path)
    raw_data_dir = os.path.join(project_root, "raw_data")
    import glob
    raw_files = glob.glob(os.path.join(raw_data_dir, "*.csv"))
    
    if not raw_files:
        print(f"Error: No hay archivos CSV en {raw_data_dir}")
        return

    print(f"Encontrados {len(raw_files)} archivos raw en {raw_data_dir}.")
    dfs = []
    for f in raw_files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"Error leyendo {f}: {e}")
    
    if not dfs: return
    df = pd.concat(dfs, ignore_index=True)

    # Load Data
    # df = pd.read_csv(input_csv) # Already loaded via concat
    print(f"Cargados {len(df)} registros.")
    
    # Load Brands
    brands_path = os.path.join(project_root, "marcas_dictionary.csv")
    brands = load_brands(brands_path)
    print(f"Cargadas {len(brands)} marcas para limpieza desde {brands_path}")
    
    # Compile Regex for Brands
    # We escape regex chars in brand names just in case
    # Pattern: \b(Brand1|Brand2|...)\b with ignore case
    if brands:
        escaped_brands = [re.escape(b) for b in brands]
        pattern_str = r'\b(' + '|'.join(escaped_brands) + r')\b'
        brands_pattern = re.compile(pattern_str, re.IGNORECASE)
    else:
        brands_pattern = re.compile(r'(?!x)x') # Match nothing
    
    # Apply Cleaning
    # We operate on 'ai_clean_name' if valid, else 'product_name'.
    # Result goes back to 'ai_clean_name' (or new column? Let's refine ai_clean_name)
    
    count_changed = 0
    
    def row_cleaner(row):
        nonlocal count_changed
        
        # Source: prefer AI cleaned name, fallback to original
        original_source = row.get('ai_clean_name', '')
        if pd.isna(original_source) or original_source == '':
            original_source = row['product_name']
            
        final_name = clean_name_logic(str(original_source), brands_pattern)
        if final_name is None: # Stopword guard triggered
            # Revert to original but Clean it (Title Case)
            final_name = str(original_source).title()
        
        # Apply unit standardization ALWAYS (to both cleaned and reverted names)
        final_name = standardize_units(final_name)
        
        # Check if changed (ignoring case for change detection)
        # Ensure final_name is string (it should be, but safety first)
        if final_name and str(final_name).lower() != str(original_source).lower():
            count_changed += 1
            
        return final_name

    df['ai_clean_name'] = df.apply(row_cleaner, axis=1)
    
    print(f"Nombres limpiados/modificados: {count_changed}")
    
    # Save Outputs
    today_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Create folder if not exists (though data exists)
    # output_dir is inside local_processing_testing/data/1_cleaned
    # We use current_dir (which is local_processing_testing)
    output_dir = os.path.join(current_dir, "data", "1_cleaned")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_path = os.path.join(output_dir, f"cleaned_{today_str}.csv")
    
    # Save 'latest' inside the specific folder
    latest_path = os.path.join(output_dir, "latest_cleaned.csv")
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    df.to_csv(latest_path, index=False, encoding='utf-8-sig')
    
    print(f"[EXITO] Datos limpios guardados en: {latest_path}")

if __name__ == "__main__":
    process_cleaning()
