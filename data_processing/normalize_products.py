import pandas as pd
import os
import glob
from rapidfuzz import process, fuzz
from datetime import datetime
from rich import print
import re

def extract_sizes(text):
    """
    Extracts ALL size/quantity patterns found in the text.
    Returns a sorted tuple of strings like ('400 iu', '250 caps') to identify the set of measurements.
    """
    if not isinstance(text, str): return tuple()
    
    # Regex for common supplement units
    pattern = r'(\d+(?:[.,]\d+)?)\s*(lbs?|libras?|kgs?|kilos?|gr?|gramos?|oz|onzas?|tabs?|tabletas?|caps?|c[aá]psulas?|softgels?|soft|comprimidos?|servicios?|servs?|svs?|scoops?|sachets?|unid\.|unida?d?e?s?|mcg|mg|iu|ui|ml|l|litros?|ampollas?|amp|billions?|billones?|porciones?)'
    
    unit_map = {
        'lbs': 'lb', 'libra': 'lb', 'libras': 'lb',
        'kgs': 'kg', 'kilo': 'kg', 'kilos': 'kg',
        'gr': 'g', 'gramo': 'g', 'gramos': 'g',
        'oz': 'oz', 'onza': 'oz', 'onzas': 'oz',
        'tabs': 'tabs', 'tableta': 'tabs', 'tabletas': 'tabs',
        'caps': 'caps', 'cap': 'caps', 'cápsula': 'caps', 'cápsulas': 'caps', 'capsula': 'caps', 'capsulas': 'caps', 'softgel': 'caps', 'softgels': 'caps', 'soft': 'caps',
        'comprimido': 'tabs', 'comprimidos': 'tabs',
        'servicios': 'serv', 'servicio': 'serv', 'servs': 'serv', 'serv': 'serv', 'svs': 'serv', 'sv': 'serv',
        'scoops': 'scoop', 'scoop': 'scoop',
        'sachet': 'sachet', 'sachets': 'sachet',
        'unid.': 'unid', 'unid': 'unid', 'unidad': 'unid', 'unidades': 'unid', 'und': 'unid',
        'mcg': 'mcg', 'mg': 'mg',
        'iu': 'iu', 'ui': 'iu',
        'ml': 'ml', 'l': 'l', 'litro': 'l', 'litros': 'l',
        'ampollas': 'amp', 'ampolla': 'amp', 'amp': 'amp',
        'billion': 'billion', 'billions': 'billion', 'billon': 'billion', 'billones': 'billion',
        'porciones': 'serv', 'porcion': 'serv'
    }
    
    matches = re.findall(pattern, text.lower())
    found_sizes = set()
    
    for qty, unit_str in matches:
        qty = qty.replace(',', '.')
        # Normalize unit
        unit = unit_map.get(unit_str, unit_str.rstrip('s'))
        if unit_str in unit_map: unit = unit_map[unit_str]

        try:
            qty_val = float(qty)
            if qty_val.is_integer():
                qty = str(int(qty_val))
            else:
                qty = str(qty_val)
        except:
            pass
            
        found_sizes.add(f"{qty} {unit}")
        
    return tuple(sorted(list(found_sizes)))

def detect_packaging(text):
    if not isinstance(text, str): return None
    lower_text = text.lower()
    if 'caja ' in lower_text or lower_text.startswith('caja'): return 'caja'
    if 'display' in lower_text: return 'display'
    if 'bandeja' in lower_text: return 'bandeja'
    if 'pack ' in lower_text or lower_text.startswith('pack'): return 'pack'
    return None 

def extract_pack_quantity(text):
    if not isinstance(text, str): return None
    # Matches "5x" or "Pack 2" or "Pack de 2" or starting "2 "
    # Case: "5x" (existing)
    match_x = re.search(r'(?:^|\s)(\d+)\s*[xX]\s', text)
    if match_x:
        return match_x.group(1)

    # Case: Starting number indicating pack (e.g. "2 Mass Extreme")
    # Must be at start of string, followed by space, avoiding "100% ..."
    match_start = re.search(r'^(\d+)\s+(?!\d|%|lbs|kg|gr)', text)
    if match_start:
         val = int(match_start.group(1))
         if val > 1 and val < 10: # Safety: only treat small numbers as pack quantities
             return str(val)

    # Case: Starting number indicating pack (e.g. "2 Mass Extreme")
    # Must be at start of string, followed by space, avoiding "100% ..."
    match_start = re.search(r'^(\d+)\s+(?!\d|%|lbs|kg|gr)', text)
    if match_start:
         val = int(match_start.group(1))
         if val > 1 and val < 10: # Safety: only treat small numbers as pack quantities
             return str(val)
        
    match_pack = re.search(r'pack\s+(?:de\s+)?(\d+)', text.lower())
    if match_pack:
        return match_pack.group(1)
        
    return None

def extract_flavors(text):
    if not isinstance(text, str): return set()
    t = text.lower()
    
    flavor_keywords = [
        'chocolate', 'choco', 'vainilla', 'vanilla', 'frutilla', 'strawberry', 
        'mani', 'maní', 'peanut', 'coco', 'coconut', 'banana', 'platano', 'plátano',
        'brownie', 'fudge', 'cookies', 'cream', 'berry', 'berries', 'frutos', 'bosque',
        'mocka', 'mocha', 'cafe', 'café', 'coffee', 'caramel', 'caramelo', 'toffee',
        'orange', 'naranja', 'limon', 'limón', 'lemon', 'piña', 'pineapple',
        'mango', 'maracuya', 'passion', 'sandia', 'watermelon', 'uva', 'grape',
        'blue', 'razz', 'raspberry', 'frambuesa', 'manzana', 'apple', 'punch', 'fruit',
        'unflavored', 'sin sabor', 'neutro', 'natural', 'bitter',
        'cocada', 'blanco', 'white', 'nuts', 'power', 'crunch', 'crunchy', 'creamy'
    ]
    
    found_flavors = set()
    for f in flavor_keywords:
        if f in t:
            found_flavors.add(f)
            
    return found_flavors

def check_critical_mismatch(text1, text2):
    t1 = text1.lower()
    t2 = text2.lower()
    
    # Use word boundaries for stricter matching
    keywords = [
        'vegan', 'deluxe', 'joy', 'milkii', 'wpc80', 'isolate', 
        'hydro', 'concentrate', 'bar', 'barrita', 'men', 'senior', 'd3', 'b12',
        'pro', 'creamy', 'crunchy', 'hero', 'super', 'dark', 'instant', 'intense',
        'gold', 'turbo', 'shock', 'storm', 'caffeine', 'cafeina', 'sabores',
        'fusili', 'filini', 'tubo', 'sachet', 'vivace', 'cremoso', 'intenso', 'dolce', 'top',
        # Gender specificity
        'woman', 'women', 'her', 'hers', 'female', 'mujer'
    ]
    
    for kw in keywords:
        # Regex check: \bkeyword\b
        # Escape kw mainly for safety, though these are simple words
        pattern = r'\b' + re.escape(kw) + r'\b'
        
        in1 = bool(re.search(pattern, t1))
        in2 = bool(re.search(pattern, t2))
        
        if in1 != in2:
            return True 
            
    # Check for "con" vs "sin" mismatch (contextual)
    # E.g. "sin cafeina" vs "con cafeina"
    # Simple check: if one has "sin " and the other doesn't (and it's not part of a word like 'sincero' - covered by space)
    # or "con " vs missing "con ".
    
    # Actually, simpler: check if "sin " is present in one but not other.
    # Check for "con" vs "sin" mismatch (contextual)
    # E.g. "sin cafeina" vs "con cafeina"
    if ('sin ' in t1) != ('sin ' in t2):
        return True
        
    return False

def check_percentage_mismatch(text1, text2):
    # Extracts numbers followed by %
    # Returns True if mismatch found
    p1 = set(re.findall(r'(\d+)%', text1))
    p2 = set(re.findall(r'(\d+)%', text2))
    return p1 != p2

def normalize_names(threshold=87):
    processed_dir = "processed_data"
    output_dir = os.path.join(processed_dir, "fuzzy_matched")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    files = glob.glob(os.path.join(processed_dir, "all_products_*.csv"))
    if not files:
        print("[red]No hay archivo all_products_*.csv[/red]")
        return
    
    files.sort()
    latest_file = files[-1]
    print(f"[blue]Cargando datos de: {latest_file}[/blue]")
    
    df = pd.read_csv(latest_file)
    unique_names = df['product_name'].dropna().unique()
    
    mapping = {}
    normalized_names = [] 
    
    # Create brand lookup (taking the first brand found for each product name)
    # This assumes a product name doesn't change brand across sites (usually true)
    brand_map = df.drop_duplicates('product_name').set_index('product_name')['brand'].astype(str).to_dict()
    
    def cleaner(text):
        if not isinstance(text, str): return ""
        return text.lower().strip()

    print("[blue]Iniciando clustering AVANZADO (Multi-size + Boundary Check)...[/blue]")
    
    count = 0
    total = len(unique_names)
    
    for name in unique_names:
        count += 1
        if count % 100 == 0:
            print(f"  Procesando {count}/{total}...")
            
        clean_name = cleaner(name)
        sizes_candidate = extract_sizes(name) # Tuple of sizes
        pack_candidate = detect_packaging(name)
        Nx_candidate = extract_pack_quantity(name)
        flavors_candidate = extract_flavors(name)
        
        matches = process.extract(
            clean_name, 
            normalized_names, 
            scorer=fuzz.token_sort_ratio, 
            processor=cleaner,
            limit=5, 
            score_cutoff=threshold
        )
        
        best_match_name = None
        
        for match_tuple in matches:
            candidate_rep = match_tuple[0]
            
            # 1. Critical Keywords (Boundary Sensitive)
            if check_critical_mismatch(name, candidate_rep):
                continue

            # 1.1 Percentage Mismatch (70% vs 79%)
            if check_percentage_mismatch(name, candidate_rep):
                continue

            # 2. Pack Quantity (5x)
            Nx_rep = extract_pack_quantity(candidate_rep)
            if Nx_candidate != Nx_rep:
                continue

            # 3. Size Logic (SET Comparison)
            # If "400 iu, 250 soft" vs "400 iu, 200 soft"
            # Set1: {400 iu, 250 caps} | Set2: {400 iu, 200 caps}
            # Intersection: {400 iu}. Difference: {250 caps} vs {200 caps}
            # Rule: If BOTH have detected sizes, the SETS must match EXACTLY (or subset? No, exact for safety).
            # "Prostar 5lb" -> {5 lb}. "Prostar 5lb + Shaker" -> {5 lb}. Match OK.
            # "Prostar 5lb" -> {5 lb}. "Prostar 1lb" -> {1 lb}. Mismatch.
            
            sizes_rep = extract_sizes(candidate_rep)
            
            if sizes_candidate and sizes_rep:
                if sizes_candidate != sizes_rep: 
                    # They have different size configurations
                    continue

            # 4. Packaging Status
            pack_rep = detect_packaging(candidate_rep)
            if pack_candidate != pack_rep:
                continue
            
            # 5. Flavors
            flavors_rep = extract_flavors(candidate_rep)
            # 5. Flavors
            flavors_rep = extract_flavors(candidate_rep)
            if flavors_candidate != flavors_rep:
                continue
            
            # 6. Brand Strictness
            # If both have a valid brand, they MUST match.
            b1 = brand_map.get(name, "N/D").lower()
            b2 = brand_map.get(candidate_rep, "N/D").lower()
            
            invalid_brands = ["n/d", "nan", "none", ""]
            
            if b1 not in invalid_brands and b2 not in invalid_brands:
                # Use fuzzy match for brands to handle "Muscletech" vs "Muscle Tech"
                if fuzz.ratio(b1, b2) < 85:
                     continue
            
            
            best_match_name = candidate_rep
            break
        
        if best_match_name:
            mapping[name] = best_match_name
        else:
            normalized_names.append(name)
            mapping[name] = name

    print(f"Clustering terminado. {len(unique_names)} -> {len(normalized_names)}.")
    
    df['normalized_name'] = df['product_name'].map(mapping)
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    output_file = os.path.join(output_dir, f"normalized_products_{today_str}.csv")
    
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"[green]Archivo guardado: [bold]{output_file}[/bold][/green]")

if __name__ == "__main__":
    normalize_names()
