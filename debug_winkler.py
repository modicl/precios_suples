import csv
from rapidfuzz import process, fuzz, utils

def load_brand_dictionary(filepath):
    brands = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('nombre_marca'):
                    brands.append(row['nombre_marca'].strip())
    except Exception as e:
        print(f"Error loading dictionary: {e}")
    return brands

canonical_brands = load_brand_dictionary('marcas_dictionary.csv')
name = "WINKLER NUTRITION"

print(f"Total canonical brands: {len(canonical_brands)}")

match_result = process.extractOne(
    name, 
    canonical_brands, 
    scorer=fuzz.WRatio, 
    processor=utils.default_process
)

if match_result:
    best_match, score, _ = match_result
    print(f"Input: '{name}'")
    print(f"Match: '{best_match}'")
    print(f"Score: {score}")
else:
    print("No match found")
