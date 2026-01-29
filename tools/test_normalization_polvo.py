from rapidfuzz import process, fuzz
import re

def check_critical_mismatch(text1, text2):
    t1 = text1.lower()
    t2 = text2.lower()
    
    # EXISTING KEYWORDS COPIED FROM normalize_products.py
    keywords = [
        'vegan', 'deluxe', 'joy', 'milkii', 'wpc80', 'isolate', 
        'hydro', 'concentrate', 'bar', 'barrita', 'men', 'senior', 'd3', 'b12',
        'pro', 'creamy', 'crunchy', 'hero', 'super', 'dark', 'instant', 'intense',
        'gold', 'turbo', 'shock', 'storm', 'caffeine', 'cafeina', 'sabores',
        'fusili', 'filini', 'tubo', 'sachet', 'vivace', 'cremoso', 'intenso', 'dolce', 'top',
        # Variant specifics
        'sf', 'regular', 'atena', 'afrodita', 'sirena', 'scugnizzo', 'moka',
        # Bundles/Gifts
        'shaker', 'regalo', 'vaso', 'botella', 'mochila',
        # Gender specificity
        'woman', 'women', 'her', 'hers', 'female', 'mujer', 'ellos', 'ellas', 'hombre', 'hombres',
        # Forms
        'polvo', 'caps', 'capsulas', 'tabletas', 'comprimidos', 'softgel', 'liquido'
    ]
    
    for kw in keywords:
        pattern = r'\b' + re.escape(kw) + r'\b'
        in1 = bool(re.search(pattern, t1))
        in2 = bool(re.search(pattern, t2))
        if in1 != in2:
            return True, kw
            
    if ('sin ' in t1) != ('sin ' in t2):
        return True, "sin"
        
    return False, None

def cleaner(text):
    return text.lower().strip()

name1 = "Bicarbonato De Sodio Polvo 250 gr"
name2 = "Bicarbonato De Sodio 250 gr"

print(f"Comparando:\n1. '{name1}'\n2. '{name2}'")

# Check Fuzz Score
score = fuzz.token_sort_ratio(cleaner(name1), cleaner(name2))
print(f"Fuzz Score: {score}")

# Check Critical Mismatch
mismatch, kw = check_critical_mismatch(name1, name2)
print(f"Critical Mismatch: {mismatch} (Keyword: {kw})")

if score >= 87 and not mismatch:
    print("RESULTADO: SE AGRUPAN (Fail)")
else:
    print("RESULTADO: SE SEPARAN (Pass)")
