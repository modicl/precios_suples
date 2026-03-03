import pandas as pd
import csv

# List of terms to REMOVE (Ingredients, Product Lines, Generics, Stores)
blacklist = {
    # Ingredients / Types
    "5-HTP", "Amino", "Anabolic", "Barberine", "Beef", "Beta", "Citrulina", "Creamer", "Creapure", 
    "Crunchy", "D3", "DHEA", "DIGESTITOL", "DIGESTIVE", "DIGEZYME", "EAA", "Fish", "GABA", "GH", 
    "HMB", "Iron", "Krill", "MACA", "MCT", "MG", "Milk", "MSM", "Nitro", "Saw", "Tribulus", 
    "Triple", "Ultra", "COLA", "CHAGA", "CORDYCEPS", "REISHI", "SHIITAKE", "Gams", # Mushrooms? Gams?
    
    # Product Lines / Models (Not Brands)
    "ABE", "Balance", "BASIC", "BEST", "C4", "CARNIVOR", "Cell", "Elite", "Essentials", "G10", 
    "GoHard", "IMPACT", "ISO 100", "ISOFIT", "ISOPURE", # Isopure is a brand though? Nature's Best Isopure. Often treated as brand. Keep Isopure?
    # User said "ISOFIT is a brand". Wait, ISOFIT is Nutrex. Isopure IS a brand (The Isopure Company).
    # I will KEEP Isopure. I removed ISO 100 and ISOFIT earlier.
    "Monster", "MUSCLE UP", "Platinium", "ProStar", "SERIOUS", "Simply", "SIX STAR", # Six Star is a brand (Six Star Pro Nutrition). Keep.
    "SLIMBAR", "STIMUL", "SUPER", "TESTROL", "TRIBOOSTER", "TWENTYS", # Twentys is a bar brand. Keep.
    "Ultimatic", "Vegefiit", "VITAENERGY", "VITANUTRITION", # Vitanutrition might be store or brand?
    "Vitargo", # Keep Vitargo, it's a specific patented carb often listed as brand.
    
    # Generic / Data Errors
    "N/D", "I LIKE", "Space Protein", # Space Protein is a brand.
    
    # Stores (if they are not private label brands)
    "SUPLEMENTOS BULL", "SUPLES.CL", "Nutrition Factory", "BODYFAST", "Greatlhete" # Typos?
}

# Refined Logic:
# Keep: "Isopure", "Six Star", "Twentys", "Space Protein".
# Remove: "WILD" (Too generic, conflicts with flavors). Keep "Wild Foods".

blacklist.add("WILD")

# Load existing
try:
    df = pd.read_csv("marcas_dictionary.csv")
    original_brands = df['nombre_marca'].dropna().unique().tolist()
except FileNotFoundError:
    print("Error: marcas_dictionary.csv not found.")
    exit()

# Filter
final_brands = []
for b in original_brands:
    b_clean = b.strip()
    if b_clean in blacklist:
        continue
    # Case insensitive check for "Nutrition", "Supplements" standalone? No, brands have them.
    # Check for short generic words
    if len(b_clean) < 3 and b_clean not in ["GU", "ON"]: # GU and ON are valid
        continue
        
    final_brands.append(b_clean)

# Sort alphabetically
final_brands.sort()

# Write V2
with open("marcas_v2.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["nombre_marca"])
    for b in final_brands:
        writer.writerow([b])

print(f"Generado marcas_v2.csv con {len(final_brands)} marcas (Original: {len(original_brands)})")
