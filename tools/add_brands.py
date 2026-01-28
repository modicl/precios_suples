import csv
import os

def add_missing_brands():
    file_path = 'marcas_dictionary.csv'
    
    # Marcas identificadas como faltantes en el análisis
    new_brands = [
        "Activlab",
        "Perfect Nutrition",
        "Amix",
        "PVL",
        "FNL",
        "Natura Vit",
        "Revitta",
        "Wild Protein",
        "Wild Fit",
        "FitSupps",
        "Naturalist",
        "Crevet",
        "Dokal",
        "Suerox",
        "Veggimilk",
        "Infor", # INFOR PRO BABY...
        "Regenesis", # REGENESIS MAX
        "Cerolip",
        "Quemalip",
        "Sinapetit",
        "Varinat",
        "Oxiplus",
        "Nerelax",
        "Broxul",
        "Chlorella Purity", # Parece marca o linea
        "Spirulina Wellness",
        "Vitakron",
        "X-Gear",
        "Innovative Fit", # Asegurar que esté
        "Your Protein", # "YOUR PROTEIN WHEY"
        "Underfive", # "Underfive Protein Bar"
        "Space Protein", # "Barra Space Protein"
        "Milkii", # "Milkii Protein Bar"
        "Nutra Go", # Posible en barras
        "BioTechUSA" # Vi uno de Biotech
    ]

    # Leer marcas existentes para no duplicar
    existing_brands = set()
    if os.path.exists(file_path):
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                brand = row.get('nombre_marca')
                if brand:
                    existing_brands.add(brand.strip().lower())
    
    # Determinar último ID
    last_id = 0
    if os.path.exists(file_path):
        with open(file_path, mode='r', encoding='utf-8') as f:
            # Leer última línea para obtener ID
            lines = f.readlines()
            if len(lines) > 1:
                last_line = lines[-1].strip()
                if ',' in last_line:
                    try:
                        last_id = int(last_line.split(',')[0])
                    except:
                        pass # Header o error

    # Añadir nuevas
    added_count = 0
    with open(file_path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for brand in new_brands:
            if brand.lower() not in existing_brands:
                last_id += 1
                writer.writerow([last_id, brand])
                existing_brands.add(brand.lower())
                added_count += 1
                print(f"Añadida: {brand}")
            else:
                print(f"Ya existe: {brand}")

    print(f"\nProceso completado. Se añadieron {added_count} marcas nuevas.")

if __name__ == "__main__":
    add_missing_brands()
