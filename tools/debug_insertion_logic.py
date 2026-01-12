import pandas as pd
import sqlalchemy as sa
import os

# Mock the database state or use logic to print what would happen
def debug_logic():
    print("Iniciando debug de logica de insercion...")
    df = pd.read_csv("processed_data/fuzzy_matched/normalized_products_2026-01-12.csv")
    
    # Filter for Foodtech related rows
    term = "Foodtech"
    subset = df[df['product_name'].str.contains(term, case=False, na=False) | df['normalized_name'].str.contains(term, case=False, na=False)]
    
    print(f"Filas encontradas en CSV para '{term}': {len(subset)}")
    print(subset[['site_name', 'product_name', 'normalized_name', 'price']].to_string())
    
    # Simulate Step 5: Productos Insertion
    print("\n--- Simulacion Insercion Productos ---")
    
    # Mock existing DB products (Assume empty initially since valid clear)
    productos_existentes = set() 
    productos_simulados = {} # name -> id
    next_id = 1
    
    for index, row in subset.iterrows():
        nombre = row["product_name"]
        
        # In actual script, we use (nombre, id_marca, id_sub) as key for uniqueness
        # Here we simplify to Name for debug
        
        if nombre not in productos_simulados:
            productos_simulados[nombre] = next_id
            print(f"Insertando nuevo producto ID {next_id}: '{nombre}'")
            next_id += 1
        else:
            print(f"Producto '{nombre}' ya existe con ID {productos_simulados[nombre]}")

    # Simulate Step 6: Link Creation
    print("\n--- Simulacion Producto-Tienda ---")
    tienda_ids = {
        "SupleTech": 1,
        "AllNutrition": 2,
        "Suples.cl": 3,
        "OneNutrition": 4,
        "Strongest": 5,
        "ChileSuplementos": 6,
        "SupleStore": 7
    }
    
    for index, row in subset.iterrows():
        prod_name = row["product_name"]
        site = row["site_name"]
        price = row["price"]
        
        id_prod = productos_simulados.get(prod_name)
        id_tienda = tienda_ids.get(site)
        
        print(f"Row: '{prod_name}' @ '{site}' ($ {price}) -> Link ID_PROD {id_prod} - ID_TIENDA {id_tienda}")

if __name__ == "__main__":
    debug_logic()
