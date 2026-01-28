import csv
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_processing.brand_matcher import BrandMatcher

def run_test():
    matcher = BrandMatcher()
    
    # Lista de productos de prueba simulando casos reales de los scrapers
    test_products = [
        "Gold Standard 100% Whey 5lbs Optimum Nutrition",
        "Dymatize ISO 100 5 Lbs",
        "Cellucor C4 Original 30 Servings",
        "BPI Sports Best BCAA 30 Servings",
        "BPI-Sports Best Protein", # Caso con guion
        "Mutant Mass 15 Lbs",
        "Creatina Monohidrato 300g Kevin Levrone",
        "Ronnie Coleman King Whey",
        "Jym Pre Jym 20 servings", # "Jym" vs "Pre Jym"
        "Universal Nutrition Animal Pak 44 Packs",
        "Scitec Nutrition 100% Whey Protein",
        "Muscletech Nitro Tech 4lbs",
        "Ultimate Nutrition Prostar 100% Whey",
        "Gat Sport Nitraflex",
        "Rule 1 R1 Protein 5lbs",
        "Shaker Pro 500ml", # Posible sin marca o marca generica
        "Proteina Vegana 1kg", # Sin marca clara
        "Bsn Syntha-6 Edge",
        "Nutrex Lipo 6 Black",
        "Winkler Nutrition W1",
        "Italo Grottini Whey",
        "Ironrex Whey Protein"
    ]

    output_file = 'brand_matching_test_results.csv'
    
    print(f"Ejecutando test de matching con {len(test_products)} productos...")
    
    with open(output_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Product Name', 'Detected Brand', 'Match Found?'])
        
        for product in test_products:
            brand = matcher.get_best_match(product)
            found = "Yes" if brand != "N/D" else "No"
            writer.writerow([product, brand, found])
            
    print(f"Resultados guardados en: {os.path.abspath(output_file)}")

if __name__ == "__main__":
    run_test()
