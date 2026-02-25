import pandas as pd
import glob
import os

def inspect():
    # Check normalized
    files = glob.glob("processed_data/fuzzy_matched/normalized_products_*.csv")
        
    if not files:
        print("No normalized files found.")
        return

    files.sort()
    latest = files[-1]
    print(f"Inspecting file: {latest}")
    
    try:
        df = pd.read_csv(latest)
        
        # Search for the product user mentioned
        # Broaden to "Foodtech"
        term = "Foodtech"
        
        # Search in normalized_name OR product_name
        subset = df[
            (df['product_name'].str.contains(term, case=False, na=False)) | 
            (df['normalized_name'].str.contains(term, case=False, na=False))
        ]
        
        print(f"\n--- Found {len(subset)} rows matching '{term}' ---")
        
        if len(subset) > 0:
            columns = ['site_name', 'product_name', 'normalized_name', 'price', 'date']
            print(subset[columns].sort_values(by='site_name').to_string(index=False))
            
            # Check if prices are identical
            unique_prices = subset['price'].unique()
            print(f"\nUnique prices found: {unique_prices}")
        else:
            print("No matches found.")

    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == "__main__":
    inspect()
