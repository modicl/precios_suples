import pandas as pd
import glob
import os

csv_file = "c:/Users/Villa/Documents/proyectos_python/precios_suples/processed_data/fuzzy_matched/normalized_products_2026-01-29.csv"
# Check if file exists, if not check inserted
if not os.path.exists(csv_file):
    csv_file = "c:/Users/Villa/Documents/proyectos_python/precios_suples/processed_data/inserted_data/normalized_products_2026-01-29.csv"

print(f"Reading {csv_file}")
df = pd.read_csv(csv_file)
print("Unique dates in CSV:")
print(df['date'].unique())

print("Checking for Simply rows:")
simply = df[df['product_name'].str.contains("Simply", case=False, na=False)]
print(simply[['product_name', 'date', 'price']])
