import pandas as pd
import os

# Define path
file_path = "c:/Users/Villa/Documents/proyectos_python/precios_suples/processed_data/fuzzy_matched/normalized_products_2026-01-29.csv"

print(f"Reading {file_path}...")
df = pd.read_csv(file_path)

initial_count = len(df)
print(f"Initial rows: {initial_count}")
print(f"Unique dates before: {df['date'].unique()}")

# Filter for today only
target_date = "2026-01-29"
df_filtered = df[df['date'] == target_date]

final_count = len(df_filtered)
print(f"Final rows: {final_count}")
print(f"filtered out {initial_count - final_count} rows")

if final_count > 0:
    df_filtered.to_csv(file_path, index=False)
    print("File overwritten with filtered data.")
else:
    print("Error: Filtering resulted in empty dataframe. Aborting save.")
