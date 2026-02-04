import pandas as pd
import os

file_path = "processed_data/fuzzy_matched/normalized_products_2026-02-04.csv"
try:
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} rows.")
    
    # Check for null dates
    null_dates = df[df['date'].isna()]
    print(f"Rows with NaN date: {len(null_dates)}")
    
    # Check for NaT string
    nat_dates = df[df['date'].astype(str) == 'NaT']
    print(f"Rows with 'NaT' string date: {len(nat_dates)}")
    
    # Simulate conversion
    df['date_converted'] = pd.to_datetime(df['date'], errors='coerce')
    bad_dates = df[df['date_converted'].isna()]
    print(f"Rows with invalid date format (resulting in NaT): {len(bad_dates)}")
    
    # Check if any bad dates result in the traceback behavior
    # Traceback said: parameters: ... 'fecha_precio': NaT
    
    for index, row in bad_dates.head().iterrows():
        try:
            val = pd.to_datetime(row["date"]).date()
            print(f"Row {index}: pd.to_datetime('date').date() = {val} (Type: {type(val)})")
        except Exception as e:
            print(f"Row {index}: Error converting: {e}")
            val = pd.to_datetime(row["date"])
            print(f"Row {index}: pd.to_datetime('date') = {val} (Type: {type(val)})")

except Exception as e:
    print(f"Error: {e}")
