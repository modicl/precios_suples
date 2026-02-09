import sys
import os
import pandas as pd

# Add root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from tools.categorizer import ProductCategorizer

def test_google():
    print("--- Testing Google Gemini Integration (100 products) ---")
    
    # 1. Load sample data (Step 1 CSV or RAW)
    # Let's try to find raw data
    import glob
    raw_files = glob.glob("raw_data/*.csv")
    if not raw_files:
        print("No raw data found.")
        return
        
    df = pd.read_csv(raw_files[0]).head(50) # 50 items for 1 batch
    print(f"Loaded {len(df)} items from {raw_files[0]}")
    
    # 2. Init Categorizer (Should detect Google provider from env)
    # We pass None as DB connection to skip DB loading for this quick test if possible,
    # BUT Categorizer needs DB to load categories map.
    # So we need DB connection.
    
    from tools.db_multiconnect import get_targets
    import sqlalchemy as sa
    
    targets = get_targets()
    url = next((t['url'] for t in targets if t['name'] == 'Local'), None)
    engine = sa.create_engine(url)
    
    cat = ProductCategorizer(db_connection=engine.connect(), enable_ai=True)
    
    if cat.provider != "google":
        print(f"[FAIL] Provider is {cat.provider}, expected 'google'. Check .env")
        return
    else:
        print("[OK] Provider is Google.")

    # 3. Prepare Batch
    items = []
    for _, row in df.iterrows():
        items.append({
            'product': row['product_name'],
            'brand': str(row.get('brand', 'Unknown')),
            'context': str(row.get('subcategory', ''))
        })
        
    # 4. Run Batch
    print("Sending batch to Gemini...")
    import time
    start = time.time()
    results = cat.classify_batch(items)
    elapsed = time.time() - start
    
    print(f"Batch finished in {elapsed:.2f} seconds.")
    
    # 5. Show sample results
    count_ok = 0
    for i, res in enumerate(results[:5]):
        print(f"Item {i+1}: {items[i]['product']} -> {res['nombre_subcategoria'] if res else 'None'} | Clean: {res.get('ai_clean_name') if res else ''}")
        if res: count_ok += 1
        
    print(f"Success Rate (Sample): {count_ok}/5")
    
if __name__ == "__main__":
    test_google()
