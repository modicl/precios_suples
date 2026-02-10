
import pandas as pd
import os
import sys
import sqlalchemy as sa

# Add current directory to path so we can import step1_ai_classification
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from step1_ai_classification import apply_audit_fixes, get_local_engine
from tools.categorizer import ProductCategorizer

def create_sample():
    # 1. Load the checkpoint (containing AI classifications)
    checkpoint_path = "local_processing_testing/ai_checkpoint.csv"
    if not os.path.exists(checkpoint_path):
        print(f"Error: {checkpoint_path} not found.")
        return

    print(f"Loading data from {checkpoint_path}...")
    df = pd.read_csv(checkpoint_path)
    
    # Store original subcategory for comparison
    df['original_subcategory'] = df['subcategory']

    # 2. Setup Categorizer (DB connection needed for validation)
    print("Connecting to DB...")
    engine = get_local_engine()
    categorizer = ProductCategorizer(db_connection=engine.connect(), enable_ai=False)

    # 3. Apply the Audit Fixes
    print("Applying Audit Fixes...")
    df_fixed = apply_audit_fixes(df.copy(), categorizer)

    # 4. Select Sample of 100
    # Priority: Rows that were changed by the fix
    changed_rows = df_fixed[df_fixed['subcategory'] != df_fixed['original_subcategory']]
    print(f"Found {len(changed_rows)} rows changed by audit rules.")
    
    # Priority: Specific keywords mentioned in audit (Wild, HMB, ZMA, Creatina)
    keywords = ['wild', 'hmb', 'zma', 'creatina', 'beta', 'citrulina']
    keyword_mask = df_fixed['product_name'].str.contains('|'.join(keywords), case=False, na=False)
    keyword_rows = df_fixed[keyword_mask]
    
    # Combine (Changed + Keywords + Random)
    sample = pd.concat([changed_rows, keyword_rows]).drop_duplicates()
    
    if len(sample) < 100:
        remaining = 100 - len(sample)
        others = df_fixed[~df_fixed.index.isin(sample.index)]
        if not others.empty:
            random_sample = others.sample(n=min(remaining, len(others)), random_state=42)
            sample = pd.concat([sample, random_sample])
    else:
        sample = sample.head(100)

    # 5. Save Sample
    output_path = "local_processing_testing/audit_test_sample_100.csv"
    # Select relevant columns for evaluation
    cols = ['product_name', 'brand', 'original_subcategory', 'subcategory', 'category', 'price', 'link']
    # Ensure columns exist
    cols = [c for c in cols if c in sample.columns]
    
    sample[cols].to_csv(output_path, index=False)
    print(f"Sample saved to {output_path}")
    print("\nSample Preview:")
    print(sample[['product_name', 'original_subcategory', 'subcategory']].head(10))

if __name__ == "__main__":
    # Add project root to path
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    create_sample()
