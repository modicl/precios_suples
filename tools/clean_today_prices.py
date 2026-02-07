import os
import sys
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def clean_today_prices(engine, db_name):
    print(f"\n--- Cleaning Today's Prices for: {db_name} ---")
    
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Execute deletion
            print(f"[{db_name}] Deleting records from historia_precios where fecha_precio is TODAY...")
            result = conn.execute(text("DELETE FROM historia_precios WHERE fecha_precio::date = CURRENT_DATE"))
            row_count = result.rowcount
            
            trans.commit()
            print(f"[{db_name}] Successfully deleted {row_count} records.")
            
        except Exception as e:
            trans.rollback()
            print(f"[{db_name}] Error cleaning prices: {e}")

def main():
    load_dotenv()
    
    targets = get_targets()
    if not targets:
        print("Error: No database targets found.")
        return

    print(f"Found database targets: {[t['name'] for t in targets]}")

    for target in targets:
        db_name = target["name"]
        db_url = target["url"]
        
        try:
            print(f"\nConnecting to {db_name}...")
            engine = sa.create_engine(db_url)
            clean_today_prices(engine, db_name)
        except Exception as e:
            print(f"Error connecting/processing {db_name}: {e}")

if __name__ == "__main__":
    main()
