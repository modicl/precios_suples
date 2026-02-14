import os
import sys
import sqlalchemy as sa
import random

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

def get_local_engine():
    targets = get_targets()
    local_target = next((t for t in targets if t['name'] == 'Local'), None)
    if not local_target:
        print("Error: No se encontró la configuración para 'Local' en db_multiconnect.")
        sys.exit(1)
    return sa.create_engine(local_target['url'])

def check_casing():
    engine = get_local_engine()
    with engine.connect() as conn:
        print("--- Checking Product Names Casing ---")
        query = sa.text("SELECT nombre_producto FROM productos ORDER BY random() LIMIT 10")
        results = conn.execute(query).fetchall()
        for row in results:
            print(f"Product: {row.nombre_producto}")

if __name__ == "__main__":
    check_casing()
