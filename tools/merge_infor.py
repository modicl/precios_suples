"""
Merges id=9238 (INFOR PRO KIDS, N/D brand) into id=7505 (INFOR PRO KIDS, INFOR brand).
Steps in a single transaction:
  1. UPDATE producto_tienda: reassign id_producto_tienda=6639 to id=7505
  2. UPDATE click_analytics: reassign all rows from 9238 to 7505
  3. DELETE productos WHERE id_producto=9238
"""
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dotenv import load_dotenv
import sqlalchemy as sa

load_dotenv()

url = os.environ["DB_HOST_PROD"]
engine = sa.create_engine(url)

KEEP_ID   = 7505   # INFOR brand — master
DELETE_ID = 9238   # N/D brand — to be removed

with engine.begin() as conn:   # begin() auto-commits or rolls back on exception
    # 1. Reassign producto_tienda rows
    r1 = conn.execute(sa.text(
        "UPDATE producto_tienda SET id_producto = :keep WHERE id_producto = :del"
    ), {"keep": KEEP_ID, "del": DELETE_ID})
    print(f"producto_tienda updated: {r1.rowcount} row(s)")

    # 2. Reassign click_analytics rows
    r2 = conn.execute(sa.text(
        "UPDATE click_analytics SET id_producto = :keep WHERE id_producto = :del"
    ), {"keep": KEEP_ID, "del": DELETE_ID})
    print(f"click_analytics updated: {r2.rowcount} row(s)")

    # 3. Delete the duplicate product
    r3 = conn.execute(sa.text(
        "DELETE FROM productos WHERE id_producto = :del"
    ), {"del": DELETE_ID})
    print(f"productos deleted: {r3.rowcount} row(s)")

print("Merge committed successfully.")
