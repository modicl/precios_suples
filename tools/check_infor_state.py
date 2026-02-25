"""
Checks current DB state for INFOR PRO KIDS merge (id=9238 → id=7505).
"""
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dotenv import load_dotenv
import sqlalchemy as sa

load_dotenv()

url = os.environ["DB_HOST_PROD"]
engine = sa.create_engine(url)

with engine.connect() as conn:
    # Check both productos
    rows = conn.execute(sa.text("""
        SELECT p.id_producto, p.nombre_producto, m.nombre_marca AS marca, p.id_subcategoria
        FROM productos p
        LEFT JOIN marcas m ON m.id_marca = p.id_marca
        WHERE p.id_producto IN (7505, 9238)
        ORDER BY p.id_producto
    """)).fetchall()
    print("=== productos ===")
    for r in rows:
        print(dict(r._mapping))

    # Check producto_tienda
    rows = conn.execute(sa.text("""
        SELECT pt.id_producto_tienda, pt.id_producto, t.nombre_tienda AS tienda
        FROM producto_tienda pt
        JOIN tiendas t ON t.id_tienda = pt.id_tienda
        WHERE pt.id_producto IN (7505, 9238)
        ORDER BY pt.id_producto
    """)).fetchall()
    print("\n=== producto_tienda ===")
    for r in rows:
        print(dict(r._mapping))

    # Check click_analytics
    rows = conn.execute(sa.text("""
        SELECT id_producto, COUNT(*) AS cnt
        FROM click_analytics
        WHERE id_producto IN (7505, 9238)
        GROUP BY id_producto
    """)).fetchall()
    print("\n=== click_analytics ===")
    for r in rows:
        print(dict(r._mapping))

    # Check historia_precios (via producto_tienda)
    rows = conn.execute(sa.text("""
        SELECT hp.id_producto_tienda, pt.id_producto, COUNT(*) AS cnt
        FROM historia_precios hp
        JOIN producto_tienda pt ON pt.id_producto_tienda = hp.id_producto_tienda
        WHERE pt.id_producto IN (7505, 9238)
        GROUP BY hp.id_producto_tienda, pt.id_producto
        ORDER BY pt.id_producto
    """)).fetchall()
    print("\n=== historia_precios (via producto_tienda) ===")
    for r in rows:
        print(dict(r._mapping))
