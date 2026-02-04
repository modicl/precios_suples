import os
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = sa.create_engine(DATABASE_URL)

with engine.connect() as conn:
    print("Searching for products with 'Winkler' in name...")
    products = conn.execute(text("""
        SELECT p.nombre_producto, m.nombre_marca 
        FROM productos p 
        JOIN marcas m ON p.id_marca = m.id_marca 
        WHERE p.nombre_producto ILIKE '%Winkler%'
        LIMIT 20
    """)).fetchall()
    
    for p in products:
        print(f"Product: {p.nombre_producto} | Brand: {p.nombre_marca}")

    print("\nCheck existing brands with 'Winkler'...")
    brands = conn.execute(text("SELECT id_marca, nombre_marca FROM marcas WHERE nombre_marca ILIKE '%Winkler%'")).fetchall()
    for b in brands:
        print(f"Brand ID: {b.id_marca}, Name: {b.nombre_marca}")
