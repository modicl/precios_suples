import pandas as pd

df = pd.read_csv("processed_data/fuzzy_matched/normalized_products_2026-01-06.csv")

## Subcategorias 
nombres_subcategorias = df[["subcategory","category"]].drop_duplicates().values.tolist()
nombres_subcategorias_json = [{"nombre_subcategoria": subcategoria , "categoria": categoria} for subcategoria,categoria in nombres_subcategorias]

# Revisar si hay subcategorias vacias
for item in nombres_subcategorias_json:
    if item["nombre_subcategoria"] == "" or pd.isna(item["nombre_subcategoria"]):
        print("Hay subcategorias vacias")
        break

# Revisar si hay categorias vacias
for item in nombres_subcategorias_json:
    if item["categoria"] == "" or pd.isna(item["categoria"]):
        print("Hay categorias vacias")
        break