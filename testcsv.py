import pandas as pd

df = pd.read_csv("processed_data/fuzzy_matched/normalized_products_2026-01-06.csv")

print(len(df["brand"].unique().tolist()))
print(len(df[["brand","description","normalized_name"]][df["brand"].isna()]))
print(len(df["brand"][df["brand"].notna()].unique().tolist()))