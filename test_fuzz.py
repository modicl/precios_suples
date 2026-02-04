from rapidfuzz import fuzz, process

name = "WINKLER NUTRITION"
target = "WINKLER"
canonical_brands = ["WINKLER", "OTRA MARCA"]

score_wratio = fuzz.WRatio(name, target)
print(f"WRatio('{name}', '{target}'): {score_wratio}")

match = process.extractOne(name, canonical_brands, scorer=fuzz.WRatio)
print(f"Best match for '{name}': {match}")
