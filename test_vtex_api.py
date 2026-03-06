"""
test_vtex_api.py
================
Script de prueba para validar que la VTEX Catalog API puede reemplazar
el scraping con browser en SupleTech y SuplementosMayoristas.

Si la validación es exitosa, se puede crear un scraper sin Playwright
que llame directamente a la API → ~10x más rápido.

Uso:
    python test_vtex_api.py
    python test_vtex_api.py --store supletech
    python test_vtex_api.py --store mayoristas
    python test_vtex_api.py --csv   # guarda resultados en CSV para comparar
"""

import requests
import json
import csv
import time
import argparse
import os
from datetime import datetime

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}
PAGE_SIZE = 50
DELAY_S   = 0.3   # entre requests (ser amigable con el servidor)

# ---------------------------------------------------------------------------
# Config: mapeo de categorías a rutas VTEX para cada tienda
# (category_path → subcategoría en nuestro schema)
# ---------------------------------------------------------------------------

SUPLETECH_CONFIG = {
    "base_url":  "https://www.supletech.cl",
    "site_name": "SupleTech",
    "categories": [
        # (main_category, subcategory, fq_path)
        # Proteínas — árbol: id=1 > id=76 > id=77 (Whey) > subcats
        ("Proteinas", "Proteína de Whey",     "C:/1/76/77/79/"),   # Concentradas
        ("Proteinas", "Proteína Hidrolizada",  "C:/1/76/77/80/"),   # Hidrolizadas
        ("Proteinas", "Proteína Aislada",      "C:/1/76/77/81/"),   # Isolate
        ("Proteinas", "Proteína Aislada",      "C:/1/76/77/105/"),  # Clear Whey
        ("Proteinas", "Proteína Vegana",       "C:/1/76/78/"),      # Veganas (cat completa)
        ("Proteinas", "Proteína de Carne",     "C:/1/76/90/"),      # De Carne
        ("Proteinas", "Caseína",               "C:/1/76/104/"),     # Colágeno/Caseína
        # Creatinas — árbol: id=11
        ("Creatinas", "Micronizada",           "C:/1/11/50/"),
        ("Creatinas", "Creatina Monohidrato",  "C:/1/11/51/"),
        # Pre Entrenos — árbol: id=2 > id=17
        ("Pre Entrenos", "Pre Entreno",        "C:/2/17/"),
        # Ganadores — árbol: id=1 > id=15
        ("Ganadores de Peso", "Ganadores de Peso", "C:/1/15/"),
        # Aminoácidos — árbol: id=1 > id=13
        ("Aminoacidos y BCAA", "BCAAs",              "C:/1/13/55/"),
        ("Aminoacidos y BCAA", "EAAs (Esenciales)",  "C:/1/13/89/"),
        ("Aminoacidos y BCAA", "Otros Aminoacidos y BCAA", "C:/1/13/107/"),
        # Quemadores — árbol: id=2 > id=16
        ("Perdida de Grasa", "Quemadores",     "C:/2/16/"),
        # Snacks — árbol: id=3
        ("Snacks y Comida", "Barritas Y Snacks Proteicas", "C:/3/18/"),
        ("Snacks y Comida", "Shake Proteicos",             "C:/3/67/"),
        # Bebidas — árbol: id=3
        ("Bebidas Nutricionales", "Bebidas Nutricionales", "C:/3/94/"),
        # Vitaminas — árbol: id=4 (Bienestar)
        ("Vitaminas y Minerales", "Omega 3 y Aceites",   "C:/4/28/"),
        ("Vitaminas y Minerales", "Probióticos",         "C:/4/27/"),
        ("Vitaminas y Minerales", "Vitaminas y Minerales", "C:/4/"),
    ],
}

MAYORISTAS_CONFIG = {
    "base_url":  "https://www.suplementosmayoristas.cl",
    "site_name": "SuplementosMayoristas",
    "categories": [
        # Árbol: id=1 (Proteínas) > subcats
        ("Proteinas", "Proteína de Whey",    "C:/1/13/"),     # Whey protein
        ("Proteinas", "Proteína Aislada",    "C:/1/12/"),     # Whey Isolate
        ("Proteinas", "Proteína Hidrolizada","C:/1/14/"),     # Whey Hidrolizadas
        ("Proteinas", "Proteína Aislada",    "C:/1/15/"),     # Clear Whey
        ("Proteinas", "Proteína Vegana",     "C:/1/16/"),     # Proteínas Veganas
        ("Proteinas", "Proteína de Carne",   "C:/1/9288/"),   # De Carne
        # id=2 Creatinas
        ("Creatinas", "Micronizada",         "C:/2/17/"),
        ("Creatinas", "Creatina Monohidrato","C:/2/18/"),
        # id=3 Quemadores
        ("Perdida de Grasa", "Quemadores",   "C:/3/"),
        # id=4 Pre entreno
        ("Pre Entrenos", "Pre Entreno",      "C:/4/"),
        # id=5 Aminoácidos
        ("Aminoacidos y BCAA", "BCAAs",               "C:/5/23/"),
        ("Aminoacidos y BCAA", "EAAs (Esenciales)",   "C:/5/9290/"),
        ("Aminoacidos y BCAA", "Otros Aminoacidos y BCAA", "C:/5/9297/"),
        ("Aminoacidos y BCAA", "Otros Aminoacidos y BCAA", "C:/5/9298/"),
        # id=9 Ganadores
        ("Ganadores de Peso", "Ganadores De Peso", "C:/9/"),
    ],
}

# ---------------------------------------------------------------------------
# Extracción desde la API
# ---------------------------------------------------------------------------

def _get_page(base_url: str, fq_path: str, offset: int, page_size: int) -> tuple[list, int]:
    """Retorna (productos, total). Lanza excepción si falla."""
    url = (
        f"{base_url}/api/catalog_system/pub/products/search"
        f"?fq={fq_path}&_from={offset}&_to={offset + page_size - 1}"
    )
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()
    resources = r.headers.get("resources", "0-0/0")
    try:
        total = int(resources.split("/")[-1])
    except Exception:
        total = len(data)
    return data, total


def _extract_product(raw: dict, site_name: str, main_cat: str, subcategory: str) -> dict | None:
    """
    Transforma un producto VTEX al formato CSV del pipeline.
    Retorna None si no tiene precio ni disponibilidad.
    """
    name  = raw.get("productName", "").strip()
    brand = raw.get("brand", "N/D").strip()
    link  = raw.get("link", "")
    desc  = raw.get("description", "")

    if not name:
        return None

    # Tomar el primer item con seller activo
    price = 0
    image_url = ""
    thumb_url = ""
    sku = ""
    is_available = False

    for item in raw.get("items", []):
        sellers = item.get("sellers", [])
        for seller in sellers:
            if not seller.get("sellerDefault"):
                continue
            offer = seller.get("commertialOffer", {})
            price = int(offer.get("Price", 0) or 0)
            is_available = offer.get("IsAvailable", False)

        images = item.get("images", [])
        if images:
            thumb_url = images[0].get("imageUrl", "")
            image_url = thumb_url

        ref_id = item.get("referenceId", [])
        if ref_id:
            sku = ref_id[0].get("Value", "")
        break  # solo primer item

    return {
        "date":               datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "site_name":          site_name,
        "category":           main_cat,
        "subcategory":        subcategory,
        "product_name":       name,
        "brand":              brand,
        "price":              price,
        "link":               link,
        "rating":             "0",
        "reviews":            "0",
        "active_discount":    str(False),
        "thumbnail_image_url": thumb_url,
        "image_url":          image_url,
        "sku":                sku,
        "description":        desc,
    }


def fetch_store(config: dict, verbose: bool = True) -> list[dict]:
    """Obtiene todos los productos de una tienda vía API."""
    site   = config["site_name"]
    base   = config["base_url"]
    cats   = config["categories"]
    seen   = set()     # dedup por link
    result = []

    print(f"\n{'='*60}")
    print(f"  {site} — {len(cats)} categorías")
    print(f"{'='*60}")

    for main_cat, subcategory, fq_path in cats:
        offset = 0
        cat_count = 0

        while True:
            try:
                data, total = _get_page(base, fq_path, offset, PAGE_SIZE)
            except Exception as e:
                print(f"  [ERROR] {main_cat}/{subcategory} ({fq_path}): {e}")
                break

            if not data:
                break

            for raw in data:
                prod = _extract_product(raw, site, main_cat, subcategory)
                if not prod:
                    continue
                link = prod["link"]
                if link in seen:
                    continue
                seen.add(link)
                result.append(prod)
                cat_count += 1

            offset += PAGE_SIZE
            if offset >= total or len(data) < PAGE_SIZE:
                break

            time.sleep(DELAY_S)

        if verbose:
            print(f"  [{main_cat}] {subcategory} ({fq_path}): {cat_count} productos")

    print(f"\n  TOTAL {site}: {len(result)} productos únicos")
    return result


# ---------------------------------------------------------------------------
# Validaciones
# ---------------------------------------------------------------------------

def validate(products: list[dict], site_name: str):
    """Muestra estadísticas de calidad de los datos obtenidos."""
    print(f"\n{'='*60}")
    print(f"  VALIDACION — {site_name}")
    print(f"{'='*60}")

    total      = len(products)
    no_name    = sum(1 for p in products if not p["product_name"])
    no_price   = sum(1 for p in products if p["price"] == 0)
    no_brand   = sum(1 for p in products if p["brand"] in ("N/D", "", None))
    no_link    = sum(1 for p in products if not p["link"])
    has_image  = sum(1 for p in products if p["image_url"])

    print(f"  Total productos  : {total}")
    print(f"  Sin nombre       : {no_name}  ({no_name/total*100:.1f}%)" if total else "")
    print(f"  Sin precio (0)   : {no_price}  ({no_price/total*100:.1f}%)" if total else "")
    print(f"  Sin marca        : {no_brand}  ({no_brand/total*100:.1f}%)" if total else "")
    print(f"  Sin link         : {no_link}  ({no_link/total*100:.1f}%)" if total else "")
    print(f"  Con imagen       : {has_image}  ({has_image/total*100:.1f}%)" if total else "")

    # Distribución por categoría
    from collections import Counter
    cat_counts = Counter(p["subcategory"] for p in products)
    print(f"\n  Distribución por subcategoría:")
    for subcat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"    {subcat:<40} {cnt:>4}")

    # Muestra de 3 productos
    print(f"\n  Muestra de productos:")
    for p in products[:3]:
        print(f"    [{p['subcategory'][:20]}] {p['product_name'][:50]} | {p['brand']} | ${p['price']:,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", choices=["supletech", "mayoristas", "ambas"], default="ambas")
    parser.add_argument("--csv",   action="store_true", help="Guardar resultados en CSV")
    args = parser.parse_args()

    configs = []
    if args.store in ("supletech", "ambas"):
        configs.append(SUPLETECH_CONFIG)
    if args.store in ("mayoristas", "ambas"):
        configs.append(MAYORISTAS_CONFIG)

    all_products = []
    t_start = time.monotonic()

    for config in configs:
        products = fetch_store(config, verbose=True)
        validate(products, config["site_name"])
        all_products.extend(products)

    elapsed = time.monotonic() - t_start
    m, s = divmod(int(elapsed), 60)
    print(f"\n  Tiempo total API: {m}m {s}s")
    print(f"  (Comparar contra scraper browser: SupleTech ~21 min, Mayoristas ~24 min)")

    if args.csv and all_products:
        out_dir = os.path.join(os.path.dirname(__file__), "raw_data")
        os.makedirs(out_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        for config in configs:
            site = config["site_name"]
            prods = [p for p in all_products if p["site_name"] == site]
            if not prods:
                continue
            fname = os.path.join(out_dir, f"TEST_vtex_api_{site.lower()}_{ts}.csv")
            with open(fname, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=list(prods[0].keys()))
                writer.writeheader()
                writer.writerows(prods)
            print(f"  CSV guardado: {fname}")


if __name__ == "__main__":
    main()
