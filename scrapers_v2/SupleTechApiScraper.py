"""
SupleTechApiScraper.py
======================
Scraper para SupleTech vía VTEX Catalog API (sin Playwright).
~10x más rápido que el scraper browser tradicional.

Categorías validadas contra la API en test_vtex_api.py.

Uso:
    python SupleTechApiScraper.py
    python SupleTechApiScraper.py --headless   # ignorado, solo para compat con RunAll
"""

import csv
import os
import sys
import time
import requests
import argparse
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, current_dir)

from BaseScraper import BaseScraper

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}
PAGE_SIZE = 50
DELAY_S   = 0.3  # entre páginas (ser amigable con el servidor)

# Categorías descubiertas via /api/catalog_system/pub/category/tree/3
# Formato: (main_category, subcategory, fq_path)
CATEGORIES = [
    ("Proteinas",             "Proteína de Whey",           "C:/1/76/77/79/"),   # Concentradas
    ("Proteinas",             "Proteína Hidrolizada",        "C:/1/76/77/80/"),   # Hidrolizadas
    ("Proteinas",             "Proteína Aislada",            "C:/1/76/77/81/"),   # Isolate
    ("Proteinas",             "Proteína Aislada",            "C:/1/76/77/105/"),  # Clear Whey
    ("Proteinas",             "Proteína Vegana",             "C:/1/76/78/"),
    ("Proteinas",             "Proteína de Carne",           "C:/1/76/90/"),
    ("Proteinas",             "Caseína",                     "C:/1/76/104/"),
    ("Creatinas",             "Micronizada",                 "C:/1/11/50/"),
    ("Creatinas",             "Creatina Monohidrato",        "C:/1/11/51/"),
    ("Pre Entrenos",          "Pre Entreno",                 "C:/2/17/"),
    ("Ganadores de Peso",     "Ganadores de Peso",           "C:/1/15/"),
    ("Aminoacidos y BCAA",    "BCAAs",                       "C:/1/13/55/"),
    ("Aminoacidos y BCAA",    "EAAs (Esenciales)",           "C:/1/13/89/"),
    ("Aminoacidos y BCAA",    "Otros Aminoacidos y BCAA",    "C:/1/13/107/"),
    ("Perdida de Grasa",      "Quemadores",                  "C:/2/16/"),
    ("Snacks y Comida",       "Barritas Y Snacks Proteicas", "C:/3/18/"),
    ("Snacks y Comida",       "Shake Proteicos",             "C:/3/67/"),
    ("Bebidas Nutricionales", "Bebidas Nutricionales",       "C:/3/94/"),
    ("Vitaminas y Minerales", "Omega 3 y Aceites",           "C:/4/28/"),
    ("Vitaminas y Minerales", "Probióticos",                 "C:/4/27/"),
    ("Vitaminas y Minerales", "Vitaminas y Minerales",       "C:/4/"),
]

CSV_HEADERS = [
    "date", "site_name", "category", "subcategory", "product_name",
    "brand", "price", "link", "rating", "reviews", "active_discount",
    "thumbnail_image_url", "image_url", "sku", "description",
]


class SupleTechApiScraper(BaseScraper):
    def __init__(self):
        super().__init__(
            base_url="https://www.supletech.cl",
            headless=True,
            site_name="SupleTech",
        )
        self.subfolder = "supletech"

    # ------------------------------------------------------------------
    # VTEX API helpers
    # ------------------------------------------------------------------

    def _get_page(self, fq_path: str, offset: int) -> tuple:
        """Retorna (lista_productos, total). Lanza excepción si falla."""
        url = (
            f"{self.base_url}/api/catalog_system/pub/products/search"
            f"?fq={fq_path}&_from={offset}&_to={offset + PAGE_SIZE - 1}"
        )
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
        try:
            total = int(r.headers.get("resources", "0-0/0").split("/")[-1])
        except Exception:
            total = len(data)
        return data, total

    def _build_product(self, raw: dict, main_cat: str, subcategory: str) -> dict | None:
        """Transforma un producto VTEX al formato CSV del pipeline."""
        name = raw.get("productName", "").strip()
        if not name:
            return None

        brand_raw = raw.get("brand", "N/D").strip()
        brand = self.enrich_brand(brand_raw, name)
        if not brand or brand == "N/D":
            return None

        link = raw.get("link", "")
        desc = raw.get("description", "")

        price = 0
        image_url_raw = ""
        sku = ""

        for item in raw.get("items", []):
            for seller in item.get("sellers", []):
                if not seller.get("sellerDefault"):
                    continue
                offer = seller.get("commertialOffer", {})
                price = int(offer.get("Price", 0) or 0)
            images = item.get("images", [])
            if images:
                image_url_raw = images[0].get("imageUrl", "")
            ref_id = item.get("referenceId", [])
            if ref_id:
                sku = ref_id[0].get("Value", "")
            break  # solo primer item

        image_url = self.download_image(image_url_raw, self.subfolder) if image_url_raw else ""

        return {
            "date":               datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "site_name":          self.site_name,
            "category":           main_cat,
            "subcategory":        subcategory,
            "product_name":       self.clean_text(name),
            "brand":              brand,
            "price":              price,
            "link":               link,
            "rating":             "0",
            "reviews":            "0",
            "active_discount":    str(False),
            "thumbnail_image_url": image_url,
            "image_url":          image_url,
            "sku":                sku,
            "description":        self.clean_description(desc),
        }

    # ------------------------------------------------------------------
    # Override run(): no Playwright, puro requests
    # ------------------------------------------------------------------

    def run(self):
        output_dir = os.path.join(project_root, "raw_data")
        os.makedirs(output_dir, exist_ok=True)
        csv_filename = (
            f"productos_{self.site_name.lower()}_"
            f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        )
        csv_file = os.path.join(output_dir, csv_filename)

        from rich import print as rprint
        rprint(f"[bold cyan][{self.site_name}][/bold cyan] Modo API — {csv_filename}")
        self._log_info(f"Iniciando API scraping. Archivo: {csv_filename}")

        # Pre-cargar inventario S3 una sola vez
        self._ensure_s3_cache(self.subfolder)

        seen = set()
        total_count = 0

        try:
            with open(csv_file, mode="w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writeheader()

                for main_cat, subcategory, fq_path in CATEGORIES:
                    offset = 0
                    cat_count = 0

                    while True:
                        try:
                            data, total = self._get_page(fq_path, offset)
                        except Exception as e:
                            rprint(f"[red][{self.site_name}] ERROR {fq_path}: {e}[/red]")
                            self._log_info(f"ERROR {fq_path}: {e}")
                            break

                        if not data:
                            break

                        for raw in data:
                            link = raw.get("link", "")
                            if link in seen:
                                continue
                            seen.add(link)

                            prod = self._build_product(raw, main_cat, subcategory)
                            if not prod:
                                continue

                            writer.writerow(prod)
                            f.flush()
                            cat_count += 1
                            total_count += 1

                        offset += PAGE_SIZE
                        if offset >= total or len(data) < PAGE_SIZE:
                            break
                        time.sleep(DELAY_S)

                    rprint(f"  [{main_cat}] {subcategory}: {cat_count} productos")
                    self._log_info(f"{fq_path}: {cat_count} productos")

        except Exception as e:
            rprint(f"[red][{self.site_name}] Error crítico: {e}[/red]")
            self._log_info(f"ERROR CRÍTICO: {e}")
            raise

        rprint(f"[bold green][{self.site_name}] TOTAL: {total_count} productos únicos[/bold green]")
        self._log_info(f"Finalizado. Total: {total_count} productos.")

    def extract_process(self, page):
        raise NotImplementedError("SupleTechApiScraper usa run() directo, no Playwright.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--headless", action="store_true", help="Ignorado (sin browser)")
    parser.parse_args()
    SupleTechApiScraper().run()
