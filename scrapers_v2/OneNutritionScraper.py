"""
OneNutritionScraper.py
======================
Scraper para OneNutrition.cl vía la API JSON pública de Shopify (sin Playwright).

La tienda migró de PrestaShop a Shopify en sep 2026: las URLs /tienda/<cat>
redirigen a /collections/<cat> y la grilla #js-product-list ya no existe.
Shopify expone /collections/<handle>/products.json con título, marca (vendor),
variantes con precio e imágenes, así que no hace falta un navegador.

Uso:
    python OneNutritionScraper.py
    python OneNutritionScraper.py --headless   # ignorado, solo para compat con RunAll
"""
import argparse
import csv
import html
import os
import re
import sys
import time
from datetime import datetime

from playwright.sync_api import sync_playwright
from rich import print

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, current_dir)

from BaseScraper import BaseScraper, BrowserSlots, _install_resource_blocking
from CategoryClassifier import CategoryClassifier

PAGE_SIZE = 250   # máximo que acepta products.json
DELAY_S   = 1.5   # entre páginas (ser amigable con el servidor; Cloudflare devuelve 429 si se abusa)
COLLECTION_DELAY_S = 3.0  # pausa entre colecciones
MAX_RETRIES   = 3         # intentos por petición ante 429/503 (esperas de 60s y 120s)
MAX_BACKOFF_S = 180       # espera máxima entre reintentos

# (main_category, subcategoría determinista, handle de la colección Shopify)
# La subcategoría final la refina CategoryClassifier.
CATEGORIES = [
    ("Proteinas",             "Proteína de Whey",         "proteinas"),
    ("Creatinas",             "Creatina Monohidrato",     "creatinas"),
    ("Vitaminas y Minerales", "Vitaminas y Minerales",    "vitaminas-salud"),
    ("Pre Entrenos",          "Pre Entreno",              "energia-resistencia"),
    ("Ganadores de Peso",     "Ganadores De Peso",        "ganadores-de-masa"),
    ("Aminoacidos y BCAA",    "Otros Aminoacidos y BCAA", "aminoacidos-1"),
    ("Perdida de Grasa",      "Dieta Quemadores",         "dieta-quemadores"),
    ("Snacks y Comida",       "Barras Snack",             "barras-snack"),
]

CSV_HEADERS = [
    "date", "site_name", "category", "subcategory", "product_name",
    "brand", "price", "link", "rating", "reviews", "active_discount",
    "thumbnail_image_url", "image_url", "sku", "description",
]

# Patrones de sufijos de bundle que OneNutrition agrega al nombre del producto.
# Cuando la tienda regala un accesorio/suplemento, lo concatena con " + " al nombre
# base. Estos patrones identifican ese sufijo para que el pipeline lo trate como el
# mismo producto base (sin el regalo) y no cree un duplicado en Packs.
#
# Condición para stripear: el segmento ANTES del " + " debe contener una cantidad
# de presentación (peso/volumen/unidades) Y el sufijo NO debe comenzar con el nombre
# de otro suplemento (creatina, proteina, omega, etc.) — esos son packs reales.
# Ejemplos que SÍ stripeamos (sufijo = accesorio sin valor nutricional propio):
#   "ISO 100 (1.4 Lb) DYMATIZE + SHAKER DYMATIZE"      → "ISO 100 1.4lb DYMATIZE"
#   "METAPURE 2KG QNT + BALDE PLÁSTICO"                → "METAPURE 2KG QNT"
# Ejemplos que NO stripeamos (sufijo = otro suplemento → producto separado):
#   "ISO 100 5LB DYMATIZE + CREATINA 300 GR DYMATIZE"  → sin cambio
#   "ISO 100 5LB DYMATIZE + CREATINA 500 OSTROVIT"     → sin cambio
#   "ISO 100 5LB + CREAPURE KIFFER 400 GR"             → sin cambio
# Ejemplos que NO stripeamos (fórmulas combinadas):
#   "Calcio + Magnesio + Zinc 100 Tab Solgar"           → sin cambio
#   "ZMA + B6 60Cap Winkler"                            → sin cambio
#   "Mg + B6 90Tabletas Ostrovit"                       → sin cambio
_QTY_RE = re.compile(
    r'\b\d+(?:[.,]\d+)?\s*(?:gr|g|kg|lb|lbs|cap|caps|tab|tabs|mg|ml|softgel|servicios|uni)\b',
    re.IGNORECASE
)

# Palabras que, si aparecen al inicio del sufijo, indican que es otro suplemento
# (no un accesorio) → NO stripear, conservar como producto separado.
_SUPPLEMENT_SUFFIX_RE = re.compile(
    r'^(?:creatina|creapure|proteina|whey|caseina|colageno|collagen|omega|vitamina|vitamin|'
    r'magnesio|zinc|calcio|calcium|hierro|bcaa|glutamina|glutamine|cla|carnitina|carnitine|'
    r'cafeina|caffeine|pre.?entreno|preworkout|gainers?|mass|gainer|aminoacid|amino|'
    r'melatonin|melatonina|tribulus|ashwagandha|zma|casein|albumin|albumina|arginine|'
    r'arginina|citrulline|citrulina|beta.?alanine|beta.?alanina|hmb|ecdysterone|ecdisterona)',
    re.IGNORECASE
)

# Regex para normalizar cantidades de presentación entre paréntesis y colapsar
# el espacio entre número y unidad.
# "(1.4 Lb)" → "1.4lb"  |  "(2 Kg)" → "2kg"  |  "5 LB" → "5lb"
# Esto hace que "ISO 100 (1.4 Lb) DYMATIZE" produzca el mismo clean_name
# que "Iso 100 1.4lb" de otras tiendas, evitando duplicados en el pipeline.
_SIZE_PARENS_RE = re.compile(
    r'\((\d+(?:[.,]\d+)?)\s*(lb|lbs|kg|g|gr|ml|mg|oz)\)',
    re.IGNORECASE
)
_SIZE_SPACE_RE = re.compile(
    r'(\d+(?:[.,]\d+)?)\s+(lb|lbs|kg|g|gr|ml|mg|oz)\b',
    re.IGNORECASE
)


def _normalize_size_notation(title: str) -> str:
    """
    Normaliza la notación de cantidades de presentación para que el pipeline
    de matching fuzzy (step2) agrupe correctamente productos de distintas tiendas.

    Transformaciones:
        "(1.4 Lb)"  → "1.4lb"
        "(2 Kg)"    → "2kg"
        "5 LB"      → "5lb"
        "300 GR"    → "300gr"

    No toca unidades dentro de paréntesis que no son cantidades de presentación
    (e.g. "(60 Cap)" se mantiene porque 'cap' no está en la lista de unidades
    de presentación de este regex).
    """
    # Paso 1: eliminar paréntesis alrededor de la cantidad
    result = _SIZE_PARENS_RE.sub(lambda m: m.group(1) + m.group(2).lower(), title)
    # Paso 2: colapsar espacio entre número y unidad
    result = _SIZE_SPACE_RE.sub(lambda m: m.group(1) + m.group(2).lower(), result)
    return result


def _strip_bundle_suffix(title: str) -> str:
    """
    Elimina el sufijo de bundle del título si el segmento antes del ' + '
    ya contiene una cantidad de presentación (peso/volumen/unidades) Y el
    sufijo no es otro suplemento (en cuyo caso es un pack real y se conserva).

    Ejemplos stripeados (sufijo = accesorio):
        "ISO 100 (1.4 Lb) DYMATIZE + SHAKER DYMATIZE"     → "ISO 100 1.4lb DYMATIZE"
        "METAPURE 2KG QNT + BALDE PLÁSTICO"               → "METAPURE 2KG QNT"
    Ejemplos conservados (sufijo = otro suplemento → pack real):
        "ISO 100 5LB DYMATIZE + CREATINA 300 GR DYMATIZE" → sin cambio
        "ISO 100 5LB + CREAPURE KIFFER 400 GR"            → sin cambio
    Ejemplos conservados (fórmulas combinadas):
        "Calcio + Magnesio + Zinc 100 Tab Solgar"          → sin cambio
        "ZMA + B6 60Cap Winkler"                           → sin cambio
    """
    # Normalizar notación de cantidades antes de buscar el corte
    title = _normalize_size_notation(title)

    # Iterar todos los ' + ' del título para encontrar el punto de corte correcto
    for m in re.finditer(r'\s*\+\s*', title):
        prefix = title[:m.start()]
        suffix_start = title[m.end():]

        # Si el sufijo es otro suplemento, es un pack real → no stripear
        if _SUPPLEMENT_SUFFIX_RE.match(suffix_start):
            return title

        # Solo stripear si el prefijo ya contiene una cantidad de presentación
        if _QTY_RE.search(prefix):
            cleaned = prefix.strip()
            return cleaned if cleaned else title

        # También stripear si lo que sigue es "Shaker" (accesorio sin cantidad en prefijo)
        if re.match(r'Shaker\b', suffix_start, re.IGNORECASE):
            cleaned = prefix.strip()
            return cleaned if cleaned else title

    return title


class OneNutritionScraper(BaseScraper):
    def __init__(self):
        super().__init__(
            base_url="https://onenutrition.cl",
            headless=True,
            site_name="OneNutrition",
        )
        self.subfolder = "onenutrition"
        self.classifier = CategoryClassifier()

    # ------------------------------------------------------------------
    # Shopify API helpers
    # ------------------------------------------------------------------

    def _get_page(self, handle: str, page: int) -> list:
        """
        Retorna la lista de productos de una página de la colección.

        Se pide con un navegador real (Playwright), no con `requests`: Cloudflare responde 429
        a clientes no-navegador (huella TLS / sin cookies) aunque la IP esté bien, mientras que
        un Chromium obtiene 200 sobre la misma URL (verificado nov 2026).
        """
        url = f"{self.base_url}/collections/{handle}/products.json?limit={PAGE_SIZE}&page={page}"
        for attempt in range(1, MAX_RETRIES + 1):
            resp = self._browser_page.goto(url, wait_until="domcontentloaded", timeout=45000)
            status = resp.status if resp else 0
            if status in (429, 503) and attempt < MAX_RETRIES:
                # Rate limit real: respetar Retry-After (acotado) con backoff creciente
                try:
                    retry_after = int(resp.headers.get("retry-after", ""))
                except ValueError:
                    retry_after = 0
                wait = min(max(retry_after, 30) * attempt, MAX_BACKOFF_S)
                print(f"[yellow][{self.site_name}] {status} en {handle} p{page}; "
                      f"reintento {attempt}/{MAX_RETRIES - 1} en {wait}s[/yellow]")
                time.sleep(wait)
                continue
            if status >= 400 or status == 0:
                raise RuntimeError(f"{status} Client Error en {url}")
            return resp.json().get("products", [])

    @staticmethod
    def _to_int_price(value) -> int:
        try:
            return int(float(value or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _html_to_text(body_html: str) -> str:
        text = re.sub(r"<(br|/p|/li|/h\d)[^>]*>", "\n", body_html or "", flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        return html.unescape(text)

    def _build_product(self, raw: dict, main_cat: str, subcategory: str) -> dict | None:
        """Transforma un producto Shopify al formato CSV del pipeline."""
        raw_title = (raw.get("title") or "").strip()
        if not raw_title:
            return None
        title = _strip_bundle_suffix(self.clean_text(raw_title))

        brand = self.enrich_brand(self.clean_text(raw.get("vendor") or "N/D"), title, scan_title=True)
        if not brand or brand == "N/D":
            return None

        # Precio: variante disponible más barata (lo que muestra la grilla);
        # si ninguna está disponible, se usan todas las variantes.
        variants = raw.get("variants") or []
        if not variants:
            return None
        available = [v for v in variants if v.get("available")] or variants
        variant = min(available, key=lambda v: self._to_int_price(v.get("price")))
        price = self._to_int_price(variant.get("price"))
        if price <= 0:
            return None
        active_discount = self._to_int_price(variant.get("compare_at_price")) > price

        description = self.clean_description(self._html_to_text(raw.get("body_html", "")))
        final_category, final_subcategory = self.classifier.classify(
            title, description, main_cat, subcategory, brand
        )

        images = raw.get("images") or []
        image_url_raw = images[0].get("src", "") if images else ""
        image_url = self.download_image(image_url_raw, self.subfolder) if image_url_raw else ""

        return {
            "date":                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "site_name":           self.site_name,
            "category":            self.clean_text(final_category),
            "subcategory":         final_subcategory,
            "product_name":        title,
            "brand":               brand,
            "price":               price,
            "link":                f"{self.base_url}/products/{raw.get('handle', '')}",
            "rating":              "0",
            "reviews":             "0",
            "active_discount":     str(active_discount),
            "thumbnail_image_url": image_url,
            "image_url":           image_url,
            "sku":                 variant.get("sku") or "",
            "description":         description,
        }

    # ------------------------------------------------------------------
    # Override run(): API JSON de Shopify, pedida desde un Chromium real (un solo navegador)
    # ------------------------------------------------------------------

    def run(self):
        """Espera un slot global de navegador, abre Chromium y delega en _scrape()."""
        slots = BrowserSlots.from_env()
        if slots is not None:
            slots.acquire(self.site_name)
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    headless=self.headless,
                    args=["--disable-blink-features=AutomationControlled", "--no-sandbox",
                          "--disable-dev-shm-usage"],
                )
                try:
                    major = browser.version.split(".")[0]
                    context = browser.new_context(
                        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                    f"(KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36"),
                        locale="es-CL",
                        timezone_id="America/Santiago",
                        viewport={"width": 1366, "height": 768},
                    )
                    if os.environ.get("BLOCK_RESOURCES", "0") == "1":
                        _install_resource_blocking(context)
                    self._browser_page = context.new_page()
                    # Cookies de Cloudflare: se visita el sitio una vez antes de pedir los JSON
                    try:
                        self._browser_page.goto(self.base_url, wait_until="domcontentloaded", timeout=45000)
                    except Exception as e:
                        print(f"[yellow][{self.site_name}] No se pudo precargar la portada: {e}[/yellow]")
                    self._scrape()
                finally:
                    browser.close()
        finally:
            if slots is not None:
                slots.release()

    def _scrape(self):
        output_dir = os.path.join(project_root, "raw_data")
        os.makedirs(output_dir, exist_ok=True)
        csv_filename = (
            f"productos_{self.site_name.lower()}_"
            f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        )
        csv_file = os.path.join(output_dir, csv_filename)

        print(f"[bold cyan][{self.site_name}][/bold cyan] Modo API Shopify — {csv_filename}")
        self._log_info(f"Iniciando API scraping. Archivo: {csv_filename}")

        total_count = 0
        failed_collections = []
        rate_limited = False

        try:
            with open(csv_file, mode="w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writeheader()

                for main_cat, subcategory, handle in CATEGORIES:
                    page = 1
                    cat_count = 0

                    while True:
                        try:
                            data = self._get_page(handle, page)
                        except Exception as e:
                            print(f"[red][{self.site_name}] ERROR {handle} p{page}: {e}[/red]")
                            self._log_info(f"ERROR {handle} p{page}: {e}")
                            failed_collections.append(handle)
                            # 429/503 que persiste tras los reintentos = IP bloqueada: las demás
                            # colecciones fallarían igual, así que se corta (evita esperar minutos por cada una).
                            if "429" in str(e) or "503" in str(e):
                                rate_limited = True
                            break

                        if not data:
                            break

                        for raw in data:
                            # Deduplicación entre colecciones (un producto puede estar en varias)
                            product_handle = raw.get("handle", "")
                            if product_handle in self.seen_urls:
                                continue
                            self.seen_urls.add(product_handle)

                            prod = self._build_product(raw, main_cat, subcategory)
                            if not prod:
                                continue

                            writer.writerow(prod)
                            f.flush()
                            cat_count += 1
                            total_count += 1

                        if len(data) < PAGE_SIZE:
                            break
                        page += 1
                        time.sleep(DELAY_S)

                    print(f"  [{main_cat}] {handle}: {cat_count} productos")
                    self._log_info(f"{handle}: {cat_count} productos")
                    if rate_limited:
                        print(f"[bold red][{self.site_name}] Rate limit persistente: se aborta el resto de colecciones.[/bold red]")
                        self._log_info("Rate limit persistente: aborta colecciones restantes.")
                        break
                    time.sleep(COLLECTION_DELAY_S)

        except Exception as e:
            print(f"[red][{self.site_name}] Error crítico: {e}[/red]")
            self._log_info(f"ERROR CRÍTICO: {e}")
            raise

        print(f"[bold green][{self.site_name}] TOTAL: {total_count} productos únicos[/bold green]")
        self._log_info(f"Finalizado. Total: {total_count} productos.")

        # Que RunAll lo marque FALLO en vez de [OK] cuando faltan datos
        if total_count == 0 or failed_collections:
            msg = (f"{len(failed_collections)} colección(es) fallaron: {', '.join(failed_collections)}"
                   if failed_collections else "0 productos extraídos")
            print(f"[bold red][{self.site_name}] {msg}[/bold red]")
            self._log_info(f"FALLO: {msg}")
            sys.exit(1)

    def extract_process(self, page):
        raise NotImplementedError("OneNutritionScraper usa run() directo, no Playwright.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--headless", action="store_true", help="Ignorado (sin browser)")
    parser.parse_args()
    OneNutritionScraper().run()
