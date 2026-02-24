# Scraper para SportNutriShop.cl (Shopify / Dawn theme)
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from playwright.sync_api import Page
from datetime import datetime
from rich import print
import re
import csv
import os


class SportNutriShopScraper(BaseScraper):
    def __init__(self, headless=False):

        # Taxonomy mapping: main_category -> list of { url, subcategory }
        # Excluded: Actividad Sexual, Accesorios, Shakers y botellas
        category_urls = {
            "Proteinas": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/proteinas-y-ganadores",
                    "subcategory": "CATEGORIZAR_PROTEINA"
                }
            ],
            "Creatinas": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/creatinas-y-glutaminas",
                    "subcategory": "Creatina Monohidrato"
                }
            ],
            "Aminoacidos y BCAA": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/arginina",
                    "subcategory": "Otros Aminoacidos y BCAA"
                },
                {
                    "url": "https://www.sportnutrishop.cl/collections/aminos-y-bcaas",
                    "subcategory": "Aminoácidos"
                },
                {
                    "url": "https://www.sportnutrishop.cl/collections/glutaminas",
                    "subcategory": "Otros Aminoacidos y BCAA"
                }
            ],
            "Pre Entrenos": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/pre-entrenamientos",
                    "subcategory": "Pre Entreno"
                },
                {
                    "url": "https://www.sportnutrishop.cl/collections/beta-alanina",
                    "subcategory": "Pre Entreno"
                }
            ],
            "Ganadores de Peso": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/ganadores-de-peso",
                    "subcategory": "Ganadores De Peso"
                }
            ],
            "Perdida de Grasa": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/control-de-peso-y-energia",
                    "subcategory": "Quemadores"
                }
            ],
            "Bebidas Nutricionales": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/energeticas",
                    "subcategory": "Bebidas Nutricionales"
                }
            ],
            "Snacks y Comida": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/barras-proteicas",
                    "subcategory": "Barritas Y Snacks Proteicas"
                }
            ],
            "Vitaminas y Minerales": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/vitaminas-y-minerales-1",
                    "subcategory": "Vitaminas y Minerales"
                },
                {
                    "url": "https://www.sportnutrishop.cl/collections/omega-4",
                    "subcategory": "Omega 3 y Probióticos"
                },
                {
                    "url": "https://www.sportnutrishop.cl/collections/omega-3",
                    "subcategory": "Pro Hormonales"
                }
            ],
            "Packs": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/packs",
                    "subcategory": "Packs"
                }
            ]
        }

        selectors = {
            "product_card": ".card-wrapper",
            "product_name": ".card__heading a, .full-unstyled-link",
            "price_final": ".price-item--sale, .price-item--regular",
            "price_old": ".price-item--regular.price__compare, .price--on-sale .price-item--regular",
            "link": ".card__heading a, .full-unstyled-link",
            "thumbnail": ".card__media img, .media img",
            "next_button": ".pagination__next"
        }

        super().__init__(
            base_url="https://www.sportnutrishop.cl",
            headless=headless,
            category_urls=category_urls,
            selectors=selectors,
            site_name="SportNutriShop"
        )

        self.classifier = CategoryClassifier()

        # Load brands dictionary for brand extraction from titles
        self.brands_list = []
        brands_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'marcas_dictionary.csv')
        try:
            with open(brands_csv, encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    brand_name = row.get('nombre_marca', '').strip()
                    if brand_name:
                        self.brands_list.append(brand_name)
        except Exception as e:
            print(f"[yellow]Advertencia: No se pudo cargar marcas_dictionary.csv: {e}[/yellow]")

    # ------------------------------------------------------------------
    # Brand extraction helpers
    # ------------------------------------------------------------------

    def _extract_brand_from_title(self, title: str) -> str:
        """
        Tries to extract a brand from the product title using two strategies:
        1. Last segment after ' - ' heuristic (SportNutriShop titles often end in '- BRAND')
        2. Dictionary scan: check if any known brand name appears anywhere in the title.
        Returns the brand string if found, or 'N/D' if not.
        """
        if not title:
            return "N/D"

        title_upper = title.upper()

        # Strategy 1: last segment after ' - '
        if " - " in title:
            candidate = title.split(" - ")[-1].strip()
            # Validate against dictionary (case-insensitive)
            for b in self.brands_list:
                if b.upper() == candidate.upper():
                    return b
            # If the candidate looks like a real brand (not a number/size/etc.), use it
            if candidate and not re.match(r'^\d', candidate) and len(candidate) > 1:
                # Do a quick partial check against dictionary
                for b in self.brands_list:
                    if b.upper() in candidate.upper() or candidate.upper() in b.upper():
                        return b
                # Return the raw candidate — enrich_brand will validate/fallback further
                return candidate

        # Strategy 2: full dictionary scan
        for b in self.brands_list:
            if b.upper() in title_upper:
                return b

        return "N/D"

    # ------------------------------------------------------------------
    # Main scraping logic
    # ------------------------------------------------------------------

    def extract_process(self, page: Page):
        print(f"[green]Iniciando scraping Determinista (V2) de SportNutriShop...[/green]")

        context = page.context

        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                deterministic_sub = item['subcategory']

                print(f"\n[bold blue]Procesando:[/bold blue] {main_category} -> {deterministic_sub} ({url})")

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)

                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")

                        # Scroll to trigger lazy-load
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(1000)

                        try:
                            page.wait_for_selector(self.selectors['product_card'], timeout=7000)
                        except Exception:
                            print(f"[red]No se encontraron productos en {url} (página {page_number}).[/red]")
                            break

                        product_cards = page.locator(self.selectors['product_card'])
                        count = product_cards.count()
                        print(f"  > Encontrados {count} productos en esta página.")

                        for i in range(count):
                            producto = product_cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            # --- Link ---
                            link = "N/D"
                            link_el = producto.locator(self.selectors['link'])
                            if link_el.count() > 0:
                                href = link_el.first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href

                            # --- Deduplication ---
                            if link != "N/D" and link in self.seen_urls:
                                continue
                            if link != "N/D":
                                self.seen_urls.add(link)

                            # --- Title ---
                            title = "N/D"
                            name_el = producto.locator(self.selectors['product_name'])
                            if name_el.count() > 0:
                                raw_title = name_el.first.inner_text()
                                title = self.clean_text(raw_title)

                            # --- Thumbnail ---
                            thumbnail_url = ""
                            img_el = producto.locator(self.selectors['thumbnail'])
                            if img_el.count() > 0:
                                img = img_el.first
                                # Priority: srcset (highest res) > data-src > src
                                srcset = img.get_attribute("srcset")
                                data_src = img.get_attribute("data-src")
                                src = img.get_attribute("src")

                                raw_img = ""
                                if srcset:
                                    candidates = srcset.split(',')
                                    if candidates:
                                        raw_img = candidates[-1].strip().split(' ')[0]
                                if not raw_img and data_src and "base64" not in data_src:
                                    raw_img = data_src
                                if not raw_img and src and "base64" not in src:
                                    raw_img = src

                                if raw_img:
                                    if raw_img.startswith('//'):
                                        raw_img = "https:" + raw_img
                                    # Strip Shopify size suffix (e.g. _300x.jpg)
                                    clean_img = raw_img.split('?')[0]
                                    clean_img = re.sub(r'_\d+x(\d+)?', '', clean_img)
                                    thumbnail_url = clean_img

                            # --- Price ---
                            price = 0
                            active_discount = False

                            # Check if on-sale (Dawn theme: .price--on-sale on parent)
                            is_on_sale = producto.locator('.price--on-sale').count() > 0

                            if is_on_sale:
                                active_discount = True
                                sale_el = producto.locator('.price-item--sale')
                                if sale_el.count() > 0:
                                    p_text = sale_el.first.inner_text()
                                    clean_p = re.sub(r'[^\d]', '', p_text)
                                    if clean_p:
                                        price = int(clean_p)
                            else:
                                reg_el = producto.locator('.price-item--regular')
                                if reg_el.count() > 0:
                                    p_text = reg_el.first.inner_text()
                                    clean_p = re.sub(r'[^\d]', '', p_text)
                                    if clean_p:
                                        price = int(clean_p)

                            # --- Detail page: image, SKU, description ---
                            image_url = ""
                            sku = ""
                            description = ""

                            if link != "N/D":
                                detail_page = None
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)

                                    # 1. Main image — Open Graph
                                    try:
                                        og_img = detail_page.locator('meta[property="og:image"]').first.get_attribute('content')
                                        if og_img:
                                            image_url = og_img
                                    except Exception:
                                        pass

                                    # JSON-LD fallback
                                    if not image_url:
                                        try:
                                            json_img = detail_page.evaluate('''() => {
                                                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                                for (const s of scripts) {
                                                    try {
                                                        const d = JSON.parse(s.innerText);
                                                        if ((d["@type"] === "Product" || d["@type"] === "ProductGroup") && d.image)
                                                            return Array.isArray(d.image) ? d.image[0] : d.image;
                                                    } catch(e){}
                                                }
                                                return null;
                                            }''')
                                            if json_img:
                                                image_url = json_img
                                        except Exception:
                                            pass

                                    # DOM fallback
                                    if not image_url:
                                        try:
                                            dom_img = detail_page.locator('.product__media img, .product-media-container img').first
                                            if dom_img.count() > 0:
                                                src = dom_img.get_attribute("src")
                                                if src:
                                                    image_url = "https:" + src if src.startswith('//') else src
                                        except Exception:
                                            pass

                                    # 2. SKU — Shopify JSON-LD or meta
                                    try:
                                        sku_val = detail_page.evaluate('''() => {
                                            const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                            for (const s of scripts) {
                                                try {
                                                    const d = JSON.parse(s.innerText);
                                                    if (d["@type"] === "Product" && d.sku) return d.sku;
                                                    if (d.offers) {
                                                        const offers = Array.isArray(d.offers) ? d.offers : [d.offers];
                                                        if (offers[0] && offers[0].sku) return offers[0].sku;
                                                    }
                                                } catch(e){}
                                            }
                                            return null;
                                        }''')
                                        if sku_val:
                                            sku = str(sku_val).strip()
                                    except Exception:
                                        pass

                                    # 3. Description
                                    try:
                                        desc_el = detail_page.locator('.product__description, .product-single__description').first
                                        if desc_el.count() > 0:
                                            description = desc_el.inner_text().strip()
                                    except Exception:
                                        pass

                                    detail_page.close()

                                except Exception as e:
                                    print(f"[yellow]Error cargando detalle {link}: {e}[/yellow]")
                                    if detail_page:
                                        try:
                                            detail_page.close()
                                        except Exception:
                                            pass

                            # --- Image download (S3 / local) ---
                            site_folder = self.site_name.replace(" ", "_").lower()
                            if thumbnail_url:
                                local_thumb = self.download_image(thumbnail_url, subfolder=site_folder)
                                if local_thumb:
                                    thumbnail_url = local_thumb
                            if image_url:
                                local_img = self.download_image(image_url, subfolder=site_folder)
                                if local_img:
                                    image_url = local_img

                            # --- Brand extraction ---
                            brand = self._extract_brand_from_title(title)
                            brand = self.enrich_brand(brand, title)

                            # --- Classification ---
                            final_category, final_sub = self.classifier.classify(
                                title, description, main_category, deterministic_sub, brand
                            )

                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(final_category),
                                'subcategory': final_sub,
                                'product_name': title,
                                'brand': brand,
                                'price': price,
                                'link': link,
                                'rating': "0",
                                'reviews': "0",
                                'active_discount': active_discount,
                                'thumbnail_image_url': thumbnail_url,
                                'image_url': image_url,
                                'sku': sku,
                                'description': description
                            }

                        # --- Pagination ---
                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href = next_btn.first.get_attribute("href")
                            if href:
                                print(f"  > Avanzando a página {page_number + 1}...")
                                page.goto(self.base_url + href if href.startswith('/') else href,
                                          wait_until="domcontentloaded")
                                page_number += 1
                                page.wait_for_timeout(2000)
                            else:
                                print("  > Click en botón Siguiente...")
                                next_btn.first.click()
                                page.wait_for_timeout(3000)
                                page_number += 1
                        else:
                            print("  > No hay más páginas.")
                            break

                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")


if __name__ == "__main__":
    scraper = SportNutriShopScraper(headless=True)
    scraper.run()
