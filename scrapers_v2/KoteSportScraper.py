# Scraper para KoteSport.cl (WooCommerce / WoodMart theme)
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from playwright.sync_api import Page
from datetime import datetime
from rich import print
import re
import csv
import os


class KoteSportScraper(BaseScraper):
    def __init__(self, headless=False):

        category_urls = {
            "Proteinas": [
                {
                    "url": "https://kotesport.cl/categoria-producto/proteinas/whey/",
                    "subcategory": "Whey Protein"
                },
                {
                    "url": "https://kotesport.cl/categoria-producto/proteinas/isolada/",
                    "subcategory": "Proteína Isolatada"
                },
                {
                    "url": "https://kotesport.cl/categoria-producto/proteinas/huevo/",
                    "subcategory": "Proteínas De Huevo O Carne"
                },
                {
                    "url": "https://kotesport.cl/categoria-producto/proteinas/vegana/",
                    "subcategory": "Proteína Vegana"
                }
            ],
            "Ganadores de Peso": [
                {
                    "url": "https://kotesport.cl/categoria-producto/proteinas/ganadores/",
                    "subcategory": "Ganadores De Peso"
                }
            ],
            "Creatinas": [
                {
                    "url": "https://kotesport.cl/categoria-producto/aminoacidos/creatina/",
                    "subcategory": "Creatina Monohidrato"
                }
            ],
            "Pre Entrenos": [
                 {
                    # Some sites use pre-workout/ or similar, combining all preworkouts here
                    "url": "https://kotesport.cl/categoria-producto/pre-workout/",
                    "subcategory": "Pre Entreno"
                }
            ],
            "Perdida de Grasa": [
                {
                    "url": "https://kotesport.cl/categoria-producto/pre-workout/oxidador/",
                    "subcategory": "Quemadores"
                }
            ],
            "Bebidas Nutricionales": [
                {
                    "url": "https://kotesport.cl/categoria-producto/pre-workout/energeticas/",
                    "subcategory": "Bebidas Nutricionales"
                }
            ],
            "Aminoacidos y BCAA": [
                {
                    "url": "https://kotesport.cl/categoria-producto/aminoacidos/bcaa-amino/",
                    "subcategory": "Aminoácidos"
                },
                {
                    "url": "https://kotesport.cl/categoria-producto/aminoacidos/glutamina/",
                    "subcategory": "Otros Aminoacidos y BCAA"
                }
            ],
            "Vitaminas y Minerales": [
                {
                    "url": "https://kotesport.cl/categoria-producto/natural-vitaminas/multivitaminicos/",
                    "subcategory": "Vitaminas y Minerales"
                },
                {
                    "url": "https://kotesport.cl/categoria-producto/natural-vitaminas/omega-3/",
                    "subcategory": "Omega 3 y Probióticos"
                },
                {
                    "url": "https://kotesport.cl/categoria-producto/natural-vitaminas/colageno/",
                    "subcategory": "Colágeno y Cuidado Articular"
                }
            ],
            "Snacks y Comida": [
                {
                    "url": "https://kotesport.cl/categoria-producto/barra-proteina/",
                    "subcategory": "Barritas Y Snacks Proteicas"
                }
            ]
        }

        selectors = {
            # Exclude .wd-hover-small cards — those are sidebar/widget mini-cards
            # that share the same .product-grid-item class but belong to a single-column
            # widget block (grid-columns-1), not the main product grid (wd-grid-g).
            # They render the title hidden via CSS, causing inner_text() to return "".
            "product_card": ".product-grid-item:not(.wd-hover-small)",
            "product_name": ".wd-entities-title a",
            "link": ".wd-entities-title a",
            "thumbnail": ".product-image-link img",
            "next_button": ".next.page-numbers"
        }

        super().__init__(
            base_url="https://kotesport.cl",
            headless=headless,
            category_urls=category_urls,
            selectors=selectors,
            site_name="KoteSport"
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
            # Ordenar por longitud descendente: marcas más largas tienen prioridad
            # (evita que "GU" matchee antes que "The Gummies")
            self.brands_list.sort(key=len, reverse=True)
        except Exception as e:
            print(f"[yellow]Advertencia: No se pudo cargar marcas_dictionary.csv: {e}[/yellow]")

    # ------------------------------------------------------------------
    # Brand extraction helpers
    # ------------------------------------------------------------------

    def _normalize_brand(self, raw: str) -> str:
        """
        Matches a raw brand string (e.g. from the DOM) against the brands dictionary
        and returns the canonical entry if a match is found, otherwise returns the
        raw string as-is.

        Uses a longest-match strategy so that e.g. "Nutrex Research" is preferred
        over "Nutrex" when both are in the dictionary and both match.

        Direction cases handled:
          - dict entry is substring of raw  → "WINKLER NUTRITION" → "WINKLER"
          - raw is substring of dict entry  → raw is a prefix of a longer canonical name
        """
        if not raw:
            return raw
        raw_upper = raw.upper().strip()
        best: str | None = None
        for b in self.brands_list:
            b_upper = b.upper()
            if b_upper in raw_upper or raw_upper in b_upper:
                if best is None or len(b) > len(best):
                    best = b
        return best if best is not None else raw

    def _extract_brand_from_text(self, text: str) -> str:
        """
        Scans any text string (title, description, etc.) against the brands dictionary.
        Returns the longest matching brand name (to prefer e.g. "Nutrex Research" over
        "Nutrex"), or 'N/D' if none found.
        """
        if not text:
            return "N/D"

        text_upper = text.upper()
        best: str | None = None
        for b in self.brands_list:
            if b.upper() in text_upper:
                if best is None or len(b) > len(best):
                    best = b

        return best if best is not None else "N/D"

    def _extract_brand_from_title(self, title: str) -> str:
        """Kept for backward compatibility — delegates to _extract_brand_from_text."""
        return self._extract_brand_from_text(title)

    # ------------------------------------------------------------------
    # Main scraping logic
    # ------------------------------------------------------------------

    def extract_process(self, page: Page):
        print(f"[green]Iniciando scraping Determinista (V2) de KoteSport...[/green]")

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

                        # Scroll to trigger lazy-load if any
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(1000)

                        try:
                            page.wait_for_selector(self.selectors['product_card'], timeout=7000)
                        except Exception:
                            # 404 or empty category
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
                                # Fallback to text_content() in case CSS hides the element
                                # (inner_text() respects visibility, text_content() does not)
                                if not raw_title.strip():
                                    raw_title = name_el.first.text_content() or ""
                                title = self.clean_text(raw_title)

                            # --- Thumbnail ---
                            thumbnail_url = ""
                            img_el = producto.locator(self.selectors['thumbnail'])
                            if img_el.count() > 0:
                                img = img_el.first
                                src = img.get_attribute("src")
                                data_src = img.get_attribute("data-src")

                                raw_img = data_src if data_src else src
                                if raw_img:
                                    if raw_img.startswith('//'):
                                        raw_img = "https:" + raw_img
                                    
                                    # Strip WooCommerce size suffix like -300x300.jpg if present
                                    clean_img = re.sub(r'-\d+x\d+(\.\w+)$', r'\1', raw_img.split('?')[0])
                                    thumbnail_url = clean_img

                            # --- Price ---
                            price = 0
                            active_discount = False

                            is_on_sale = producto.locator('ins').count() > 0

                            if is_on_sale:
                                active_discount = True
                                sale_el = producto.locator('ins .woocommerce-Price-amount bdi, ins > .woocommerce-Price-amount')
                                if sale_el.count() > 0:
                                    p_text = sale_el.first.inner_text()
                                    clean_p = re.sub(r'[^\d]', '', p_text)
                                    if clean_p:
                                        price = int(clean_p)
                            else:
                                reg_el = producto.locator('.price > .woocommerce-Price-amount bdi, .price .woocommerce-Price-amount bdi')
                                if reg_el.count() > 0:
                                    p_text = reg_el.first.inner_text()
                                    clean_p = re.sub(r'[^\d]', '', p_text)
                                    if clean_p:
                                        price = int(clean_p)

                            # --- Detail page: image, SKU, description, brand ---
                            image_url = ""
                            sku = ""
                            description = ""
                            brand_from_page = ""

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
                                                        // WooCommerce often puts it in a graph
                                                        if (d["@graph"]) {
                                                           for (const n of d["@graph"]) {
                                                               if (n["@type"] === "Product" && n.image) return n.image;
                                                           }
                                                        }
                                                        if (d["@type"] === "Product" && d.image) return d.image;
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
                                            dom_img = detail_page.locator('.woocommerce-product-gallery__image img').first
                                            if dom_img.count() > 0:
                                                src = dom_img.get_attribute("src")
                                                data_src = dom_img.get_attribute("data-large_image")
                                                
                                                i_src = data_src if data_src else src
                                                if i_src:
                                                    image_url = "https:" + i_src if i_src.startswith('//') else i_src
                                        except Exception:
                                            pass

                                    # 2. SKU
                                    try:
                                        sku_el = detail_page.locator('.sku')
                                        if sku_el.count() > 0:
                                            sku = sku_el.first.inner_text().strip()
                                    except Exception:
                                        pass

                                    # 3. Description
                                    try:
                                        desc_el = detail_page.locator('#tab-description, .woocommerce-product-details__short-description').first
                                        if desc_el.count() > 0:
                                            # Strip HTML tags nicely and only take inner text
                                            description = desc_el.inner_text().strip()
                                    except Exception:
                                        pass

                                    # 4. Brand — WoodMart brand block (.wd-product-brands)
                                    # Priority: img[alt] > text content of the link
                                    # _normalize_brand maps the raw DOM value (which may
                                    # include site typos like "Winkler Nutrition_" or
                                    # longer forms like "WINKLER NUTRITION") to the
                                    # canonical entry in the brands dictionary.
                                    try:
                                        brand_img = detail_page.locator('.wd-product-brands img').first
                                        if brand_img.count() > 0:
                                            alt = brand_img.get_attribute('alt') or ""
                                            if alt.strip():
                                                brand_from_page = self._normalize_brand(self.clean_text(alt))
                                        if not brand_from_page:
                                            brand_link = detail_page.locator('.wd-product-brands a').first
                                            if brand_link.count() > 0:
                                                brand_from_page = self._normalize_brand(self.clean_text(brand_link.inner_text()))
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
                            # Priority: 1) DOM brand block on detail page
                            #           2) Title scan against dictionary
                            #           3) Description scan against dictionary
                            #           4) enrich_brand (LLM / fuzzy fallback)
                            brand = brand_from_page or "N/D"
                            if brand == "N/D":
                                brand = self._extract_brand_from_text(title)
                            if brand == "N/D":
                                brand = self._extract_brand_from_text(description)
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
                                # WooCommerce pagination links usually are absolute or relative paths
                                next_url = href if href.startswith('http') else self.base_url + href
                                page.goto(next_url, wait_until="domcontentloaded")
                                page_number += 1
                                page.wait_for_timeout(2000)
                            else:
                                print("  > No hay atributo href en el botón Siguiente...")
                                break
                        else:
                            print("  > No hay más páginas.")
                            break

                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")


if __name__ == "__main__":
    scraper = KoteSportScraper(headless=True)
    scraper.run()
