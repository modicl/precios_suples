# Scraper para BYON.cl (Shopify)
# Brand is available on the product detail page via [class*="vendor"] / .title-vendor
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from playwright.sync_api import Page
from datetime import datetime
from rich import print
import re


class BYONScraper(BaseScraper):
    def __init__(self, headless=False):

        # Taxonomy mapping: main_category -> list of { url, subcategory }
        # Excluded: Ropa y Accesorios, Shakers, Bidones, Straps
        category_urls = {
            "Proteinas": [
                {
                    "url": "https://www.byon.cl/collections/proteina-de-suero-de-leche",
                    "subcategory": "Proteína de Whey"
                },
                {
                    "url": "https://www.byon.cl/collections/proteina-isolate",
                    "subcategory": "Proteína Aislada"
                },
                {
                    "url": "https://www.byon.cl/collections/proteina-hidrolizada",
                    "subcategory": "Proteína Hidrolizada"
                },
                {
                    "url": "https://www.byon.cl/collections/proteina-vegana",
                    "subcategory": "Proteína Vegana"
                },
                {
                    "url": "https://www.byon.cl/collections/proteina-de-carne",
                    "subcategory": "Proteína de Carne"
                },
                {
                    "url": "https://www.byon.cl/collections/proteina-sin-lactosa",
                    "subcategory": "CATEGORIZAR_PROTEINA"
                },
                {
                    "url": "https://www.byon.cl/collections/proteina-libre-de-gluten",
                    "subcategory": "CATEGORIZAR_PROTEINA"
                },
            ],
            "Ganadores de Peso": [
                {
                    "url": "https://www.byon.cl/collections/ganador-de-peso",
                    "subcategory": "Ganadores De Peso"
                },
            ],
            "Creatinas": [
                {
                    "url": "https://www.byon.cl/collections/creatinas",
                    "subcategory": "Creatina Monohidrato"
                },
            ],
            "Pre Entrenos": [
                {
                    "url": "https://www.byon.cl/collections/pre-entrenamientos",
                    "subcategory": "Pre Entreno"
                },
                {
                    "url": "https://www.byon.cl/collections/beta-alanina",
                    "subcategory": "Pre Entreno"
                },
                {
                    "url": "https://www.byon.cl/collections/cafeinas",
                    "subcategory": "Cafeína"
                },
                {
                    "url": "https://www.byon.cl/collections/argininas",
                    "subcategory": "Óxido Nítrico"
                },
                {
                    "url": "https://www.byon.cl/collections/citrulina",
                    "subcategory": "Óxido Nítrico"
                },
            ],
            "Aminoacidos y BCAA": [
                {
                    "url": "https://www.byon.cl/collections/aminoacidos",
                    "subcategory": "Aminoácidos"
                },
                {
                    "url": "https://www.byon.cl/collections/bcaa",
                    "subcategory": "BCAA"
                },
                {
                    "url": "https://www.byon.cl/collections/eaa",
                    "subcategory": "Aminoácidos"
                },
                {
                    "url": "https://www.byon.cl/collections/glutaminas",
                    "subcategory": "Otros Aminoacidos y BCAA"
                },
                {
                    "url": "https://www.byon.cl/collections/hmb",
                    "subcategory": "Otros Aminoacidos y BCAA"
                },
                {
                    "url": "https://www.byon.cl/collections/taurina",
                    "subcategory": "Otros Aminoacidos y BCAA"
                },
            ],
            "Perdida de Grasa": [
                {
                    "url": "https://www.byon.cl/collections/dieta-y-quemadores",
                    "subcategory": "Quemadores"
                },
                {
                    "url": "https://www.byon.cl/collections/carnitinas",
                    "subcategory": "L-Carnitina"
                },
                {
                    "url": "https://www.byon.cl/collections/cla",
                    "subcategory": "Quemadores"
                },
                {
                    "url": "https://www.byon.cl/collections/quemadores-nocturnos",
                    "subcategory": "Quemadores"
                },
                {
                    "url": "https://www.byon.cl/collections/libre-de-estimulantes",
                    "subcategory": "Quemadores"
                },
            ],
            "Vitaminas y Minerales": [
                {
                    "url": "https://www.byon.cl/collections/vitaminas-y-salud",
                    "subcategory": "Vitaminas y Minerales"
                },
                {
                    "url": "https://www.byon.cl/collections/multivitaminicos",
                    "subcategory": "Multivitaminicos"
                },
                {
                    "url": "https://www.byon.cl/collections/vitaminas-a-b-c-d-e",
                    "subcategory": "Vitaminas y Minerales"
                },
                {
                    "url": "https://www.byon.cl/collections/colagenos",
                    "subcategory": "Colágeno"
                },
                {
                    "url": "https://www.byon.cl/collections/omegas",
                    "subcategory": "Omega 3 y Probióticos"
                },
                {
                    "url": "https://www.byon.cl/collections/adaptogenos",
                    "subcategory": "Vitaminas y Minerales"
                },
                {
                    "url": "https://www.byon.cl/collections/magnesios-y-zincs",
                    "subcategory": "Vitaminas y Minerales"
                },
                {
                    "url": "https://www.byon.cl/collections/minerales",
                    "subcategory": "Vitaminas y Minerales"
                },
                {
                    "url": "https://www.byon.cl/collections/probioticos",
                    "subcategory": "Omega 3 y Probióticos"
                },
                {
                    "url": "https://www.byon.cl/collections/antioxidantes",
                    "subcategory": "Vitaminas y Minerales"
                },
                {
                    "url": "https://www.byon.cl/collections/zma",
                    "subcategory": "Vitaminas y Minerales"
                },
                {
                    "url": "https://www.byon.cl/collections/biotin",
                    "subcategory": "Vitaminas y Minerales"
                },
            ],
            "Pro Hormonales": [
                {
                    "url": "https://www.byon.cl/collections/pro-hormonales",
                    "subcategory": "Pro Hormonales"
                },
            ],
            "Snacks y Comida": [
                {
                    "url": "https://www.byon.cl/collections/barras-de-proteina-1",
                    "subcategory": "Barritas Y Snacks Proteicas"
                },
                {
                    "url": "https://www.byon.cl/collections/geles-y-snacks",
                    "subcategory": "Snacks Dulces"
                },
                {
                    "url": "https://www.byon.cl/collections/snacks-y-endurence",
                    "subcategory": "Bebidas Nutricionales"
                },
            ],
            "Packs": [
                {
                    "url": "https://www.byon.cl/collections/packs",
                    "subcategory": "Packs"
                },
            ],
        }

        selectors = {
            "product_card": ".card-wrapper",
            "product_name": ".card__heading .full-unstyled-link, .card__heading a",
            "price_final": ".price-item--sale",
            "price_regular": ".price-item--regular",
            "link": ".card__heading .full-unstyled-link, .card__heading a",
            "thumbnail": ".card__media img",
            "next_button": 'a[aria-label="Página siguiente"]',
            # Detail page
            "vendor": "[class*='vendor']",
            "description": ".product__description",
            "sku_ld": "script[type='application/ld+json']",
        }

        super().__init__(
            base_url="https://www.byon.cl",
            headless=headless,
            category_urls=category_urls,
            selectors=selectors,
            site_name="BYON"
        )

        self.classifier = CategoryClassifier()

    # ------------------------------------------------------------------
    # Main scraping logic
    # ------------------------------------------------------------------

    def extract_process(self, page: Page):
        print(f"[green]Iniciando scraping Determinista (V2) de BYON.cl...[/green]")

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

                        # Scroll to trigger lazy-load images
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(1000)

                        try:
                            page.wait_for_selector(self.selectors['product_card'], timeout=7000)
                        except Exception:
                            print(f"[yellow]No se encontraron productos en {url} (página {page_number}).[/yellow]")
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
                                # Priority: srcset (highest res) > src
                                srcset = img.get_attribute("srcset")
                                src = img.get_attribute("src")

                                raw_img = ""
                                if srcset:
                                    candidates = srcset.split(',')
                                    if candidates:
                                        raw_img = candidates[-1].strip().split(' ')[0]
                                if not raw_img and src and "base64" not in src:
                                    raw_img = src

                                if raw_img:
                                    if raw_img.startswith('//'):
                                        raw_img = "https:" + raw_img
                                    # Strip Shopify size suffix (e.g. _300x.jpg)
                                    clean_img = raw_img.split('?')[0]
                                    clean_img = re.sub(r'_\d+x(\d+)?', '', clean_img)
                                    thumbnail_url = clean_img

                            # --- Price & Discount ---
                            price = 0
                            active_discount = False

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

                            # --- Detail page: brand, image, SKU, description ---
                            brand = "N/D"
                            image_url = ""
                            sku = ""
                            description = ""

                            if link != "N/D":
                                detail_page = None
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)

                                    # 1. Brand — available via [class*="vendor"] above the product title
                                    try:
                                        vendor_el = detail_page.locator(self.selectors['vendor']).first
                                        if vendor_el.count() > 0:
                                            raw_brand = vendor_el.inner_text().strip()
                                            if raw_brand:
                                                brand = self.clean_text(raw_brand)
                                    except Exception:
                                        pass

                                    # 2. Main image — Open Graph
                                    try:
                                        og_img = detail_page.locator('meta[property="og:image"]').first.get_attribute('content')
                                        if og_img:
                                            image_url = og_img
                                    except Exception:
                                        pass

                                    # JSON-LD fallback for image
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

                                    # 3. SKU — JSON-LD
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

                                    # 4. Description
                                    try:
                                        desc_el = detail_page.locator(self.selectors['description']).first
                                        if desc_el.count() > 0:
                                            description = desc_el.inner_text().strip()
                                    except Exception:
                                        pass

                                    # 5. Price fallback — JSON-LD (cuando el listing no expuso precio)
                                    if price == 0:
                                        try:
                                            jsonld_price = detail_page.evaluate('''() => {
                                                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                                for (const s of scripts) {
                                                    try {
                                                        const d = JSON.parse(s.innerText);
                                                        const offers = d.offers
                                                            ? (Array.isArray(d.offers) ? d.offers : [d.offers])
                                                            : [];
                                                        for (const o of offers) {
                                                            if (o.price && parseFloat(o.price) > 0)
                                                                return parseFloat(o.price);
                                                        }
                                                    } catch(e) {}
                                                }
                                                return null;
                                            }''')
                                            if jsonld_price and float(jsonld_price) > 0:
                                                price = int(float(jsonld_price))
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

                            # --- Brand enrichment fallback ---
                            brand = self.enrich_brand(brand, title)

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
                                page.goto(
                                    self.base_url + href if href.startswith('/') else href,
                                    wait_until="domcontentloaded"
                                )
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
    scraper = BYONScraper(headless=True)
    scraper.run()
