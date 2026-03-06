# Scraper para BYON.cl (Parte 2 de 2)
# Categorias: Aminoacidos, Perdida de Grasa, Vitaminas, Pro Hormonales, Snacks, Packs
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from playwright.sync_api import Page
from datetime import datetime
from rich import print
import re


class BYONScraperPart2(BaseScraper):
    def __init__(self, headless=False):

        category_urls = {
            "Aminoacidos y BCAA": [
                {"url": "https://www.byon.cl/collections/aminoacidos", "subcategory": "Aminoácidos"},
                {"url": "https://www.byon.cl/collections/bcaa", "subcategory": "BCAA"},
                {"url": "https://www.byon.cl/collections/eaa", "subcategory": "Aminoácidos"},
                {"url": "https://www.byon.cl/collections/glutaminas", "subcategory": "Otros Aminoacidos y BCAA"},
                {"url": "https://www.byon.cl/collections/hmb", "subcategory": "Otros Aminoacidos y BCAA"},
                {"url": "https://www.byon.cl/collections/taurina", "subcategory": "Otros Aminoacidos y BCAA"},
            ],
            "Perdida de Grasa": [
                {"url": "https://www.byon.cl/collections/dieta-y-quemadores", "subcategory": "Quemadores"},
                {"url": "https://www.byon.cl/collections/carnitinas", "subcategory": "L-Carnitina"},
                {"url": "https://www.byon.cl/collections/cla", "subcategory": "Quemadores"},
                {"url": "https://www.byon.cl/collections/quemadores-nocturnos", "subcategory": "Quemadores"},
                {"url": "https://www.byon.cl/collections/libre-de-estimulantes", "subcategory": "Quemadores"},
            ],
            "Vitaminas y Minerales": [
                {"url": "https://www.byon.cl/collections/vitaminas-y-salud", "subcategory": "Vitaminas y Minerales"},
                {"url": "https://www.byon.cl/collections/multivitaminicos", "subcategory": "Multivitaminicos"},
                {"url": "https://www.byon.cl/collections/vitaminas-a-b-c-d-e", "subcategory": "Vitaminas y Minerales"},
                {"url": "https://www.byon.cl/collections/colagenos", "subcategory": "Colágeno"},
                {"url": "https://www.byon.cl/collections/omegas", "subcategory": "Omega 3 y Probióticos"},
                {"url": "https://www.byon.cl/collections/adaptogenos", "subcategory": "Vitaminas y Minerales"},
                {"url": "https://www.byon.cl/collections/magnesios-y-zincs", "subcategory": "Vitaminas y Minerales"},
                {"url": "https://www.byon.cl/collections/minerales", "subcategory": "Vitaminas y Minerales"},
                {"url": "https://www.byon.cl/collections/probioticos", "subcategory": "Omega 3 y Probióticos"},
                {"url": "https://www.byon.cl/collections/antioxidantes", "subcategory": "Vitaminas y Minerales"},
                {"url": "https://www.byon.cl/collections/zma", "subcategory": "Vitaminas y Minerales"},
                {"url": "https://www.byon.cl/collections/biotin", "subcategory": "Vitaminas y Minerales"},
            ],
            "Pro Hormonales": [
                {"url": "https://www.byon.cl/collections/pro-hormonales", "subcategory": "Pro Hormonales"},
            ],
            "Snacks y Comida": [
                {"url": "https://www.byon.cl/collections/barras-de-proteina-1", "subcategory": "Barritas Y Snacks Proteicas"},
                {"url": "https://www.byon.cl/collections/geles-y-snacks", "subcategory": "Snacks Dulces"},
                {"url": "https://www.byon.cl/collections/snacks-y-endurence", "subcategory": "Bebidas Nutricionales"},
            ],
            "Packs": [
                {"url": "https://www.byon.cl/collections/packs", "subcategory": "Packs"},
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
            "vendor": "[class*='vendor']",
            "description": ".product__description",
            "sku_ld": "script[type='application/ld+json']",
        }

        super().__init__(
            base_url="https://www.byon.cl",
            headless=headless,
            category_urls=category_urls,
            selectors=selectors,
            site_name="BYON",
            output_suffix="_part2"
        )
        self.classifier = CategoryClassifier()

    def extract_process(self, page: Page):
        print(f"[green]Iniciando scraping BYON Parte 2/2 (Aminoacidos, Quemadores, Vitaminas, Snacks, Packs)...[/green]")
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

                            link = "N/D"
                            link_el = producto.locator(self.selectors['link'])
                            if link_el.count() > 0:
                                href = link_el.first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href

                            if link != "N/D" and link in self.seen_urls:
                                continue
                            if link != "N/D":
                                self.seen_urls.add(link)

                            title = "N/D"
                            name_el = producto.locator(self.selectors['product_name'])
                            if name_el.count() > 0:
                                raw_title = name_el.first.inner_text()
                                title = self.clean_text(raw_title)

                            thumbnail_url = ""
                            img_el = producto.locator(self.selectors['thumbnail'])
                            if img_el.count() > 0:
                                img = img_el.first
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
                                    if raw_img.startswith('//'): raw_img = "https:" + raw_img
                                    clean_img = raw_img.split('?')[0]
                                    clean_img = re.sub(r'_\d+x(\d+)?', '', clean_img)
                                    thumbnail_url = clean_img

                            price = 0
                            active_discount = False
                            is_on_sale = producto.locator('.price--on-sale').count() > 0
                            if is_on_sale:
                                active_discount = True
                                sale_el = producto.locator('.price-item--sale')
                                if sale_el.count() > 0:
                                    clean_p = re.sub(r'[^\d]', '', sale_el.first.inner_text())
                                    if clean_p: price = int(clean_p)
                            else:
                                reg_el = producto.locator('.price-item--regular')
                                if reg_el.count() > 0:
                                    clean_p = re.sub(r'[^\d]', '', reg_el.first.inner_text())
                                    if clean_p: price = int(clean_p)

                            brand = "N/D"
                            image_url = ""
                            sku = ""
                            description = ""

                            if link != "N/D":
                                detail_page = None
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)

                                    try:
                                        vendor_el = detail_page.locator(self.selectors['vendor']).first
                                        if vendor_el.count() > 0:
                                            raw_brand = vendor_el.inner_text().strip()
                                            if raw_brand: brand = self.clean_text(raw_brand)
                                    except Exception: pass

                                    try:
                                        og_img = detail_page.locator('meta[property="og:image"]').first.get_attribute('content')
                                        if og_img: image_url = og_img
                                    except Exception: pass

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
                                            if json_img: image_url = json_img
                                        except Exception: pass

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
                                        if sku_val: sku = str(sku_val).strip()
                                    except Exception: pass

                                    try:
                                        desc_el = detail_page.locator(self.selectors['description']).first
                                        if desc_el.count() > 0:
                                            description = desc_el.inner_text().strip()
                                    except Exception: pass

                                    if price == 0:
                                        try:
                                            jsonld_price = detail_page.evaluate('''() => {
                                                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                                for (const s of scripts) {
                                                    try {
                                                        const d = JSON.parse(s.innerText);
                                                        const offers = d.offers ? (Array.isArray(d.offers) ? d.offers : [d.offers]) : [];
                                                        for (const o of offers) {
                                                            if (o.price && parseFloat(o.price) > 0) return parseFloat(o.price);
                                                        }
                                                    } catch(e) {}
                                                }
                                                return null;
                                            }''')
                                            if jsonld_price and float(jsonld_price) > 0:
                                                price = int(float(jsonld_price))
                                        except Exception: pass

                                    detail_page.close()
                                except Exception as e:
                                    print(f"[yellow]Error cargando detalle {link}: {e}[/yellow]")
                                    if detail_page:
                                        try: detail_page.close()
                                        except: pass

                            brand = self.enrich_brand(brand, title)
                            site_folder = self.site_name.replace(" ", "_").lower()
                            if thumbnail_url:
                                local_thumb = self.download_image(thumbnail_url, subfolder=site_folder)
                                if local_thumb: thumbnail_url = local_thumb
                            if image_url:
                                local_img = self.download_image(image_url, subfolder=site_folder)
                                if local_img: image_url = local_img

                            final_category, final_sub = self.classifier.classify(
                                title, description, main_category, deterministic_sub, brand
                            )

                            yield {
                                'date': current_date, 'site_name': self.site_name,
                                'category': self.clean_text(final_category), 'subcategory': final_sub,
                                'product_name': title, 'brand': brand, 'price': price, 'link': link,
                                'rating': "0", 'reviews': "0", 'active_discount': active_discount,
                                'thumbnail_image_url': thumbnail_url, 'image_url': image_url,
                                'sku': sku, 'description': description
                            }

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
                                next_btn.first.click()
                                page.wait_for_timeout(3000)
                                page_number += 1
                        else:
                            print("  > No hay más páginas.")
                            break

                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")


if __name__ == "__main__":
    scraper = BYONScraperPart2(headless=True)
    scraper.run()
