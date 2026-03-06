# Scraper para SportNutriShop.cl (Parte 1 de 3)
# Categorias: Proteinas, Ganadores de Peso
from BaseScraper import BaseScraper
from BrandClassifier import BrandClassifier
from CategoryClassifier import CategoryClassifier
from playwright.sync_api import Page
from datetime import datetime
from rich import print
import re


class SportNutriShopScraperPart1(BaseScraper):
    def __init__(self, headless=False):

        # Parte 1: las colecciones mas densas en productos
        category_urls = {
            "Proteinas": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/proteinas-y-ganadores",
                    "subcategory": "CATEGORIZAR_PROTEINA"
                }
            ],
            "Ganadores de Peso": [
                {
                    "url": "https://www.sportnutrishop.cl/collections/ganadores-de-peso",
                    "subcategory": "Ganadores De Peso"
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
            site_name="SportNutriShop",
            output_suffix="_part1"
        )

        self.classifier = CategoryClassifier()
        self._brand_clf = BrandClassifier()

    def _extract_brand_from_title(self, title: str) -> str:
        if not title:
            return "N/D"
        if " - " in title:
            candidate = title.split(" - ")[-1].strip()
            if candidate and not re.match(r'^\d', candidate) and len(candidate) > 1:
                return self._brand_clf.normalize_brand(candidate)
        return self._brand_clf.extract_from_title(title)

    def extract_process(self, page: Page):
        print(f"[green]Iniciando scraping SportNutriShop Parte 1/3 (Proteinas, Ganadores)...[/green]")

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
                                    clean_img = raw_img.split('?')[0]
                                    clean_img = re.sub(r'_\d+x(\d+)?', '', clean_img)
                                    thumbnail_url = clean_img

                            # --- Price ---
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

                            # --- Detail page: image, SKU, description ---
                            image_url = ""
                            sku = ""
                            description = ""

                            if link != "N/D":
                                detail_page = None
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)

                                    try:
                                        og_img = detail_page.locator('meta[property="og:image"]').first.get_attribute('content')
                                        if og_img:
                                            image_url = og_img
                                    except Exception:
                                        pass

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

                                    if not image_url:
                                        try:
                                            dom_img = detail_page.locator('.product__media img, .product-media-container img').first
                                            if dom_img.count() > 0:
                                                src = dom_img.get_attribute("src")
                                                if src:
                                                    image_url = "https:" + src if src.startswith('//') else src
                                        except Exception:
                                            pass

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

                                    try:
                                        desc_val = detail_page.evaluate('''() => {
                                            const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                            for (const s of scripts) {
                                                try {
                                                    const d = JSON.parse(s.innerText);
                                                    if (d["@type"] === "Product" && d.description)
                                                        return d.description;
                                                } catch(e){}
                                            }
                                            return null;
                                        }''')
                                        if desc_val:
                                            description = str(desc_val).strip()
                                    except Exception:
                                        pass

                                    if not description:
                                        try:
                                            desc_el = detail_page.locator('details .rte, .product__description, .product-single__description').first
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
    scraper = SportNutriShopScraperPart1(headless=True)
    scraper.run()
