# Scraper para AllNutrition.cl (Parte 1 de 2)
# Categorias: Proteinas, Creatinas, Ganadores de Peso
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from rich import print
from datetime import datetime
import re
import random
import time


class AllNutritionScraperPart1(BaseScraper):
    def __init__(self, headless=False):

        self.category_urls = {
            "Proteinas": [
                {"url": "https://allnutrition.cl/collections/whey-protein", "subcategory": "Whey Protein"},
                {"url": "https://allnutrition.cl/collections/proteinas-isoladas", "subcategory": "Proteinas Isoladas"},
                {"url": "https://allnutrition.cl/collections/proteinas-de-carne", "subcategory": "Proteinas De Carne"},
                {"url": "https://allnutrition.cl/collections/proteinas-liquidas", "subcategory": "Proteinas Liquidas"},
                {"url": "https://allnutrition.cl/collections/proteinas-veganas", "subcategory": "Proteinas Veganas"},
                {"url": "https://allnutrition.cl/collections/proteinas-vegetarianas", "subcategory": "Proteinas Vegetarianas"},
                {"url": "https://allnutrition.cl/collections/barras-proteicas", "subcategory": "Barras Proteicas"},
                {"url": "https://allnutrition.cl/collections/snack-proteico", "subcategory": "Snack Proteico"}
            ],
            "Creatinas": [
                {"url": "https://allnutrition.cl/collections/creatinas", "subcategory": "Creatinas"}
            ],
            "Ganadores de Peso": [
                {"url": "https://allnutrition.cl/collections/ganadores-de-peso", "subcategory": "Ganadores De Peso"}
            ]
        }

        selectors = {
            'product_card': '.c-card-product',
            'title': '.c-card-producto__title h6',
            'title_secondary': '.c-card-product__title',
            'vendor': '.c-card-product__vendor',
            'price': '.c-card-product__price',
            'price_old_nested': '.c-card-product__price-old',
            'link': 'a.link--not-decoration',
            'thumbnail': '.c-card-product__image img',
            'rating': '.rating .rating-star',
            'reviews': '.rating-text-count',
            'active_discount': '.c-card-product__discount',
            'next_button': 'a[aria-label="Página siguiente"]'
        }

        super().__init__("https://allnutrition.cl", headless, self.category_urls, selectors,
                         site_name="AllNutrition", output_suffix="_part1")
        self.classifier = CategoryClassifier()

    def _goto_with_retry(self, page, url, wait_until="domcontentloaded", timeout=60000, max_retries=3):
        for attempt in range(1, max_retries + 1):
            try:
                page.goto(url, wait_until=wait_until, timeout=timeout)
                return
            except Exception as e:
                if attempt == max_retries:
                    raise
                wait_sec = (2 ** attempt) + random.uniform(1, 3)
                print(f"[yellow]  >> goto falló (intento {attempt}/{max_retries}): {e}. Reintentando en {wait_sec:.1f}s...[/yellow]")
                time.sleep(wait_sec)

    def extract_process(self, page):
        print(f"[green]Iniciando scraping AllNutrition Parte 1/2 (Proteinas, Creatinas, Ganadores)...[/green]")
        context = page.context

        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                deterministic_subcategory = item['subcategory']
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {deterministic_subcategory} ({url})")

                try:
                    delay = random.uniform(3, 8)
                    print(f"  > Esperando {delay:.1f}s antes de la siguiente categoría...")
                    time.sleep(delay)

                    self._goto_with_retry(page, url, wait_until="domcontentloaded", timeout=60000)
                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")
                        try:
                            page.wait_for_selector(self.selectors['product_card'], timeout=8000)
                        except:
                            print(f"[red]No se encontraron productos en {url} o tardó demasiado.[/red]")
                            break

                        producto_cards = page.locator(self.selectors['product_card'])
                        count = producto_cards.count()
                        print(f"  > Encontrados {count} productos en esta página.")

                        for i in range(count):
                            producto = producto_cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            title = "N/D"
                            if producto.locator(self.selectors['title']).count() > 0:
                                raw_title = producto.locator(self.selectors['title']).first.inner_text()
                                title = self.clean_text(raw_title)
                            elif producto.locator(self.selectors['title_secondary']).count() > 0:
                                raw_title = producto.locator(self.selectors['title_secondary']).first.inner_text()
                                title = self.clean_text(raw_title)

                            brand = "N/D"
                            if producto.locator(self.selectors['vendor']).count() > 0:
                                raw_brand = producto.locator(self.selectors['vendor']).first.inner_text()
                                brand = self.clean_text(raw_brand)

                            link = "N/D"
                            if producto.locator(self.selectors['link']).count() > 0:
                                href = producto.locator(self.selectors['link']).first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href

                            if link != "N/D" and link in self.seen_urls:
                                print(f"[yellow]  >> Producto duplicado omitido: {title}[/yellow]")
                                continue
                            if link != "N/D":
                                self.seen_urls.add(link)

                            thumbnail_url = ""
                            if producto.locator(self.selectors['thumbnail']).count() > 0:
                                thumb_src = producto.locator(self.selectors['thumbnail']).first.get_attribute('src')
                                if thumb_src:
                                    thumbnail_url = "https:" + thumb_src if thumb_src.startswith('//') else thumb_src

                            price = 0
                            price_elem = producto.locator(self.selectors['price'])
                            if price_elem.count() > 0:
                                try:
                                    price_text = price_elem.first.evaluate(f"""(el, oldSelector) => {{
                                        const clone = el.cloneNode(true);
                                        const old = clone.querySelector(oldSelector);
                                        if (old) old.remove();
                                        return clone.innerText.trim();
                                    }}""", self.selectors['price_old_nested'])
                                    clean_price = re.sub(r'[^\d]', '', price_text)
                                    if clean_price: price = int(clean_price)
                                except:
                                    price_text = price_elem.first.inner_text()
                                    clean_price = re.sub(r'[^\d]', '', price_text)
                                    if clean_price: price = int(clean_price)

                            active_discount = False
                            if producto.locator(self.selectors['active_discount']).count() > 0:
                                active_discount = True

                            image_url = ""
                            sku = ""
                            description = ""

                            if link != "N/D":
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)

                                    img_el = detail_page.locator('.slide:not(.d-none) img, .c-gallery-product__item:not(.d-none) img').first
                                    if img_el.count() > 0:
                                        src = img_el.get_attribute('src')
                                        if src:
                                            image_url = "https:" + src if src.startswith('//') else src

                                    sku_el = detail_page.locator('.s-main-product__sku, .product-sku').first
                                    if sku_el.count() > 0:
                                        sku = sku_el.inner_text().strip()

                                    try:
                                        description_js = detail_page.evaluate(r'''() => {
                                            let parts = [];
                                            document.querySelectorAll('.s-main-product-benefits__item').forEach(el => {
                                                if(el.innerText) parts.push(el.innerText.trim());
                                            });
                                            document.querySelectorAll('.s-main-product-table__content').forEach(el => {
                                                if(el.innerText) parts.push(el.innerText.trim());
                                            });
                                            document.querySelectorAll('.s-main-product-table-compare__accordion-item').forEach(el => {
                                                if(el.innerText) parts.push(el.innerText.trim());
                                            });
                                            if (parts.length === 0) {
                                                const old = document.querySelector('.s-main-product__text-wrapper, .c-product-description');
                                                if (old && old.innerText) parts.push(old.innerText.trim());
                                            }
                                            return parts.length > 0 ? parts.join(' | ').replace(/\n/g, ' ').replace(/\s+/g, ' ') : null;
                                        }''')
                                        if description_js:
                                            description = description_js
                                    except Exception as desc_e:
                                        desc_el = detail_page.locator('.s-main-product__text-wrapper, .c-product-description').first
                                        if desc_el.count() > 0:
                                            description = desc_el.inner_text().strip()

                                    detail_page.close()
                                except Exception as e:
                                    print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass

                            final_category, final_subcategory = self.classifier.classify(
                                title, description, main_category, deterministic_subcategory, brand
                            )

                            site_folder = self.site_name.replace(" ", "_").lower()
                            if thumbnail_url:
                                local_thumb = self.download_image(thumbnail_url, subfolder=site_folder)
                                if local_thumb: thumbnail_url = local_thumb
                            if image_url:
                                local_img = self.download_image(image_url, subfolder=site_folder)
                                if local_img: image_url = local_img

                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(final_category),
                                'subcategory': final_subcategory,
                                'product_name': title,
                                'brand': self.enrich_brand(brand, title),
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

                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href = next_btn.first.get_attribute("href")
                            if href:
                                next_url = self.base_url + href if href.startswith('/') else href
                                self._goto_with_retry(page, next_url, wait_until="domcontentloaded", timeout=60000)
                                page_number += 1
                            else:
                                next_btn.first.click()
                                page_number += 1
                        else:
                            break

                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")


if __name__ == "__main__":
    scraper = AllNutritionScraperPart1(headless=True)
    scraper.run()
