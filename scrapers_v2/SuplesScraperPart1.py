# Scraper para Suples.cl (Parte 1 de 2)
# Categorias: Proteinas, Creatinas, Pre Entrenos, Ganadores de Peso, Aminoacidos y BCAA
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from rich import print
from datetime import datetime
import re


class SuplesScraperPart1(BaseScraper):
    def __init__(self, headless=False):

        category_urls = {
            "Proteinas": [
                {"url": "https://www2.suples.cl/collections/proteina-whey", "subcategory": "Proteína de Whey"},
                {"url": "https://www2.suples.cl/collections/proteina-isolate", "subcategory": "Proteína Aislada"},
                {"url": "https://www2.suples.cl/collections/proteinas-hidrolizadas", "subcategory": "Proteína Hidrolizada"},
                {"url": "https://www2.suples.cl/collections/proteinas-caseinas", "subcategory": "Caseína"},
                {"url": "https://www2.suples.cl/collections/proteinas-de-carne", "subcategory": "Proteína de Carne"},
                {"url": "https://www2.suples.cl/collections/proteinas-veganas", "subcategory": "Proteína Vegana"},
                {"url": "https://www2.suples.cl/collections/proteinas-liquidas", "subcategory": "Shakes Proteicos"},
            ],
            "Creatinas": [
                {"url": "https://www2.suples.cl/collections/creatinas", "subcategory": "Creatinas"},
            ],
            "Pre Entrenos": [
                {"url": "https://www2.suples.cl/collections/pre-workout", "subcategory": "Pre Entreno"},
                {"url": "https://www2.suples.cl/collections/arginina", "subcategory": "Óxido Nítrico"},
            ],
            "Ganadores de Peso": [
                {"url": "https://www2.suples.cl/collections/ganadores-de-masa", "subcategory": "Ganadores de Peso"},
            ],
            "Aminoacidos y BCAA": [
                {"url": "https://www2.suples.cl/collections/aminoacidos", "subcategory": "Aminoácidos"},
                {"url": "https://www2.suples.cl/collections/aminoacidos-y-nutrientes-esenciales", "subcategory": "Otros Aminoacidos y BCAA"},
                {"url": "https://www2.suples.cl/collections/hmb", "subcategory": "Otros Aminoacidos y BCAA"},
                {"url": "https://www2.suples.cl/collections/zma", "subcategory": "Minerales (Magnesio/ZMA)"},
            ],
        }

        selectors = {
            "product_grid": ".collection-list",
            "product_card": ".product-item",
            "product_name": ".product-item__title",
            "brand": ".product-item__vendor",
            "price_container": ".product-item__price-list",
            "price_highlight": ".price--highlight",
            "price_compare": ".price--compare",
            "price_default": ".price",
            "link": "a.product-item__title",
            "next_button": ".pagination__next",
            "thumbnail": "img.product-item__primary-image",
        }

        super().__init__(
            base_url="https://www2.suples.cl",
            headless=headless,
            category_urls=category_urls,
            selectors=selectors,
            site_name="Suples.cl",
            output_suffix="_part1",
        )
        self.classifier = CategoryClassifier()

    def extract_process(self, page):
        print(f"[green]Iniciando scraping Suples.cl Parte 1/2 (Proteinas, Creatinas, PreEntrenos, Ganadores, Aminoacidos)...[/green]")
        context = page.context

        for main_category, items in self.category_urls.items():
            for item in items:
                url = item["url"]
                deterministic_sub = item["subcategory"]
                print(f"\n[bold blue]Procesando:[/bold blue] {main_category} -> {deterministic_sub} ({url})")

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")
                        try:
                            page.wait_for_selector(self.selectors["product_card"], timeout=60000)
                        except Exception:
                            print(f"[red]No se encontraron productos en {url} o tardó demasiado.[/red]")
                            break

                        producto_cards = page.locator(self.selectors["product_card"])
                        count = producto_cards.count()
                        print(f"  > Encontrados {count} productos en esta página.")

                        for i in range(count):
                            producto = producto_cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            link = "N/D"
                            if producto.locator(self.selectors["link"]).count() > 0:
                                href = producto.locator(self.selectors["link"]).first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith("/") else href

                            if link != "N/D" and link in self.seen_urls:
                                continue
                            if link != "N/D":
                                self.seen_urls.add(link)

                            title = "N/D"
                            if producto.locator(self.selectors["product_name"]).count() > 0:
                                raw_title = producto.locator(self.selectors["product_name"]).first.inner_text()
                                title = self.clean_text(raw_title)

                            brand = "N/D"
                            if producto.locator(self.selectors["brand"]).count() > 0:
                                raw_brand = producto.locator(self.selectors["brand"]).first.inner_text()
                                brand = self.clean_text(raw_brand)

                            thumbnail_url = ""
                            if producto.locator(self.selectors["thumbnail"]).count() > 0:
                                t_el = producto.locator(self.selectors["thumbnail"]).first
                                t_srcset = t_el.get_attribute("data-srcset")
                                t_src = t_el.get_attribute("data-src") or t_el.get_attribute("src")
                                final_src = ""
                                if t_srcset:
                                    candidates = t_srcset.split(",")
                                    if candidates:
                                        final_src = candidates[-1].strip().split(" ")[0]
                                elif t_src:
                                    if "{width}" in t_src:
                                        final_src = t_src.replace("{width}", "500")
                                    else:
                                        final_src = t_src
                                if final_src:
                                    thumbnail_url = "https:" + final_src if final_src.startswith("//") else final_src

                            price = 0
                            active_discount = False
                            price_container = producto.locator(self.selectors["price_container"])
                            if price_container.count() > 0:
                                highlight = price_container.locator(self.selectors["price_highlight"])
                                if highlight.count() > 0:
                                    price_text = highlight.first.inner_text()
                                    active_discount = True
                                else:
                                    normal = price_container.locator(self.selectors["price_default"])
                                    price_text = normal.first.inner_text() if normal.count() > 0 else "0"
                            else:
                                general_price = producto.locator(self.selectors["price_default"])
                                price_text = general_price.first.inner_text() if general_price.count() > 0 else "0"

                            clean_price = re.sub(r"[^\d]", "", price_text)
                            if clean_price:
                                price = int(clean_price)

                            image_url = ""
                            sku = ""
                            description = ""
                            detail_page = None

                            if link != "N/D":
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)

                                    try:
                                        og_img = detail_page.locator('meta[property="og:image"]').first.get_attribute("content")
                                        if og_img:
                                            image_url = og_img
                                    except Exception: pass

                                    if not image_url:
                                        try:
                                            json_img = detail_page.evaluate('''() => {
                                                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                                for (const s of scripts) {
                                                    try {
                                                        const d = JSON.parse(s.innerText);
                                                        if ((d['@type'] === 'Product' || d['@type'] === 'ProductGroup') && d.image)
                                                            return Array.isArray(d.image) ? d.image[0] : d.image;
                                                    } catch(e){}
                                                }
                                                return null;
                                            }''')
                                            if json_img:
                                                image_url = json_img
                                        except Exception: pass

                                    if not image_url:
                                        img_el = detail_page.locator(".product-gallery__image img, .product-gallery__featured-image img").first
                                        if img_el.count() > 0:
                                            src = img_el.get_attribute("src")
                                            if src and "base64" not in src:
                                                image_url = "https:" + src if src.startswith("//") else src
                                            else:
                                                data_zoom = img_el.get_attribute("data-zoom")
                                                if data_zoom:
                                                    image_url = "https:" + data_zoom if data_zoom.startswith("//") else data_zoom

                                    sku_el = detail_page.locator(".product-meta__sku").first
                                    if sku_el.count() > 0:
                                        sku = sku_el.inner_text().strip().replace("SKU:", "").strip()

                                    desc_el = detail_page.locator(".product-description, .rte").first
                                    if desc_el.count() > 0:
                                        description = desc_el.inner_text().strip()

                                    detail_page.close()
                                except Exception as e:
                                    print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                    if detail_page:
                                        try: detail_page.close()
                                        except: pass

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
                                "date": current_date, "site_name": self.site_name,
                                "category": self.clean_text(final_category), "subcategory": final_sub,
                                "product_name": title, "brand": self.enrich_brand(brand, title),
                                "price": price, "link": link,
                                "rating": "0", "reviews": "0", "active_discount": active_discount,
                                "thumbnail_image_url": thumbnail_url, "image_url": image_url,
                                "sku": sku, "description": description,
                            }

                        next_btn = page.locator(self.selectors["next_button"])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href = next_btn.first.get_attribute("href")
                            if href:
                                page.goto(self.base_url + href if href.startswith("/") else href)
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
    scraper = SuplesScraperPart1(headless=True)
    scraper.run()
