from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class FitMarketChileScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        category_urls = {
            "Proteinas": [
                "https://fitmarketchile.cl/categoria-producto/proteinas"
            ],
            "Creatinas": [
                "https://fitmarketchile.cl/categoria-producto/creatina"
            ],
            "Aminoacidos y BCAA": [
                "https://fitmarketchile.cl/categoria-producto/aminoacidos-bcaa"
            ],
            "Perdida de Grasa": [
                "https://fitmarketchile.cl/categoria-producto/quemador-de-grasa"
            ],
            "Pre Entrenos": [
                "https://fitmarketchile.cl/categoria-producto/pre-entrenos"
            ],
            "Ganadores de Peso": [
                "https://fitmarketchile.cl/categoria-producto/ganador-de-masa"
            ],
            "Vitaminas y Minerales": [
                "https://fitmarketchile.cl/categoria-producto/vitaminas"
            ],
            "Ofertas": [
                "https://fitmarketchile.cl/categoria-producto/cuber-day"
            ],
            "Snacks y Comida": [
                "https://fitmarketchile.cl/categoria-producto/barras-de-proteina-snack"
            ]

        }

        # Selectores WooCommerce / WoodMart
        selectors = {
            "product_card": ".product-grid-item", 
            "product_name": ".wd-entities-title a",
            "price_container": ".price",
            # Price logic:
            # .price > .woocommerce-Price-amount bdi (Standard)
            # .price > ins > .woocommerce-Price-amount bdi (Sale Price / Final)
            # .price > del > .woocommerce-Price-amount bdi (Old Price)
            
            "link": ".product-image-link", 
            "image": ".product-image-link img",
            
            # Detail page
            "detail_sku": ".sku",
            "detail_desc": ".woocommerce-product-details__short-description, #tab-description",
            "detail_image": ".woocommerce-product-gallery__image img",
            "detail_brand": "N/D", # Not explicitly standardized
            "detail_title": "h1.product_title",
            
            # Out of stock
            "out_of_stock": ".out-of-stock",

            # Pagination
            "next_button": "a.next.page-numbers"
        }

        super().__init__(base_url, headless, category_urls, selectors, site_name="FitMarketChile")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en FitMarketChile...[/green]")
        context = page.context

        for main_category, urls in self.category_urls.items():
            for url in urls:
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} ({url})")
                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")
                        try:
                            # Wait for grid
                            page.wait_for_selector(self.selectors['product_card'], timeout=6000)
                        except:
                            print(f"[red]No se encontraron productos en la página {page_number} de {url}[/red]")
                            break
                        
                        cards = page.locator(self.selectors['product_card'])
                        count = cards.count()
                        print(f"  > Encontrados {count} productos.")

                        for i in range(count):
                            card = cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            # 1. Basic Info from Grid
                            title = "N/D"
                            if card.locator(self.selectors['product_name']).count() > 0:
                                raw_title = card.locator(self.selectors['product_name']).first.inner_text()
                                title = self.clean_text(raw_title)

                            link = "N/D"

                            if card.locator(self.selectors['link']).count() > 0:
                                href = card.locator(self.selectors['link']).first.get_attribute("href")
                                if href:
                                    link = href

                            # Price extraction
                            price = 0
                            active_discount = False
                            price_text = "0"

                            # Logic for WooCommerce with ins/del
                            price_container = card.locator(self.selectors['price_container']).first
                            
                            # Check for discount (ins tag exists?)
                            ins_el = price_container.locator("ins .woocommerce-Price-amount bdi")
                            del_el = price_container.locator("del .woocommerce-Price-amount bdi")
                            
                            if ins_el.count() > 0:
                                active_discount = True
                                price_text = ins_el.first.inner_text()
                                # Check old price presence mostly to confirm discount logic, but ins is enough usually
                            else:
                                # Regular price (no discount)
                                # Often direct child or just one amount
                                reg_el = price_container.locator(".woocommerce-Price-amount bdi").first
                                if reg_el.count() > 0:
                                    price_text = reg_el.inner_text()

                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

                            # Out of stock check: if 'agotado' badge is present, price is 0
                            if card.locator(self.selectors['out_of_stock']).count() > 0:
                                price = 0
                                active_discount = False

                            # Thumbnail
                            thumbnail_url = ""
                            if card.locator(self.selectors['image']).count() > 0:
                                src = card.locator(self.selectors['image']).first.get_attribute("src")
                                if src:
                                    thumbnail_url = src

                            # 2. Detail Extraction
                            image_url = ""
                            sku = ""
                            description = ""
                            brand = "N/D" 

                            if link != "N/D":
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                                    
                                    # SKU
                                    if detail_page.locator(self.selectors['detail_sku']).count() > 0:
                                        sku_text = detail_page.locator(self.selectors['detail_sku']).first.inner_text()
                                        sku = sku_text.replace('SKU:', '').strip()

                                    # Description
                                    if detail_page.locator(self.selectors['detail_desc']).count() > 0:
                                        description = detail_page.locator(self.selectors['detail_desc']).first.inner_text().strip()

                                    # Brand - Attempt to find distinct brand element or infer
                                    # Sometimes Woo themes put brand in meta or tags. 
                                    # We can leave as N/D unless a specific selector is found found widely.
                                    
                                    # Main Image
                                    if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                        # Use first image
                                        img_src = detail_page.locator(self.selectors['detail_image']).first.get_attribute("src")
                                        if img_src:
                                            image_url = img_src
                                    
                                    if not image_url and thumbnail_url:
                                        image_url = thumbnail_url

                                    detail_page.close()

                                except Exception as e:
                                    print(f"[yellow]Error consiguiendo detalles de {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass
                            
                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(main_category),
                                'subcategory': self.clean_text(main_category), 
                                'product_name': title,

                                'brand': self.enrich_brand(self.clean_text(brand), title),
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
                        
                        # Pagination Logic
                        next_btn = page.locator(self.selectors['next_button'])

                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href_next = next_btn.first.get_attribute("href")
                            if href_next:
                                print(f"Navegando a página siguiente: {href_next}")
                                page.goto(href_next)
                                page_number += 1
                            else:
                                break
                        else:
                            break

                except Exception as e:
                    print(f"[red]Error procesando categoría {main_category}: {e}[/red]")

if __name__ == "__main__":
    scraper = FitMarketChileScraper("https://fitmarketchile.cl", headless=True)
    scraper.run()
