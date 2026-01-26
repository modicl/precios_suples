from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class SuplementosBullChileScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        category_urls = {

            "Proteinas": [
                "https://www.suplementosbullchile.cl/proteinas"
            ],
            "Creatinas": [
                "https://www.suplementosbullchile.cl/creatina"
            ],
            "Pre Entrenos": [
                "https://www.suplementosbullchile.cl/pre-entreno"
            ],
            "Perdida de Grasa": [
                "https://www.suplementosbullchile.cl/quemador-energetico-1"
            ],

            "Packs": [
                "https://www.suplementosbullchile.cl/packs"
            ],
        }

        # Selectores identificados
        selectors = {
            "product_card": ".product-block",
            "product_name": ".product-block__name",
            "price_container": ".product-block__price", 
            "price_new": ".product-block__price--new", # Corrected BEM selector
            "price_old": ".product-block__price--old", # Corrected BEM selector
            "link": ".product-block__anchor",
            "image": ".product-block__image img",
            
            # Detail page
            "detail_sku": ".product-page__sku",
            "detail_desc": ".product-page__description",
            "detail_image": ".product-gallery__slide img",
            "detail_brand": ".product-page__brand", # Sometimes present
            "detail_title": "h1.product-page__title",
            # Pagination (Jumpseller standard often uses .pagination)
            "next_button": ".pagination .next a, a.next, .pagination .next" 
        }

        super().__init__(base_url, headless, category_urls, selectors, site_name="SuplementosBullChile")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en SuplementosBullChile...[/green]")
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
                            page.wait_for_selector(self.selectors['product_card'], timeout=5000)
                        except:
                            print(f"[red]No se encontraron productos en la página {page_number} de {url}[/red]")
                            break
                        
                        cards = page.locator(self.selectors['product_card'])
                        count = cards.count()
                        print(f"  > Encontrados {count} productos.")

                        for i in range(count):
                            card = cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d")

                            # 1. Basic Info from Grid
                            title = "N/D"
                            if card.locator(self.selectors['product_name']).count() > 0:
                                raw_title = card.locator(self.selectors['product_name']).first.inner_text()
                                title = self.clean_text(raw_title)

                            link = "N/D"

                            if card.locator(self.selectors['link']).count() > 0:
                                href = card.locator(self.selectors['link']).first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href

                            # Price extraction
                            price = 0
                            active_discount = False
                            price_text = "0"

                            # Try new price (sale price) - this is the final price
                            new_price_el = card.locator(self.selectors['price_new']).first
                            old_price_el = card.locator(self.selectors['price_old']).first

                            if new_price_el.count() > 0:
                                price_text = new_price_el.inner_text()
                                # IF there is also an old price, it's a discount
                                if old_price_el.count() > 0:
                                    active_discount = True
                            
                            # Fallback: if no specific new/old classes, try generic or old price as main if only that exists (unlikely in this theme)
                            elif old_price_el.count() > 0:
                                # Weird case, but if only old price exists, maybe it's not a discount? 
                                # Usually old price implies there is a new price. 
                                # Let's fallback to container if new price missing
                                if card.locator(self.selectors['price_container']).count() > 0:
                                     price_text = card.locator(self.selectors['price_container']).first.inner_text()
                            elif card.locator(self.selectors['price_container']).count() > 0:
                                price_text = card.locator(self.selectors['price_container']).first.inner_text()
                            
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

                            # Thumbnail
                            thumbnail_url = ""
                            if card.locator(self.selectors['image']).count() > 0:
                                src = card.locator(self.selectors['image']).first.get_attribute("src")
                                if src:
                                    thumbnail_url = "https:" + src if src.startswith('//') else src

                            # 2. Detail Extraction
                            image_url = ""
                            sku = ""
                            description = ""
                            brand = "N/D" # Brand often not in grid

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

                                    # Brand (Try selector)
                                    if detail_page.locator(self.selectors['detail_brand']).count() > 0:
                                        raw_brand = detail_page.locator(self.selectors['detail_brand']).first.inner_text()
                                        brand = self.clean_text(raw_brand)
                                    
                                    # Main Image

                                    if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                        # Often these are sliders, take first
                                        img_src = detail_page.locator(self.selectors['detail_image']).first.get_attribute("src")
                                        if img_src:
                                            image_url = "https:" + img_src if img_src.startswith('//') else img_src
                                    
                                    # Fallback if no specific main image found, use thumbnail high res
                                    if not image_url and thumbnail_url:
                                        image_url = thumbnail_url.replace('240x240', '1024x1024') # Jumpseller/Shopify common pattern logic attempt

                                    detail_page.close()

                                except Exception as e:
                                    print(f"[yellow]Error consiguiendo detalles de {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass
                            
                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(main_category),
                                'subcategory': self.clean_text(main_category), # No subcategories explicitly handled yet
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
                        
                        # Pagination Logic
                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href_next = next_btn.first.get_attribute("href")
                            if href_next:
                                print(f"Navegando a página siguiente: {href_next}")
                                page.goto(href_next if href_next.startswith('http') else self.base_url + href_next)
                                page_number += 1
                            else:
                                break
                        else:
                            break

                except Exception as e:
                    print(f"[red]Error procesando categoría {main_category}: {e}[/red]")

if __name__ == "__main__":
    scraper = SuplementosBullChileScraper("https://www.suplementosbullchile.cl", headless=True)
    scraper.run()
