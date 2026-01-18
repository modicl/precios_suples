from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import json

class MuscleFactoryScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        category_urls = {
            "Proteinas": [
                "https://www.musclefactory.cl/proteinas"
            ],
            "Creatinas": [
                "https://www.musclefactory.cl/productos/creatina"
            ],
            "Pre Entrenos": [
                "https://www.musclefactory.cl/productos/pre-entrenamientos"
            ],
            "Vitaminas": [
                "https://www.musclefactory.cl/vitaminas-y-minerales-🌞"
            ],
            "Por Objetivo": [
                "https://www.musclefactory.cl/por-objetivo-%F0%9F%8E%AF"
            ],
            "Ofertas": [
                "https://www.musclefactory.cl/ofertas"
            ],
            "Packs": [
                "https://www.musclefactory.cl/packs"
            ]
        }

        # Selectores Jumpseller
        selectors = {
            "product_card": ".product-block",
            "product_name": ".product-block__name",
            "price_container": ".product-block__price",
            "price_new": ".product-block__price--new", # Final price (with discount)
            "price_old": ".product-block__price--old", # Old price (crossed out)
            "link": ".product-block__anchor",
            "image": ".product-block__image img",
            
            # Detail page
            "detail_sku_json": ".product-json", # Jumpseller standard JSON data
            "detail_desc": ".product-page__description",
            "detail_image": ".product-gallery__image img",
            "detail_brand": ".product-page__brand",
            "detail_title": "h1.product-page__title",
            
            # Pagination
            "next_button": ".pager .next, .pager a:has-text('»')" 
        }

        super().__init__(base_url, headless, category_urls, selectors, site_name="MuscleFactory")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en MuscleFactory...[/green]")
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
                            # Sometimes product-block might load a bit slowly
                            page.wait_for_selector(self.selectors['product_card'], timeout=6000)
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
                                title = card.locator(self.selectors['product_name']).first.inner_text().strip()

                            link = "N/D"
                            if card.locator(self.selectors['link']).count() > 0:
                                href = card.locator(self.selectors['link']).first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href

                            # Price extraction
                            price = 0
                            active_discount = False
                            price_text = "0"

                            # Logic: price--new is usually the final price. price--old exists if there is a discount.
                            # If only standard price exists, it might be in price--new or just generic price container logic.
                            
                            new_price_el = card.locator(self.selectors['price_new']).first
                            old_price_el = card.locator(self.selectors['price_old']).first
                            
                            if new_price_el.count() > 0:
                                price_text = new_price_el.inner_text()
                                if old_price_el.count() > 0:
                                    active_discount = True
                            elif card.locator(self.selectors['price_container']).count() > 0:
                                # Fallback if specific classes aren't used
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
                            brand = "N/D" 

                            if link != "N/D":
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                                    
                                    # SKU Strategy: Try JSON first (Jumpseller standard), then DOM
                                    # 1. JSON
                                    try:
                                        json_el = detail_page.locator(self.selectors['detail_sku_json']).first
                                        if json_el.count() > 0:
                                            json_data = json.loads(json_el.inner_text())
                                            if isinstance(json_data, list) and len(json_data) > 0:
                                                sku = json_data[0].get('variant', {}).get('sku', '')
                                    except:
                                        pass
                                    
                                    # 2. DOM fallback
                                    if not sku:
                                        # Often SKU is not easily selector-able in DOM on some templates, 
                                        # but let's try a common pattern if needed
                                        pass

                                    # Description
                                    if detail_page.locator(self.selectors['detail_desc']).count() > 0:
                                        description = detail_page.locator(self.selectors['detail_desc']).first.inner_text().strip()

                                    # Brand 
                                    if detail_page.locator(self.selectors['detail_brand']).count() > 0:
                                        brand = detail_page.locator(self.selectors['detail_brand']).first.inner_text().strip()
                                    
                                    # Main Image
                                    if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                        img_src = detail_page.locator(self.selectors['detail_image']).first.get_attribute("src")
                                        if img_src:
                                            image_url = "https:" + img_src if img_src.startswith('//') else img_src
                                    
                                    if not image_url and thumbnail_url:
                                        image_url = thumbnail_url.replace('240x240', '1024x1024') 

                                    detail_page.close()

                                except Exception as e:
                                    print(f"[yellow]Error consiguiendo detalles de {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass
                            
                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': main_category,
                                'subcategory': main_category, 
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
                        # Jumpseller usually has a 'next' class or we look for text
                        next_btn = page.locator(".pager a").filter(has_text="»").first
                        if next_btn.count() == 0:
                             next_btn = page.locator(".next").first

                        if next_btn.count() > 0 and next_btn.is_visible():
                            href_next = next_btn.get_attribute("href")
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
    scraper = MuscleFactoryScraper("https://www.musclefactory.cl", headless=True)
    scraper.run()
