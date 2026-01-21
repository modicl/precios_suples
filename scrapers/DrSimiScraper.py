from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import math
import time

class DrSimiScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        # Specific category URLs provided by the user
        self.categories_config = {
            "Pronutrition": "https://www.drsimi.cl/pronutrition",
            "Colageno": "https://www.drsimi.cl/colageno?map=specificationFilter_41",
            "Aceites y Omegas": "https://www.drsimi.cl/suplementos-y-alimentos/aceites-y-omegas",
            "Alimentos": "https://www.drsimi.cl/suplementos-y-alimentos/alimentos",
            "Superalimento": "https://www.drsimi.cl/suplementos-y-alimentos/superalimento",
            "Vitaminas y Minerales": "https://www.drsimi.cl/suplementos-y-alimentos/vitaminas-y-minerales",
            "Bebidas Nutricionales": "https://www.drsimi.cl/suplementos-y-alimentos/bebidas-nutricionales",
            "Deportistas": "https://www.drsimi.cl/suplementos-y-alimentos/deportistas"
        }

        # Selectores VTEX identification
        selectors = {
            "product_card": ".vtex-product-summary-2-x-container",
            "product_name": ".vtex-product-summary-2-x-brandName",
            "price_container": ".vtex-product-price-1-x-sellingPriceValue--summary",
            "link": "a.vtex-product-summary-2-x-clearLink",
            "image": "img.vtex-product-summary-2-x-image",
        }

        super().__init__(base_url, headless, category_urls=list(self.categories_config.values()), selectors=selectors, site_name="Dr Simi")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.categories_config)} categorías en Dr. Simi...[/green]")
        
        for category_name, base_category_url in self.categories_config.items():
            print(f"\n[bold blue]Procesando categoría:[/bold blue] {category_name}")
            
            page_num = 1
            has_more = True
            
            while has_more:
                # Construct URL with pagination
                separator = "&" if "?" in base_category_url else "?"
                url = f"{base_category_url}{separator}page={page_num}"
                
                print(f"  [cyan]Página {page_num}:[/cyan] {url}")
                
                try:
                    # Navigate with a decent timeout
                    page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    # Wait for products to load
                    try:
                        page.wait_for_selector(self.selectors['product_card'], timeout=10000)
                    except:
                        print(f"    [yellow]No se encontraron más productos o la página tardó demasiado en cargar.[/yellow]")
                        has_more = False
                        continue

                    # Get all product cards
                    cards = page.locator(self.selectors['product_card'])
                    count = cards.count()
                    
                    if count == 0:
                        has_more = False
                        continue
                        
                    print(f"    Encontrados {count} productos.")

                    for i in range(count):
                        card = cards.nth(i)
                        
                        # Extract basic info
                        name = "N/D"
                        if card.locator(self.selectors['product_name']).count() > 0:
                            name = card.locator(self.selectors['product_name']).first.inner_text().strip()

                        link = "N/D"
                        if card.locator(self.selectors['link']).count() > 0:
                            href = card.locator(self.selectors['link']).first.get_attribute("href")
                            if href:
                                link = self.base_url + href if href.startswith('/') else href

                        price = 0
                        if card.locator(self.selectors['price_container']).count() > 0:
                            price_text = card.locator(self.selectors['price_container']).first.inner_text()
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

                        image_url = ""
                        if card.locator(self.selectors['image']).count() > 0:
                            image_url = card.locator(self.selectors['image']).first.get_attribute("src")

                        # Brand Logic: 
                        # - If category is "Pronutrition", brand is "Pronutrition"
                        # - Otherwise, Dr Simi doesn't show brand in list. 
                        # - We could check the name for "Simi" or just "Dr Simi" as default or "N/D"
                        brand = "N/D"
                        if category_name == "Pronutrition":
                            brand = "Pronutrition"
                        elif "simi" in name.lower():
                            brand = "Dr Simi"
                        
                        current_date = datetime.now().strftime("%Y-%m-%d")

                        yield {
                            'date': current_date,
                            'site_name': self.site_name,
                            'category': category_name,
                            'subcategory': category_name,
                            'product_name': name,
                            'brand': brand,
                            'price': price,
                            'link': link,
                            'rating': "0",
                            'reviews': "0",
                            'active_discount': False, # Could be improved by checking for old price
                            'thumbnail_image_url': image_url,
                            'image_url': image_url,
                            'sku': "N/D", # VTEX SKU is usually inside scripts or detail page
                            'description': "N/D"
                        }

                    # Check if there is a next page/show more button to decide if we continue
                    # However, VTEX usually just returns fewer products or a different state if page exceeds limit.
                    # A more robust way is to check the 'Mostrar más' button or the length of products.
                    # If we found fewer products than a typical page size (usually 12, 16, 24, 48...), or 0, we stop.
                    # But VTEX pagination with ?page=n is quite reliable. 
                    # If we found products, we try the next page.
                    page_num += 1
                    
                    # Safety break to avoid infinite loops if something goes wrong
                    if page_num > 50:
                        has_more = False

                except Exception as e:
                    print(f"    [red]Error en página {page_num}: {e}[/red]")
                    has_more = False

if __name__ == "__main__":
    scraper = DrSimiScraper("https://www.drsimi.cl", headless=True)
    scraper.run()
