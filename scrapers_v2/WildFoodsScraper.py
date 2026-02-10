from BaseScraper import BaseScraper
from playwright.sync_api import Page
import time
from datetime import datetime
import re

class WildFoodsScraper(BaseScraper):
    def __init__(self, headless=False):
        
        # 1. Deterministic Taxonomy Mapping
        # Structure: "Category Name": [ { "url": "...", "subcategory": "Exact DB Subcategory" } ]
        category_urls = {
            "Snacks y Comida": [
                {
                    "url": "https://thewildfoods.com/collections/barritas-wild-protein",
                    "subcategory": "Barritas Y Snacks Proteicas"
                },
                {
                    "url": "https://thewildfoods.com/collections/barritas-veganas",
                    "subcategory": "Barritas Y Snacks Proteicas"
                },
                {
                    "url": "https://thewildfoods.com/collections/wild-fit",
                    "subcategory": "Snacks Dulces" 
                },
                {
                    "url": "https://thewildfoods.com/collections/galletas",
                    "subcategory": "Snacks Dulces"
                },
                 {
                    "url": "https://thewildfoods.com/collections/granolas",
                    "subcategory": "Cereales"
                },
                {
                    "url": "https://thewildfoods.com/collections/the-cookie",
                    "subcategory": "Snacks Dulces"
                }
            ],
            "Creatinas": [
                {
                    "url": "https://thewildfoods.com/collections/creatina",
                    "subcategory": "Creatina Monohidrato"
                }
            ],
            "Bebidas Nutricionales": [
                {
                    "url": "https://thewildfoods.com/collections/batidos-de-proteina",
                    "subcategory": "Batidos de proteína"
                }
            ],
            "Proteinas": [
                {
                    "url": "https://thewildfoods.com/collections/whey-protein",
                    "subcategory": "Proteína de Whey"
                },
                 {
                    "url": "https://thewildfoods.com/collections/proteina-vegana",
                    "subcategory": "Proteína Vegana"
                }
            ],
            "Pre Entrenos": [
                 {
                    "url": "https://thewildfoods.com/collections/pre-entreno",
                    "subcategory": "Otros Pre Entrenos"
                }
            ]
        }
        
        selectors = {
            'product_card': '.product-item, .product-card', 
            'product_name': '.product-item__title, .product-card__title',
            'price': '.product-item__price, .price-item--sale, .price-item--regular',
            'link': 'a.product-item__image-wrapper, a.product-card__image-wrapper, .product-item__title a',
            'image': '.product-item__image img, .product-card__image img',
            'next_button': '.pagination__next'
        }
        
        super().__init__("https://thewildfoods.com", headless, category_urls, selectors, site_name="Wild Foods")

    def extract_process(self, page: Page):
        print(f"[green]Iniciando scraping Determinista (V2) de Wild Foods...[/green]")
        
        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                # The Golden Key: Explicit Subcategory from Config
                deterministic_sub = item['subcategory'] 
                
                print(f"\n[bold blue]Procesando:[/bold blue] {main_category} -> {deterministic_sub} ({url})")
                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    # Handle "Show More" or Pagination
                    while True:
                        # Auto-scroll
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        # Smart wait
                        try:
                            page.wait_for_selector(self.selectors['product_card'], timeout=5000)
                        except:
                            print(f"[yellow]No productos visibles en {url}[/yellow]")
                            break

                        products = page.locator(self.selectors['product_card'])
                        count = products.count()
                        print(f"  > Encontrados {count} productos...")
                        
                        for i in range(count):
                            p = products.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            try:
                                # Name
                                name_el = p.locator(self.selectors['product_name']).first
                                name = self.clean_text(name_el.inner_text()) if name_el.count() > 0 else "N/D"
                                
                                # Price
                                price = 0
                                price_el = p.locator(self.selectors['price'])
                                # Prioritize sale price
                                if price_el.count() > 0:
                                    # Get text, strip 'Desde', '$', '.'
                                    p_text = price_el.first.inner_text()
                                    clean_p = re.sub(r'[^\d]', '', p_text)
                                    if clean_p:
                                        price = int(clean_p)

                                # Link
                                link = "N/D"
                                link_el = p.locator(self.selectors['link']).first
                                if link_el.count() > 0:
                                    href = link_el.get_attribute('href')
                                    if href:
                                        link = self.base_url + href if href.startswith('/') else href

                                # Image
                                image_url = ""
                                img_el = p.locator(self.selectors['image']).first
                                if img_el.count() > 0:
                                    src = img_el.get_attribute('src') or img_el.get_attribute('data-src') or img_el.get_attribute('srcset')
                                    if src:
                                        if src.startswith('//'):
                                            src = "https:" + src
                                        # Clean query params for cleaner URL
                                        image_url = src.split('?')[0]

                                # Brand Strategy: Wild Foods is the brand
                                brand = "Wild Foods" 
                                if "way bar" in name.lower():
                                    brand = "Way Bar"
                                
                                yield {
                                    'date': current_date,
                                    'site_name': self.site_name,
                                    'category': main_category,         # Defined in config
                                    'subcategory': deterministic_sub,  # EXPLICIT from config
                                    'product_name': name,
                                    'brand': brand,
                                    'price': price,
                                    'link': link,
                                    'rating': "0",
                                    'reviews': "0",
                                    'active_discount': False,
                                    'thumbnail_image_url': image_url,
                                    'image_url': image_url,
                                    'sku': "",
                                    'description': f"{main_category} - {deterministic_sub}" # Placeholder desc
                                }

                            except Exception as e:
                                print(f"[red]Error parsing product: {e}[/red]")
                                continue
                        
                        # Pagination Logic
                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.is_visible():
                            print("  > Siguiente página...")
                            # Check if it's a link or button
                            href = next_btn.get_attribute('href')
                            if href:
                                page.goto(self.base_url + href if href.startswith('/') else href)
                            else:
                                next_btn.click()
                        else:
                            break
                            
                except Exception as e:
                    print(f"[red]Error critical en {url}: {e}[/red]")

if __name__ == "__main__":
    scraper = WildFoodsScraper(headless=True)
    scraper.run()
