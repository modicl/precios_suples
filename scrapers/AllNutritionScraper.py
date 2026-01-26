# Scapper para la pagina web AllNutrition.cl
from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import csv
import re

class AllNutritionScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        # Categorias y sus URLs
        category_urls = {
            "Proteinas": [
                "https://allnutrition.cl/collections/whey-protein",
                "https://allnutrition.cl/collections/proteinas-isoladas",
                "https://allnutrition.cl/collections/proteinas-de-carne",
                "https://allnutrition.cl/collections/proteinas-liquidas",
                "https://allnutrition.cl/collections/proteinas-veganas",
                "https://allnutrition.cl/collections/proteinas-vegetarianas",
                "https://allnutrition.cl/collections/barras-proteicas",
                "https://allnutrition.cl/collections/snack-proteico"
            ],
            "Creatinas": [
                "https://allnutrition.cl/collections/creatinas"
            ],
            "Vitaminas": [
                "https://allnutrition.cl/collections/multivitaminicos-y-energia",
                "https://allnutrition.cl/collections/b-complex",
                "https://allnutrition.cl/collections/vitamina-c",
                "https://allnutrition.cl/collections/vitamina-d",
                "https://allnutrition.cl/collections/vitamina-e",
                "https://allnutrition.cl/collections/magnesio",
                "https://allnutrition.cl/collections/acido-folico",
                "https://allnutrition.cl/collections/coenzima-q10",
                "https://allnutrition.cl/collections/omega-3",
                "https://allnutrition.cl/collections/super-alimentos",
                "https://allnutrition.cl/collections/arginina"
            ],
            "Pre Entrenos": [
                "https://allnutrition.cl/collections/pre-workout",
                "https://allnutrition.cl/collections/guarana-y-cafeina",
                "https://allnutrition.cl/collections/carbohidratos"
            ],
            "Ganadores de Peso": [
                "https://allnutrition.cl/collections/ganadores-de-peso"
            ],
            "Aminoacidos y BCAA": [
                "https://allnutrition.cl/collections/bcaa-1",
                "https://allnutrition.cl/collections/aminoacidos"
            ],
            "Glutamina": [
                "https://allnutrition.cl/collections/glutaminas"
            ],
            "Perdida de Grasa": [
                "https://allnutrition.cl/collections/quemadores-de-grasa"
            ],
            "Post Entreno": [
                "https://allnutrition.cl/collections/carbohidratos"
            ],
            "Snacks y Comida": [
                "https://allnutrition.cl/collections/barras-proteicas",
                "https://allnutrition.cl/collections/snack-proteico",
                "https://allnutrition.cl/collections/snacks-dulces",
                "https://allnutrition.cl/collections/snacks-salados"
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
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="AllNutrition")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en AllNutrition...[/green]")
        
        context = page.context
        
        for main_category, urls in self.category_urls.items():
            for url in urls:
                subcategory_name = url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                subcategory_name = self.clean_text(subcategory_name)
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {subcategory_name} ({url})")

                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
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
                            current_date = datetime.now().strftime("%Y-%m-%d")
                            
                            # --- GRID EXTRACTION ---
                            
                            # Title
                            title = "N/D"
                            if producto.locator(self.selectors['title']).count() > 0:
                                raw_title = producto.locator(self.selectors['title']).first.inner_text()
                                title = self.clean_text(raw_title)
                            elif producto.locator(self.selectors['title_secondary']).count() > 0:
                                raw_title = producto.locator(self.selectors['title_secondary']).first.inner_text()
                                title = self.clean_text(raw_title)
                            
                            # Brand
                            brand = "N/D"
                            if producto.locator(self.selectors['vendor']).count() > 0:
                                raw_brand = producto.locator(self.selectors['vendor']).first.inner_text()
                                brand = self.clean_text(raw_brand)

                            
                            # Link
                            link = "N/D"
                            if producto.locator(self.selectors['link']).count() > 0:
                                href = producto.locator(self.selectors['link']).first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href
                            
                            # Thumbnail
                            thumbnail_url = ""
                            if producto.locator(self.selectors['thumbnail']).count() > 0:
                                thumb_src = producto.locator(self.selectors['thumbnail']).first.get_attribute('src')
                                if thumb_src:
                                    thumbnail_url = "https:" + thumb_src if thumb_src.startswith('//') else thumb_src

                            # Price
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
                            
                            # Discount
                            active_discount = False
                            if producto.locator(self.selectors['active_discount']).count() > 0:
                                active_discount = True

                            # --- DETAIL EXTRACTION (NEW TAB) ---
                            image_url = ""
                            sku = ""
                            description = ""
                            
                            if link != "N/D":
                                try:
                                    # Open new tab logic
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                                    
                                    # 1. Image (HD)
                                    # Selector refined from inspection: .slide:not(.d-none) img OR .c-gallery-product__item:not(.d-none) img
                                    img_el = detail_page.locator('.slide:not(.d-none) img, .c-gallery-product__item:not(.d-none) img').first
                                    if img_el.count() > 0:
                                        src = img_el.get_attribute('src')
                                        if src:
                                            image_url = "https:" + src if src.startswith('//') else src
                                    
                                    # 2. SKU
                                    sku_el = detail_page.locator('.s-main-product__sku, .product-sku').first
                                    if sku_el.count() > 0:
                                        sku = sku_el.inner_text().strip()
                                    
                                    # 3. Description
                                    # Combine Benefits + Table if possible, or just benefits
                                    desc_el = detail_page.locator('.s-main-product__text-wrapper, .c-product-description').first
                                    if desc_el.count() > 0:
                                        description = desc_el.inner_text().strip()
                                        
                                    detail_page.close()
                                    
                                except Exception as e:
                                    print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                    try: detail_page.close() 
                                    except: pass

                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(main_category),
                                'subcategory': subcategory_name,

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
                            
                        # Paginación
                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href = next_btn.first.get_attribute("href")
                            if href:
                                page.goto(self.base_url + href if href.startswith('/') else href)
                                page_number += 1
                            else:
                                next_btn.first.click()
                                page_number += 1
                        else:
                            break
                
                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")

if __name__ == "__main__":
    base_url = "https://allnutrition.cl" 
    scraper = AllNutritionScraper(base_url=base_url, headless=True)
    scraper.run()
