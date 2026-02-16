# Scapper para la pagina web AllNutrition.cl
from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import csv
import re

class AllNutritionScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        # Categorias y sus URLs
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
            "Vitaminas y Minerales": [
                {"url": "https://allnutrition.cl/collections/multivitaminicos-y-energia", "subcategory": "Multivitaminicos Y Energia"},
                {"url": "https://allnutrition.cl/collections/b-complex", "subcategory": "B Complex"},
                {"url": "https://allnutrition.cl/collections/vitamina-c", "subcategory": "Vitamina C"},
                {"url": "https://allnutrition.cl/collections/vitamina-d", "subcategory": "Vitamina D"},
                {"url": "https://allnutrition.cl/collections/vitamina-e", "subcategory": "Vitamina E"},
                {"url": "https://allnutrition.cl/collections/magnesio", "subcategory": "Magnesio"},
                {"url": "https://allnutrition.cl/collections/acido-folico", "subcategory": "Acido Folico"},
                {"url": "https://allnutrition.cl/collections/coenzima-q10", "subcategory": "Coenzima Q10"},
                {"url": "https://allnutrition.cl/collections/omega-3", "subcategory": "Omega 3"},
                {"url": "https://allnutrition.cl/collections/super-alimentos", "subcategory": "Super Alimentos"},
                {"url": "https://allnutrition.cl/collections/arginina", "subcategory": "Arginina"}
            ],
            "Pre Entrenos": [
                {"url": "https://allnutrition.cl/collections/pre-workout", "subcategory": "Pre Workout"},
                {"url": "https://allnutrition.cl/collections/guarana-y-cafeina", "subcategory": "Guarana Y Cafeina"},
                {"url": "https://allnutrition.cl/collections/carbohidratos", "subcategory": "Carbohidratos"}
            ],
            "Ganadores de Peso": [
                {"url": "https://allnutrition.cl/collections/ganadores-de-peso", "subcategory": "Ganadores De Peso"}
            ],
            "Aminoacidos y BCAA": [
                {"url": "https://allnutrition.cl/collections/bcaa-1", "subcategory": "Bcaa 1"},
                {"url": "https://allnutrition.cl/collections/aminoacidos", "subcategory": "Aminoacidos"}
            ],
            "Glutamina": [
                {"url": "https://allnutrition.cl/collections/glutaminas", "subcategory": "Glutaminas"}
            ],
            "Perdida de Grasa": [
                {"url": "https://allnutrition.cl/collections/quemadores-de-grasa", "subcategory": "Quemadores De Grasa"}
            ],
            "Post Entreno": [
                {"url": "https://allnutrition.cl/collections/carbohidratos", "subcategory": "Carbohidratos"}
            ],
            "Snacks y Comida": [
                {"url": "https://allnutrition.cl/collections/barras-proteicas", "subcategory": "Barras Proteicas"},
                {"url": "https://allnutrition.cl/collections/snack-proteico", "subcategory": "Snack Proteico"},
                {"url": "https://allnutrition.cl/collections/snacks-dulces", "subcategory": "Snacks Dulces"},
                {"url": "https://allnutrition.cl/collections/snacks-salados", "subcategory": "Snacks Salados"}
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
        
        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                deterministic_subcategory = item['subcategory']
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {deterministic_subcategory} ({url})")

                
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
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
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
                            
                            # New Categorization Logic
                            final_subcategory = deterministic_subcategory
                            # cat_info = self.categorizer.classify_product(title, deterministic_subcategory)
                            # if cat_info:
                            #    final_subcategory = cat_info['nombre_subcategoria']

                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(main_category),
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
