# Scraper para la pagina web OneNutrition.cl
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from rich import print
from datetime import datetime
import re

class OneNutritionScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        category_urls = {
            "Proteinas": [
                {"url": "https://onenutrition.cl/tienda/proteinas", "subcategory": "Proteína de Whey"}
            ],
            "Creatinas": [
                {"url": "https://onenutrition.cl/tienda/creatinas", "subcategory": "Creatina Monohidrato"}
            ],
            "Vitaminas y Minerales": [
                {"url": "https://onenutrition.cl/tienda/vitaminas-salud", "subcategory": "Vitaminas y Minerales"}
            ],
            "Pre Entrenos": [
                {"url": "https://onenutrition.cl/tienda/energia-resistencia", "subcategory": "Pre Entreno"}
            ],
            "Ganadores de Peso": [
                {"url": "https://onenutrition.cl/tienda/ganadores-de-masa-", "subcategory": "Ganadores De Peso"}
            ],
            "Aminoacidos y BCAA": [
                {"url": "https://onenutrition.cl/tienda/aminoacidos", "subcategory": "Otros Aminoacidos y BCAA"},
            ],
            "Perdida de Grasa": [
                {"url": "https://onenutrition.cl/tienda/dieta-quemadores", "subcategory": "Dieta Quemadores"}
            ],
            "Snacks y Comida": [
                {"url": "https://onenutrition.cl/tienda/barras-snack", "subcategory": "Barras Snack"}
            ]
        }
        
        selectors = {
            "product_grid": "#js-product-list", 
            'product_card': '#js-product-list .product-miniature', 
            'product_name': '.product-title a',
            'brand': '.product-title a', 
            'price_container': '.product-price-and-shipping', 
            'price_current': '.price', 
            'price_regular': '.regular-price', 
            'link': '.product-title a', 
            'next_button': '.pagination .next',
            'thumbnail': 'a.thumbnail.product-thumbnail img'
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="OneNutrition")
        self.classifier = CategoryClassifier()

    def _classify_product(self, title, description, main_category, deterministic_subcategory, brand):
        """
        Clasificación de productos para OneNutrition.
        Delega completamente en CategoryClassifier (sin lógica extra específica).
        """
        return self.classifier.classify(title, description, main_category, deterministic_subcategory, brand)

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en OneNutrition...[/green]")
        
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
                            page.wait_for_selector(self.selectors['product_grid'], timeout=10000)
                        except:
                            print(f"[red]No se encontraron productos en la grilla principal (#js-product-list) de {url}.[/red]")
                            break

                        producto_cards = page.locator(self.selectors['product_card'])
                        count = producto_cards.count()
                        print(f"  > Encontrados {count} productos en esta página.")
                        
                        for i in range(count):
                            producto = producto_cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # --- Grid Extraction ---
                            
                            # Link
                            link = "N/D"
                            if producto.locator(self.selectors['link']).count() > 0:
                                href = producto.locator(self.selectors['link']).first.get_attribute("href")
                                if href:
                                    link = href 
                                    if not link.startswith('http'):
                                         link = self.base_url + link

                            # Deduplication Check
                            if link != "N/D" and link in self.seen_urls:
                                print(f"[yellow]  >> Producto duplicado omitido: {title}[/yellow]")
                                continue
                            if link != "N/D":
                                self.seen_urls.add(link)

                            # Title
                            title = "N/D"
                            if producto.locator(self.selectors['product_name']).count() > 0:
                                raw_title = producto.locator(self.selectors['product_name']).first.inner_text()
                                title = self.clean_text(raw_title)
                            
                            # Brand (inferred)
                            brand = "N/D"

                            
                            # Thumbnail
                            thumbnail_url = ""
                            if producto.locator(self.selectors['thumbnail']).count() > 0:
                                t_src = producto.locator(self.selectors['thumbnail']).first.get_attribute("src")
                                if t_src:
                                    thumbnail_url = t_src
                                    
                            # Price
                            price = 0
                            active_discount = False
                            
                            price_container = producto.locator(self.selectors['price_container'])
                            if price_container.count() > 0:
                                regular = price_container.locator(self.selectors['price_regular'])
                                if regular.count() > 0:
                                    active_discount = True
                                    current = price_container.locator(self.selectors['price_current'])
                                    price_text = current.first.inner_text() if current.count() > 0 else "0"
                                else:
                                    current = price_container.locator(self.selectors['price_current'])
                                    price_text = current.first.inner_text() if current.count() > 0 else "0"
                            else:
                                price_text = "0"
                                
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

                            # --- Detail Extraction (Multi-tab) ---
                            image_url = ""
                            sku = ""
                            description = ""
                            
                            if link != "N/D":
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)
                                    
                                    # 1. Main Image (HD)
                                    img_el = detail_page.locator('.product-cover img').first
                                    if img_el.count() > 0:
                                        src = img_el.get_attribute("src")
                                        if src:
                                            image_url = src
                                            
                                    # 2. SKU
                                    sku_el = detail_page.locator("span[itemprop='sku'], meta[itemprop='sku']").first
                                    if sku_el.count() > 0:
                                        if sku_el.get_attribute("content"):
                                             sku = sku_el.get_attribute("content")
                                        else:
                                             sku = sku_el.inner_text().strip()
                                    
                                    # 3. Description
                                    try:
                                        detail_page.wait_for_selector('#description', timeout=5000)
                                    except:
                                        pass 

                                    # Prioritize specific class strictly inside #description to avoid carousels/reviews
                                    desc_el = detail_page.locator('#description .product-description').first
                                    if desc_el.count() == 0:
                                         desc_el = detail_page.locator('#description').first
                                         
                                    if desc_el.count() > 0:
                                        description = desc_el.inner_text().strip()
                                        
                                    detail_page.close()
                                    
                                except Exception as e:
                                    print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass

                            # New Categorization Logic
                            # Use Heuristic specifically for "CATEGORIZAR_PROTEINA" as requested
                            final_category, final_subcategory = self._classify_product(title, description, main_category, deterministic_subcategory, brand)

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
                                'brand': self.enrich_brand(self.clean_text(brand), title, scan_title=True),
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
                                print(f"  > Avanzando a página {page_number + 1}...")
                                page.goto(href)
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
    base_url = "https://onenutrition.cl"
    scraper = OneNutritionScraper(base_url=base_url, headless=True)
    scraper.run()