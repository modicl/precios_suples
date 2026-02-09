# Scraper para la pagina web OneNutrition.cl
from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class OneNutritionScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        category_urls = {
            "Proteinas": [
                "https://onenutrition.cl/tienda/whey-protein",
                "https://onenutrition.cl/tienda/isolate-aislada",
                "https://onenutrition.cl/tienda/sin-sabor-cocinar",
                "https://onenutrition.cl/tienda/vegana",
                "https://onenutrition.cl/tienda/sin-lactosa",
                "https://onenutrition.cl/tienda/carne",
                "https://onenutrition.cl/tienda/caseina-",
                "https://onenutrition.cl/tienda/matriz-de-proteina-",
                "https://onenutrition.cl/tienda/soya",
                "https://onenutrition.cl/tienda/liquidas"
            ],
            "Creatinas": [
                "https://onenutrition.cl/tienda/sello-creapure",
                "https://onenutrition.cl/tienda/creatinas-"
            ],
            "Vitaminas y Minerales": [
                "https://onenutrition.cl/tienda/vitaminas-a-z",
                "https://onenutrition.cl/tienda/omega-fish-oil",
                "https://onenutrition.cl/tienda/ashwagandha",
                "https://onenutrition.cl/tienda/dhea",
                "https://onenutrition.cl/tienda/buen-dormir",
                "https://onenutrition.cl/tienda/magnesio",
                "https://onenutrition.cl/tienda/enzimas-digestivas",
                "https://onenutrition.cl/tienda/oregano-oil",
                "https://onenutrition.cl/tienda/zma-zmb",
                "https://onenutrition.cl/tienda/salud-articular",
                "https://onenutrition.cl/tienda/resveratrol-nadh",
                "https://onenutrition.cl/tienda/pro-hormonales",
                "https://onenutrition.cl/tienda/probiotico-prebiotico",
                "https://onenutrition.cl/tienda/gaba-5-htp",
                "https://onenutrition.cl/tienda/maca-tribulus",
                "https://onenutrition.cl/tienda/melena-de-leon",
                "https://onenutrition.cl/tienda/acido-alfa-lipoico",
                "https://onenutrition.cl/tienda/nac",
                "https://onenutrition.cl/tienda/mct-oil",
                "https://onenutrition.cl/tienda/hongos-funcionales",
                "https://onenutrition.cl/tienda/colageno-biotina",
                "https://onenutrition.cl/tienda/lisina",
                "https://onenutrition.cl/tienda/gummies",
                "https://onenutrition.cl/tienda/calcio-magnesio-zinc",
                "https://onenutrition.cl/tienda/vinagre-de-manzana",
                "https://onenutrition.cl/tienda/potasio-",
                "https://onenutrition.cl/tienda/astaxantina",
                "https://onenutrition.cl/tienda/hierro",
                "https://onenutrition.cl/tienda/prostata",
                "https://onenutrition.cl/tienda/inositol",
                "https://onenutrition.cl/tienda/acido-folico",
                "https://onenutrition.cl/tienda/boron",
                "https://onenutrition.cl/tienda/silimarina",
                "https://onenutrition.cl/tienda/selenio",
                "https://onenutrition.cl/tienda/zinc",
                "https://onenutrition.cl/tienda/krill-oil"
            ],
            "Pre Entrenos": [
                "https://onenutrition.cl/tienda/pre-entrenamientos",
                "https://onenutrition.cl/tienda/cafeinas",
                "https://onenutrition.cl/tienda/bebidas-energeticas"
            ],
            "Ganadores de Peso": [
                "https://onenutrition.cl/tienda/ganadores-de-masa-"
            ],
            "Aminoacidos y BCAA": [
                "https://onenutrition.cl/tienda/citrulina",
                "https://onenutrition.cl/tienda/aminoacidos",
                "https://onenutrition.cl/tienda/eaa",
                "https://onenutrition.cl/tienda/arginina",
                "https://onenutrition.cl/tienda/bcaa",
                "https://onenutrition.cl/tienda/glutaminas",
                "https://onenutrition.cl/tienda/daa",
                "https://onenutrition.cl/tienda/hmb",
                "https://onenutrition.cl/tienda/taurina"
            ],
            "Perdida de Grasa": [
                "https://onenutrition.cl/tienda/dieta-quemadores"
            ],
            "Snacks y Comida": [
                "https://onenutrition.cl/tienda/barras-snack"
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

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en OneNutrition...[/green]")
        
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
                                    desc_el = detail_page.locator('#description, .product-description').first
                                    if desc_el.count() > 0:
                                        description = desc_el.inner_text().strip()
                                        
                                    detail_page.close()
                                    
                                except Exception as e:
                                    print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass

                            # New Categorization Logic
                            final_subcategory = subcategory_name
                            cat_info = self.categorizer.classify_product(title, subcategory_name)
                            if cat_info:
                                final_subcategory = cat_info['nombre_subcategoria']

                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(main_category),
                                'subcategory': final_subcategory,
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