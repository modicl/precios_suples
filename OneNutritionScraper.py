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
            "Vitaminas": [
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
            "product_grid": "#js-product-list", # Selector específico de la grilla principal
            'product_card': '#js-product-list .product-miniature', # Items dentro de #js-product-list
            'product_name': '.product-title a',
            'brand': '.product-title a', 
            'price_container': '.product-price-and-shipping', 
            'price_current': '.price', 
            'price_regular': '.regular-price', 
            'link': '.product-title a', 
            'next_button': '.pagination .next' 
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="OneNutrition")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en OneNutrition...[/green]")
        
        for main_category, urls in self.category_urls.items():
            for url in urls:
                subcategory_name = url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {subcategory_name} ({url})")
                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")
                        try:
                            # Esperar grilla de productos específica
                            page.wait_for_selector(self.selectors['product_grid'], timeout=10000)
                        except:
                            print(f"[red]No se encontraron productos en la grilla principal (#js-product-list) de {url}.[/red]")
                            # Opcional: Tomar screenshot para debug
                            # page.screenshot(path=f"debug_{subcategory_name}.png")
                            break

                        producto_cards = page.locator(self.selectors['product_card'])
                        count = producto_cards.count()
                        print(f"  > Encontrados {count} productos en esta página.")
                        
                        for i in range(count):
                            producto = producto_cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d")
                            
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
                                title = producto.locator(self.selectors['product_name']).first.inner_text().strip()
                            
                            # Brand
                            brand = title.split(' ')[0] if title != "N/D" else "N/D"
                                
                            # Price Logic - Refined
                            price = 0
                            active_discount = False
                            
                            price_container = producto.locator(self.selectors['price_container'])
                            if price_container.count() > 0:
                                # Chequear si existe precio regular (tachado) -> Oferta
                                regular = price_container.locator(self.selectors['price_regular'])
                                if regular.count() > 0:
                                    active_discount = True
                                    # El precio final está en .price
                                    current = price_container.locator(self.selectors['price_current'])
                                    price_text = current.first.inner_text() if current.count() > 0 else "0"
                                else:
                                    # No hay oferta, precio normal en .price
                                    current = price_container.locator(self.selectors['price_current'])
                                    price_text = current.first.inner_text() if current.count() > 0 else "0"
                            else:
                                price_text = "0"
                                
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

                            # Rating / Reviews
                            rating = "0"
                            reviews = "0"
                            
                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': main_category,
                                'subcategory': subcategory_name,
                                'product_name': title,
                                'brand': brand,
                                'price': price,
                                'link': link,
                                'rating': rating,
                                'reviews': reviews,
                                'active_discount': active_discount
                            }
                        
                        # Paginación
                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href_next = next_btn.first.get_attribute("href")
                            if href_next:
                                print(f"  > Avanzando a página {page_number + 1}...")
                                page.goto(href_next)
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