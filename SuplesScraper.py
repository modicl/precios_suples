# Scraper para la pagina web Suples.cl
from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class SuplesScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        category_urls = {
            "Proteinas": [
                "https://www2.suples.cl/collections/proteina-whey",
                "https://www2.suples.cl/collections/proteina-isolate",
                "https://www2.suples.cl/collections/proteinas-hidrolizadas",
                "https://www2.suples.cl/collections/proteinas-caseinas",
                "https://www2.suples.cl/collections/proteinas-de-carne",
                "https://www2.suples.cl/collections/proteinas-veganas",
                "https://www2.suples.cl/collections/proteinas-liquidas"
            ],
            "Creatinas": [
                "https://www2.suples.cl/collections/creatinas",
            ],
            "Vitaminas": [
                "https://www2.suples.cl/collections/multivitaminicos",
                "https://www2.suples.cl/collections/vitamina-b",
                "https://www2.suples.cl/collections/vitamina-c",
                "https://www2.suples.cl/collections/vitamina-d",
                "https://www2.suples.cl/collections/vitamina-e",
                "https://www2.suples.cl/collections/magnesio",
                "https://www2.suples.cl/collections/calcio",
                "https://www2.suples.cl/collections/omega-y-acidos-grasos-1", 
                "https://www2.suples.cl/collections/magnesio-y-minerales-1",
                "https://www2.suples.cl/collections/sistema-digestivo-y-probioticos",
                "https://www2.suples.cl/collections/colageno-y-articulaciones",
                "https://www2.suples.cl/collections/antimicrobianos-naturales-y-acido-caprilico",
                "https://www2.suples.cl/collections/equilibrante-natural-adaptogenos-y-bienestar-general",
                "https://www2.suples.cl/collections/aminoacidos-y-nutrientes-esenciales",
                "https://www2.suples.cl/collections/sistemas-nervioso-y-cognitivo",
                "https://www2.suples.cl/collections/bienestar-natural-y-salud-integral",
                "https://www2.suples.cl/collections/arginina",
                "https://www2.suples.cl/collections/antioxidantes",
                "https://www2.suples.cl/collections/colagenos-1",
                "https://www2.suples.cl/collections/hmb",
                "https://www2.suples.cl/collections/omega-3",
                "https://www2.suples.cl/collections/probioticos",
                "https://www2.suples.cl/collections/zma"
            ],
            "Pre Entrenos": [
                "https://www2.suples.cl/collections/pre-workout"
            ],
            "Ganadores de Peso": [
                "https://www2.suples.cl/collections/ganadores-de-masa"
            ],
            "Aminoacidos y BCAA": [
                "https://www2.suples.cl/collections/aminoacidos"
            ],
            "Perdida de Grasa": [
                "https://www2.suples.cl/collections/cafeina",
                "https://www2.suples.cl/collections/quemadores-termogenicos",
                "https://www2.suples.cl/collections/quemadores-liquidos",
                "https://www2.suples.cl/collections/quemadores-naturales",
                "https://www2.suples.cl/collections/eliminadores-de-retencion",
                "https://www2.suples.cl/collections/quemadores-localizados",
                "https://www2.suples.cl/collections/cremas-reductoras"
            ],
            "Snacks y Comida": [
                "https://www2.suples.cl/collections/barritas-y-snacks-proteicas",
                "https://www2.suples.cl/collections/alimentos-outdoor"
            ]
        }
        
        selectors = {
            "product_grid": ".collection-list", 
            'product_card': '.product-item', 
            'product_name': '.product-item__title',
            'brand': '.product-item__vendor', 
            'price_container': '.product-item__price-list', 
            'price_highlight': '.price--highlight', 
            'price_compare': '.price--compare', 
            'price_default': '.price', 
            'link': 'a.product-item__title', 
            'next_button': '.pagination__next'
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="Suples.cl")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en Suples.cl...[/green]")
        
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
                            page.wait_for_selector(self.selectors['product_card'], timeout=5000)
                        except:
                            print(f"[red]No se encontraron productos en {url} o tardó demasiado.[/red]")
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
                                    link = self.base_url + href if href.startswith('/') else href

                            # Title
                            title = "N/D"
                            if producto.locator(self.selectors['product_name']).count() > 0:
                                title = producto.locator(self.selectors['product_name']).first.inner_text().strip()
                            
                            # Brand
                            brand = "N/D"
                            if producto.locator(self.selectors['brand']).count() > 0:
                                brand = producto.locator(self.selectors['brand']).first.inner_text().strip()
                                
                            # Price Logic - Updated
                            price = 0
                            active_discount = False
                            
                            price_container = producto.locator(self.selectors['price_container'])
                            if price_container.count() > 0:
                                highlight = price_container.locator(self.selectors['price_highlight'])
                                if highlight.count() > 0:
                                    # Hay precio oferta
                                    price_text = highlight.first.inner_text()
                                    active_discount = True
                                else:
                                    # Precio normal sin oferta
                                    normal = price_container.locator(self.selectors['price_default'])
                                    if normal.count() > 0:
                                        price_text = normal.first.inner_text()
                                    else:
                                        price_text = "0"
                            else:
                                # Fallback
                                general_price = producto.locator(self.selectors['price_default'])
                                if general_price.count() > 0:
                                    price_text = general_price.first.inner_text()
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
                                page.goto(self.base_url + href_next if href_next.startswith('/') else href_next)
                                page_number += 1
                                page.wait_for_timeout(2000)
                            else:
                                print("  > Click en botón Siguiente (sin href)...")
                                next_btn.first.click()
                                page.wait_for_timeout(3000)
                                page_number += 1
                        else:
                            print("  > No hay más páginas.")
                            break
                            
                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")

if __name__ == "__main__":
    base_url = "https://www2.suples.cl"
    scraper = SuplesScraper(base_url=base_url, headless=True)
    scraper.run()