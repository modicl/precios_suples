# Scraper para la pagina web SupleStore.cl
from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class SupleStoreScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        category_urls = {
            "Proteinas": [
                "https://www.suplestore.cl/collection/whey-blend",
                "https://www.suplestore.cl/collection/isoladas-hidrolizadas",
                "https://www.suplestore.cl/collection/carne",
                "https://www.suplestore.cl/collection/veganas",
                "https://www.suplestore.cl/collection/sin-lactosa",
                "https://www.suplestore.cl/collection/sin-gluten"
            ],
            "Creatinas": [
                "https://www.suplestore.cl/collection/creatinas",
            ],
            "Vitaminas": [
                "https://www.suplestore.cl/collection/vitaminas-salud"
            ],
            "Pre Entrenos": [
                "https://www.suplestore.cl/collection/preentrenos"
            ],
            "Ganadores de Peso": [
                "https://www.suplestore.cl/collection/ganadores-de-masa"
            ],
            "Aminoacidos y BCAA": [
                "https://www.suplestore.cl/collection/aminos-bcaa-s"
            ],
            "Perdida de Grasa": [
                "https://www.suplestore.cl/collection/qm-y-l-carnitina", # Ajuste basado en estructura probable o general
                "https://www.suplestore.cl/collection/quemadores",
                "https://www.suplestore.cl/collection/cafeina"
            ],
            "Snacks y Comida": [
                "https://www.suplestore.cl/collection/barras-proteicas",
                "https://www.suplestore.cl/collection/energeticas-batidos-y-geles"
            ]
        }
        
        selectors = {
            "product_grid": ".row", # Contenedor general suele ser row en bootstrap
            'product_card': '.bs-product', 
            'product_name': '.bs-product-info h6',
            'brand': '.bs-product-info .badge-secondary', 
            'price_final': '.bs-product-final-price', 
            'price_old': '.bs-product-old-price',
            'link': '.bs-product-info a', 
            'next_button': 'a.navegation.next, .pagination .next a' 
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="SupleStore")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en SupleStore...[/green]")
        
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
                            # Esperar producto o mensaje de no productos
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
                            
                            # Obtenemos info básica
                            
                            # Link
                            link = "N/D"
                            # El link envuelve el titulo .bs-product-info a
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
                                
                            # Price
                            price = 0
                            # Intentar precio final primero
                            price_elem = producto.locator(self.selectors['price_final'])
                            if price_elem.count() > 0:
                                price_text = price_elem.first.inner_text() 
                                # Formato "Ahora $ 19.990"
                                clean_price = re.sub(r'[^\d]', '', price_text)
                                if clean_price:
                                    price = int(clean_price)
                            
                            # Active Discount
                            # Si existe active discount, usualmente hay un precio 'old'
                            active_discount = False
                            if producto.locator(self.selectors['price_old']).count() > 0:
                                active_discount = True
                                # Opcional: Podríamos extraer el precio normal si quisiéramos comparar

                            # Rating / Reviews - No parecen estar visibles en la tarjeta
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
                        # Buscar botón "Siguiente"
                        # Selector: a.navegation.next
                        next_btn = page.locator(self.selectors['next_button'])
                        
                        # Verificar si existe y es visible (y que no sea disabled si aplica)
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href_next = next_btn.first.get_attribute("href")
                            if href_next:
                                print(f"  > Avanzando a página {page_number + 1}...")
                                # Navegamos directamente al href para ser más robustos
                                page.goto(self.base_url + href_next if href_next.startswith('/') else href_next)
                                page_number += 1
                                # Espera pequeña
                                page.wait_for_timeout(2000)
                            else:
                                # Si no tiene href (raro en 'a'), click
                                print("  > Click en botón Siguiente...")
                                next_btn.first.click()
                                page.wait_for_timeout(3000)
                                page_number += 1
                        else:
                            print("  > No hay más páginas (o botón no encontrado).")
                            break
                            
                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")

if __name__ == "__main__":
    base_url = "https://www.suplestore.cl"
    # Testing mode
    scraper = SupleStoreScraper(base_url=base_url, headless=True)
    scraper.run()