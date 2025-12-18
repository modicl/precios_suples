from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class ChileSuplementosScraper(BaseScraper):
    def __init__(self, base_url="https://www.chilesuplementos.cl", headless=False):
        category_urls = [
            "https://www.chilesuplementos.cl/categoria/productos/proteinas/",
            "https://www.chilesuplementos.cl/categoria/productos/creatinas/",
            "https://www.chilesuplementos.cl/categoria/productos/vitaminas/",
            "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/",
            "https://www.chilesuplementos.cl/categoria/productos/ganadores-de-peso/",
            "https://www.chilesuplementos.cl/categoria/productos/aminoacidos-y-bcaa/",
            "https://www.chilesuplementos.cl/categoria/productos/glutamina/",
            "https://www.chilesuplementos.cl/categoria/perdida-de-grasa/",
            "https://www.chilesuplementos.cl/categoria/productos/post-entreno/",
            "https://www.chilesuplementos.cl/categoria/productos/snacks-y-comida/"
        ]
        
        selectors = {
            'product_card': '.porto-tb-item',
            'product_name': '.post-title',
            'brand': '.tb-meta-pwb-brand',
            'price': '.price',
            'link': '.post-title a',
            'rating': '.star-rating',
            'active_discount': '.onsale',
            'next_button': '.next.page-numbers'
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="ChileSuplementos")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en ChileSuplementos...[/green]")
        
        for url in self.category_urls:
            category_name = url.rstrip('/').split('/')[-1].replace('-', ' ')
            print(f"\n[bold blue]Procesando categoría:[/bold blue] {category_name} ({url})")
            
            try:
                page.goto(url, wait_until="load", timeout=60000)
                print(f"  > Título de la página: {page.title()}")
                
                last_product_count = 0
                no_change_counter = 0
                
                while True:
                    # Esperar a que carguen los productos (o que haya al menos 1 si es el inicio)
                    try:
                        if last_product_count == 0:
                            print("  > Esperando selector de productos (.porto-tb-item)...")
                            page.wait_for_selector(self.selectors['product_card'], state="attached", timeout=30000)
                    except:
                        print(f"[red]No se encontraron productos en {url} o tardó demasiado (Timeout 30s).[/red]")
                        break

                    producto_cards = page.locator(self.selectors['product_card'])
                    current_product_count = producto_cards.count()
                    
                    # Yield nuevos productos encontrados desde el último conteo
                    if current_product_count > last_product_count:
                        print(f"Indexando productos del {last_product_count + 1} al {current_product_count}...")
                        
                        for i in range(last_product_count, current_product_count):
                            producto = producto_cards.nth(i)
                            
                            # Fecha
                            current_date = datetime.now().strftime("%Y-%m-%d")
                            
                            # Titulo
                            title = "N/D"
                            title_elem = producto.locator(self.selectors['product_name']) 
                            if title_elem.count() > 0:
                                title = title_elem.first.inner_text().strip()
                            
                            # Brand
                            brand = "N/D"
                            brand_elem = producto.locator(self.selectors['brand'])
                            if brand_elem.count() > 0:
                                brand = brand_elem.first.inner_text().strip()
                                
                            # Link
                            link = "N/D"
                            link_elem = producto.locator(self.selectors['link']).first
                            if link_elem.count() > 0:
                                href = link_elem.get_attribute("href")
                                if href:
                                    link = href
                                    
                            # Price
                            price = 0
                            price_elem = producto.locator(self.selectors['price'])
                            if price_elem.count() > 0:
                                # Estrategia 1: Buscar elementos específicos de monto (.woocommerce-Price-amount)
                                # Si hay oferta, suelen haber dos (antiguo y nuevo), el último es el precio final.
                                amounts = price_elem.locator(".woocommerce-Price-amount")
                                if amounts.count() > 0:
                                    price_text = amounts.last.inner_text()
                                else:
                                    # Fallback: Texto directo del contenedor
                                    price_text = price_elem.first.inner_text()
                                
                                # Manejo de rangos ($18.000 - $19.990) si aún persiste en el texto extraído
                                if "-" in price_text:
                                    price_text = price_text.split("-")[0]
                                    
                                try:
                                    clean_price = re.sub(r'[^\d]', '', price_text)
                                    if clean_price:
                                        price = int(clean_price)
                                except:
                                    pass
                                    
                            # Rating
                            rating = "N/D"
                            rating_elem = producto.locator(self.selectors['rating'])
                            if rating_elem.count() > 0:
                                rating_val = rating_elem.first.get_attribute("data-bs-original-title")
                                if rating_val:
                                    rating = rating_val
                                else:
                                    strong_rating = rating_elem.locator("strong.rating")
                                    if strong_rating.count() > 0:
                                        rating = strong_rating.first.inner_text().strip()

                            # Reviews
                            reviews = "0"
                            
                            # Active Discount
                            active_discount = False
                            discount_elem = producto.locator(self.selectors['active_discount'])
                            if discount_elem.count() > 0:
                                active_discount = True
                            
                            # print(f"  - {title} | {brand} | {price} | Discount: {active_discount}")
                            
                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': category_name,
                                'product_name': title,
                                'brand': brand,
                                'price': price,
                                'link': link,
                                'rating': rating,
                                'reviews': reviews,
                                'active_discount': active_discount
                            }
                        
                        last_product_count = current_product_count
                        no_change_counter = 0 # Reset counter found new items
                    
                    else:
                        no_change_counter += 1
                        
                    # Lógica de Infinite Scroll
                    print(f"Scroll hacia abajo... (Productos encontrados: {current_product_count})")
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    
                    # Espera dinámica: Si llevamos varios intentos sin cambios, esperamos menos para salir rápido
                    # Si acabamos de encontrar cosas, esperamos mas para que cargue lo siguiente
                    wait_time = 2000 if no_change_counter == 0 else 1000
                    page.wait_for_timeout(wait_time)
                    
                    # Verificar si llegaron nuevos productos
                    new_count = page.locator(self.selectors['product_card']).count()
                    
                    if new_count == last_product_count:
                        # Si tras el scroll y la espera no hay cambios...
                        if no_change_counter >= 3:
                            print("Se ha llegado al final del scroll (sin nuevos productos tras 3 intentos).")
                            break
                    
            except Exception as e:
                print(f"[red]Error categoría {url}: {e}[/red]")

if __name__ == "__main__":
    scraper = ChileSuplementosScraper(headless=False)
    scraper.run()