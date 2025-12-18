# Scapper para la pagina web ChileSuplementos
# Contiene Infinite Scroll y aveces un boton "Cargar más" , por lo que se debe usar attached, ya que los productos se agregan dinamicamente
# Hay un momento en que se agregan los productos y no se ven visualmente cada vez que hacemos un scroll

from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class ChileSuplementosScraper(BaseScraper):
    def __init__(self, base_url="https://www.chilesuplementos.cl", headless=False):

        # Categorias y sus URLs (Estructura Diccionario: Categoria -> [URLs])
        category_urls = {
            "Proteinas": [
                "https://www.chilesuplementos.cl/categoria/productos/tipo-de-proteina/whey-isolate/",
                "https://www.chilesuplementos.cl/categoria/productos/tipo-de-proteina/whey-protein/",
                "https://www.chilesuplementos.cl/categoria/productos/tipo-de-proteina/hidrolizada/",
                "https://www.chilesuplementos.cl/categoria/productos/tipo-de-proteina/caseina/",
                "https://www.chilesuplementos.cl/categoria/productos/tipo-de-proteina/clear-protein/",
                "https://www.chilesuplementos.cl/categoria/productos/tipo-de-proteina/proteina-de-carne/",
                "https://www.chilesuplementos.cl/categoria/productos/tipo-de-proteina/proteina-vegana/",
                "https://www.chilesuplementos.cl/categoria/productos/tipo-de-proteina/reemplazante-de-comidas/"
            ],
            "Creatinas": [
                "https://www.chilesuplementos.cl/categoria/productos/creatinas/tipo-de-creatina/monohidratada/",
                "https://www.chilesuplementos.cl/categoria/productos/creatinas/tipo-de-creatina/micronizada/",
                "https://www.chilesuplementos.cl/categoria/productos/creatinas/tipo-de-creatina/con-sello-creapure/",
                "https://www.chilesuplementos.cl/categoria/productos/creatinas/tipo-de-creatina/malato/",
                "https://www.chilesuplementos.cl/categoria/productos/creatinas/tipo-de-creatina/creatina-hcl/"
            ],
            "Vitaminas": [
                "https://www.chilesuplementos.cl/categoria/productos/vitaminas-y-wellness/vitaminas/vitamina-b/",
                "https://www.chilesuplementos.cl/categoria/productos/vitaminas-y-wellness/vitaminas/vitamina-c/",
                "https://www.chilesuplementos.cl/categoria/productos/vitaminas-y-wellness/vitaminas/vitamina-d/",
                "https://www.chilesuplementos.cl/categoria/productos/vitaminas-y-wellness/vitaminas/vitamina-k/",
                "https://www.chilesuplementos.cl/categoria/productos/vitaminas-y-wellness/vitaminas/multivitaminicos/"
            ],
            "Pre Entrenos": [
                "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/cafeina/",
                "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/beta-alanina/",
                "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/arginina/",
                "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/energeticas/",
                "https://www.chilesuplementos.cl/categoria/productos/snacks-y-comida/cafe/cafe-en-grano/",
                "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/requerimientos-especiales-pre-entrenos/libre-de-estimulantes/",
                "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/taurina/",
                "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/guarana/",
                "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/shots-y-geles/",
                "https://www.chilesuplementos.cl/categoria/productos/pre-entrenos/requerimientos-especiales-pre-entrenos/alto-en-estimulantes/"
            ],
            "Ganadores de Peso": [
                "https://www.chilesuplementos.cl/categoria/productos/ganadores-de-peso/"
            ],
            "Aminoacidos y BCAA": [
                "https://www.chilesuplementos.cl/categoria/productos/aminoacidos-y-bcaa/"
            ],
            "Glutamina": [
                "https://www.chilesuplementos.cl/categoria/productos/glutamina/"
            ],
            "Perdida de Grasa": [
                "https://www.chilesuplementos.cl/categoria/perdida-de-grasa/"
            ],
            "Post Entreno": [
                "https://www.chilesuplementos.cl/categoria/productos/post-entreno/"
            ],
            "Snacks y Comida": [
                "https://www.chilesuplementos.cl/categoria/productos/snacks-y-comida/"
            ]
        }
        
        selectors = {
            'product_card': '.archive-products .porto-tb-item', # Selector específico para la grilla principal
            'product_name': '.post-title',
            'brand': '.tb-meta-pwb-brand',
            'price': '.price',
            'link': '.post-title a',
            'rating': '.star-rating',
            'active_discount': '.onsale',
            'next_button': '.archive-products .next.page-numbers' # Botón específico de la paginación principal
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="ChileSuplementos")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en ChileSuplementos...[/green]")
        
        for main_category, urls in self.category_urls.items():
            for url in urls:
                # Subcategoria extraida de la URL
                subcategory_name = url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {subcategory_name} ({url})")
                
                try:
                    page.goto(url, wait_until="load", timeout=60000)
                    print(f"  > Título de la página: {page.title()}")
                    
                    last_product_count = 0
                    no_change_counter = 0
                    
                    while True:
                        # Esperar a que carguen los productos (o que haya al menos 1 si es el inicio)
                        try:
                            if last_product_count == 0: # Significa que es la primera vez que se ejecuta el bucle
                                print("  > Esperando selector de productos (.archive-products .porto-tb-item)...")
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
                                rating = "0" 
                                rating_elem = producto.locator(self.selectors['rating'])
                                if rating_elem.count() > 0:
                                    data_title = rating_elem.first.get_attribute("data-bs-original-title")
                                    if data_title:
                                        rating = data_title
                                    else:
                                        strong_rating = rating_elem.locator("strong.rating")
                                        if strong_rating.count() > 0:
                                            rating = strong_rating.first.inner_text().strip()

                                # Reviews (No disponible desde el grid al menos...)
                                reviews = "0"
                                
                                # Active Discount
                                active_discount = False
                                if producto.locator(self.selectors['active_discount']).count() > 0:
                                    active_discount = True
                                
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
                            
                            last_product_count = current_product_count
                            no_change_counter = 0 # Reset counter found new items
                    
                        else:
                            no_change_counter += 1 # Cada vez que no se encuentra un nuevo producto, se incrementa el contador.
                        
                        # Lógica de Paginación Híbrida (Botón "Cargar más" o Scroll)
                        load_more_btn = page.locator(self.selectors['next_button'])
                        
                        # Verificamos si existe en el DOM
                        if load_more_btn.count() > 0:
                            print(f"  > Botón 'Cargar más' detectado en el DOM.")
                            
                            # Intentar hacerlo visible si no lo es
                            if not load_more_btn.first.is_visible():
                                 print("  > El botón no es visible, intentando scroll rápido...")
                                 try:
                                     # Timeout reducido a 3000ms (3s) para no esperar 30s si está oculto/bugeado
                                     load_more_btn.first.scroll_into_view_if_needed(timeout=3000)
                                 except Exception as e:
                                     print(f"[yellow]  > No se pudo hacer scroll al botón (posiblemente oculto). Intentando click forzado de todas formas...[/yellow]")
                            
                            # Intentar Click
                            try:
                                print(f"  > Haciendo CLICK en 'Cargar más'...")
                                load_more_btn.first.click(force=True, timeout=3000)
                                print("  > Click enviado. Esperando 5 segundos para carga de productos...")
                                page.wait_for_timeout(5000) 
                            except Exception as e:
                                print(f"[yellow]  > Falló click normal ({e}). Intentando click JS...[/yellow]")
                                try:
                                    page.evaluate("arguments[0].click();", load_more_btn.first.element_handle())
                                    print("  > Click JS enviado. Esperando 5 segundos...")
                                    page.wait_for_timeout(5000)
                                except Exception as e_js:
                                    print(f"[red]  > Falló click JS también: {e_js}[/red]")
                                    print("  > Usando Scroll Fallback.")
                                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                    page.wait_for_timeout(2000)
                        else:
                            print(f"  > No se detectó botón 'Cargar más'.")
                           
                            print(f"  > Usando Scroll normal...")
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            page.wait_for_timeout(2000)
                        
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