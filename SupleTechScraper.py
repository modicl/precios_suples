# Scraper para la pagina web SupleTech

from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class SupleTechScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        category_urls = {
            "Proteinas": [
                "https://www.supletech.cl/suplementos-alimenticios/proteinas/whey-protein/concentradas",
                "https://www.supletech.cl/suplementos-alimenticios/proteinas/whey-protein/isolate-protein",
                "https://www.supletech.cl/suplementos-alimenticios/proteinas/whey-protein/hidrolizadas",
                "https://www.supletech.cl/suplementos-alimenticios/proteinas/whey-protein/clear-whey-isolate",
                "https://www.supletech.cl/suplementos-alimenticios/proteinas/proteinas-veganas",
                "https://www.supletech.cl/suplementos-alimenticios/proteinas/proteinas-de-carne",
                "https://www.supletech.cl/suplementos-alimenticios/proteinas/colageno"
            ],
            "Creatinas": [
                "https://www.supletech.cl/suplementos-alimenticios/creatinas/micronizada",
                "https://www.supletech.cl/suplementos-alimenticios/creatinas/monohidratada"
            ],
            "Vitaminas": [
                "https://www.supletech.cl/bienestar/vitaminas-y-minerales",
                "https://www.supletech.cl/bienestar/probioticos-y-prebioticos",
                "https://www.supletech.cl/bienestar/omega",
                "https://www.supletech.cl/bienestar/biotina-y-colageno",
                "https://www.supletech.cl/bienestar/descanso-y-sueno",
                "https://www.supletech.cl/bienestar/gummies"
            ],
            "Pre Entrenos": [
                "https://www.supletech.cl/quemadores-y-pre-entrenos/pre-entreno"
            ],
            "Ganadores de Peso": [
                "https://www.supletech.cl/suplementos-alimenticios/aumento-de-masa-corporal"
            ],
            "Aminoacidos y BCAA": [
                "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/bcaa",
                "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/eaa",
                "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/hmb-y-zma",
                "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/especificos"
            ],
            "Perdida de Grasa": [
                "https://www.supletech.cl/quemadores-y-pre-entrenos/quemadores"
            ],
            "Snacks y Comida": [
                "https://www.supletech.cl/barritas-geles-y-liquidos/barritas-proteicas",
                "https://www.supletech.cl/barritas-geles-y-liquidos/shake-proteicos",
                "https://www.supletech.cl/barritas-geles-y-liquidos/bebidas-energeticas",
            ]
        }
        
        selectors = {
            "product_grid": "#gallery-layout-container", 
            'product_card': '.vtex-product-summary-2-x-container', 
            'product_name': 'span.vtex-product-summary-2-x-productBrand',
            'brand': 'span.vtex-store-components-3-x-productBrandName', 
            'price': '.vtex-product-price-1-x-currencyContainer', 
            'link': 'a.vtex-product-summary-2-x-clearLink', 
            'rating': '.vtex-reviews-and-ratings-3-x-stars', 
            'active_discount': '', 
            'next_button': '.vtex-search-result-3-x-buttonShowMore button, .vtex-search-result-3-x-buttonShowMore a' # Botón o Link "Mostrar más"
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="SupleTech")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en SupleTech...[/green]")
        
        for main_category, urls in self.category_urls.items():
            for url in urls:
                subcategory_name = url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {subcategory_name} ({url})")
                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    # Espera inicial generosa para que carguen los ratings de los primeros productos
                    page.wait_for_timeout(3000) 
                    
                    last_product_count = 0
                    retry_scrolls = 0
                    
                    while True:
                        # Esperar productos
                        try:
                            if last_product_count == 0:
                                page.wait_for_selector(self.selectors['product_card'], timeout=20000)
                        except:
                            print(f"[red]No se encontraron productos en {url}.[/red]")
                            break

                        producto_cards = page.locator(self.selectors['product_card'])
                        current_product_count = producto_cards.count()
                        
                        if current_product_count > last_product_count:
                            print(f"Indexando productos del {last_product_count + 1} al {current_product_count}...")
                            
                            for i in range(last_product_count, current_product_count):
                                producto = producto_cards.nth(i)
                                current_date = datetime.now().strftime("%Y-%m-%d")
                                
                                # Title
                                title = "N/D"
                                if producto.locator(self.selectors['product_name']).count() > 0:
                                    title = producto.locator(self.selectors['product_name']).first.inner_text().strip()
                                
                                # Brand (Estrategia Híbrida)
                                brand = "N/D"
                                # 1. Intentar por selector CSS
                                if producto.locator(self.selectors['brand']).count() > 0:
                                    brand = producto.locator(self.selectors['brand']).first.inner_text().strip()
                                
                                # 2. Fallback: Usar lógica del título (Ej: "CACAO COLLAGEN - PROCOLLAGEN" o "HMB ... – NUTREX")
                                if brand == "N/D" or len(brand) < 2: 
                                    # Normalizamos guiones (– por -) para cubrir casos como "– NUTREX"
                                    title_normalized = title.replace("–", "-").replace("—", "-")
                                    
                                    if "-" in title_normalized:
                                        # Tomamos lo que está después del último guión
                                        potential_brand = title_normalized.split("-")[-1].strip()
                                        if len(potential_brand) > 1:
                                            brand = potential_brand
                                
                                # Link
                                link = "N/D"
                                link_elem = producto.locator(self.selectors['link'])
                                if link_elem.count() > 0:
                                    href = link_elem.first.get_attribute("href")
                                    if href:
                                        link = self.base_url + href if href.startswith('/') else href

                                # Price (Lógica Especial VTEX)
                                price = 0
                                price_elems = producto.locator(self.selectors['price'])
                                price_count = price_elems.count()
                                
                                if price_count > 0:
                                    # "El primero siempre será el precio final"
                                    final_price_elem = price_elems.first 
                                    
                                    # Concatenar todos los spans dentro (miles, centenas, simbolo, etc)
                                    price_text = final_price_elem.inner_text() 
                                    
                                    try:
                                        clean_price = re.sub(r'[^\d]', '', price_text)
                                        if clean_price:
                                            price = int(clean_price)
                                    except:
                                        pass
                                
                                # Active Discount
                                # "Si hay dos precios... hay descuento"
                                active_discount = False
                                if price_count > 1:
                                    active_discount = True

                                # Rating
                                rating = "0"
                                try:
                                    rating_container = producto.locator(self.selectors['rating'])
                                    if rating_container.count() > 0:
                                        filled_stars = rating_container.locator('.vtex-reviews-and-ratings-3-x-star--filled').count()
                                        
                                        if filled_stars == 0:
                                            # Restauramos el retry, pero más paciente (300ms x 3) para asegurar carga
                                            for _ in range(3):
                                                page.wait_for_timeout(500)
                                                filled_stars = rating_container.locator('.vtex-reviews-and-ratings-3-x-star--filled').count()
                                                if filled_stars > 0: break
                                        
                                        if filled_stars > 0:
                                            rating = str(filled_stars)
                                except:
                                    pass
                                
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
                            
                            last_product_count = current_product_count
                            retry_scrolls = 0
                        else:
                            retry_scrolls += 1
                        
                        # Paginacion Infinita / Boton Load More
                        load_more = page.locator(self.selectors['next_button'])
                        
                        # Si existe el botón (visible o no), intentamos interactuar
                        if load_more.count() > 0:
                            print("  > Botón 'Mostrar más' detectado.")
                            try:
                                # Forzamos scroll y click JS por si acaso
                                load_more.first.scroll_into_view_if_needed(timeout=3000)
                                page.evaluate("arguments[0].click();", load_more.first.element_handle())
                                page.wait_for_timeout(3000) # Esperar a que carguen productos nuevos
                            except Exception as e:
                                print(f"  [yellow]Fallo click en 'Mostrar más': {e}, intentando click normal...[/yellow]")
                                load_more.first.click(force=True, timeout=3000)
                                page.wait_for_timeout(3000)
                        else:
                            # Scroll Down si no hay botón
                            # Si despues de 3 intentos (0, 1, 2) no hay productos nuevos, asumimos fin
                            if retry_scrolls >= 3:
                                print(f"  > Fin del scroll en {url} (Sin cambios tras 3 intentos).")
                                break
                            
                            print("  > Scroll 'Natural' (Abajo -> Arriba -> Espera) para detonar carga...")
                            # 1. Ir al fondo
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            page.wait_for_timeout(2000)
                            
                            # 2. Subir progresivamente (Simulando lectura/revisión)
                            page.evaluate("window.scrollBy(0, -700)")
                            page.wait_for_timeout(1000)
                            page.evaluate("window.scrollBy(0, -500)")
                            
                            # 3. Esperar tiempo generoso para que el sitio reaccione (3s pedido por user)
                            print("  > Esperando 3s a que aparezcan productos o botón...")
                            page.wait_for_timeout(3000)
                            
                            # 4. Volver a bajar un poco por si cargó algo intermedio o al final
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            page.wait_for_timeout(1000)

                except Exception as e:
                    print(f"[red]Error en categoría {url}: {e}[/red]")

if __name__ == "__main__":
    base_url = "https://www.supletech.cl"
    scraper = SupleTechScraper(base_url=base_url, headless=False)
    scraper.run()