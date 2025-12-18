# Scapper para la pagina web AllNutrition
from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import csv
import re

class AllNutritionScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        # Categorias y sus URLs (Estructura Diccionario: Categoria -> [URLs])
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
            'title': '.c-card-producto__title h6', # Primario
            'title_secondary': '.c-card-product__title', # Secundario
            'vendor': '.c-card-product__vendor',
            'price': '.c-card-product__price',
            'price_old_nested': '.c-card-product__price-old', # Para limpiar
            'link': 'a.link--not-decoration',
            'rating': '.rating .rating-star',
            'reviews': '.rating-text-count',
            'active_discount': '.c-card-product__discount',
            'next_button': 'a[aria-label="Página siguiente"]'
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="AllNutrition")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en AllNutrition...[/green]")
        
        for main_category, urls in self.category_urls.items():
            for url in urls:
                # Subcategoria extraida de la URL (ej: /collections/whey-protein -> Whey Protein)
                subcategory_name = url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {subcategory_name} ({url})")
                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")
                        # Esperar a que carguen los productos
                        try:
                            # Esperar un poco más para asegurar que todo el contenido dinámico esté listo
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
                            
                            # Title
                            title = "N/D"
                            # Intentamos selector primario y secundario
                            if producto.locator(self.selectors['title']).count() > 0:
                                title = producto.locator(self.selectors['title']).first.inner_text().strip()
                            elif producto.locator(self.selectors['title_secondary']).count() > 0:
                                title = producto.locator(self.selectors['title_secondary']).first.inner_text().strip()
                            
                            # Brand
                            brand = "N/D"
                            if producto.locator(self.selectors['vendor']).count() > 0:
                                brand = producto.locator(self.selectors['vendor']).first.inner_text().strip()
                            
                            # Link
                            link = "N/D"
                            if producto.locator(self.selectors['link']).count() > 0:
                                href = producto.locator(self.selectors['link']).first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href

                            # Price
                            price = 0
                            price_elem = producto.locator(self.selectors['price'])
                            if price_elem.count() > 0:
                                # Usamos JS para excluir el texto de .c-card-product__price-old si está anidado
                                # Esto es necesario porque el precio en oferta y normal comparten contenedor padre
                                try:
                                    price_text = price_elem.first.evaluate(f"""(el, oldSelector) => {{
                                        const clone = el.cloneNode(true);
                                        const old = clone.querySelector(oldSelector);
                                        if (old) old.remove();
                                        return clone.innerText.trim();
                                    }}""", self.selectors['price_old_nested'])
                                    
                                    clean_price = re.sub(r'[^\d]', '', price_text)
                                    if clean_price:
                                        price = int(clean_price)
                                except Exception as e:
                                    # Fallback básico por si falla el JS
                                    price_text = price_elem.first.inner_text()
                                    clean_price = re.sub(r'[^\d]', '', price_text)
                                    if clean_price:
                                        price = int(clean_price)
                            
                            # Rating / Reviews
                            rating = "0"
                            if producto.locator(self.selectors['rating']).count() > 0:
                                # A veces es estilos, a veces texto. En AllNutrition simple suele no salir en tarjeta
                                pass 
                            
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

                        # Paginación: Buscar botón siguiente
                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href_next = next_btn.first.get_attribute("href")
                            if href_next:
                                print("  > Avanzando a la siguiente página...")
                                page.goto(self.base_url + href_next if href_next.startswith('/') else href_next)
                                page_number += 1
                                page.wait_for_timeout(2000)
                            else:
                                print("  > Click en botón Siguiente...")
                                next_btn.first.click()
                                page_number += 1
                                page.wait_for_timeout(3000)
                        else:
                            print("  > No hay más páginas en esta categoría.")
                            break
                
                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")

if __name__ == "__main__":
    # La URL base inicial puede ser cualquiera, pero el extract_process navegará a las categorías
    base_url = "https://allnutrition.cl" 
    scraper = AllNutritionScraper(base_url=base_url, headless=False)
    scraper.run()
