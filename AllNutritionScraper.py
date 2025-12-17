# Scapper para la pagina web AllNutrition
from BaseScraper import BaseScraper
from rich import print

from datetime import datetime
import csv
import re

class AllNutritionScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        category_urls = [
            "https://allnutrition.cl/collections/whey-protein",
            "https://allnutrition.cl/collections/proteinas-isoladas",
            "https://allnutrition.cl/collections/proteinas-de-carne",
            "https://allnutrition.cl/collections/proteinas-liquidas",
            "https://allnutrition.cl/collections/proteinas-veganas",
            "https://allnutrition.cl/collections/proteinas-vegetarianas",
            "https://allnutrition.cl/collections/barras-proteicas",
            "https://allnutrition.cl/collections/snack-proteico",
            "https://allnutrition.cl/collections/ganadores-de-peso",
            "https://allnutrition.cl/collections/proteinas-de-caseina",
            "https://allnutrition.cl/collections/creatinas",
            "https://allnutrition.cl/collections/pre-workout",
            "https://allnutrition.cl/collections/guarana-y-cafeina",
            "https://allnutrition.cl/collections/carbohidratos",
            "https://allnutrition.cl/collections/arginina",
            "https://allnutrition.cl/collections/barras-proteicas",
            "https://allnutrition.cl/collections/barras-saludables",
            "https://allnutrition.cl/collections/geles",
            "https://allnutrition.cl/collections/liquidos",
            "https://allnutrition.cl/collections/bcaa-1",
            "https://allnutrition.cl/collections/glutaminas",
            "https://allnutrition.cl/collections/aminoacidos",
            "https://allnutrition.cl/collections/zma",
            "https://allnutrition.cl/collections/isotonico"
        ]
        
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

        # Le pasamos los atributos al super constructor
        super().__init__(base_url, headless, category_urls, selectors, site_name="AllNutrition")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías...[/green]")
        
        for url in self.category_urls:
            # Extraer nombre limpio de categoria
            category_name = url.rstrip('/').split('/')[-1].replace('-', ' ')
            
            print(f"\n[bold blue]Procesando categoría:[/bold blue] {category_name} ({url})")
            try:
                page.goto(url, wait_until="domcontentloaded")
                
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
                    total_productos = producto_cards.count()
                    
                    print(f"Se encontraron {total_productos} productos en esta página.")
                    
                    for i in range(total_productos):
                        producto = producto_cards.nth(i)
                        
                        # --- Extracción de Datos ---
                        
                        # Fecha actual
                        current_date = datetime.now().strftime("%Y-%m-%d")

                        # Título
                        title = "N/D"
                        title_h6 = producto.locator(self.selectors['title'])
                        if title_h6.count() > 0:
                            title = title_h6.first.inner_text().strip()
                        else:
                            title_div = producto.locator(self.selectors['title_secondary'])
                            if title_div.count() > 0:
                                title = title_div.first.inner_text().strip()
                        
                        # Vendor
                        vendor = "N/D"
                        vendor_elem = producto.locator(self.selectors['vendor'])
                        if vendor_elem.count() > 0:
                            vendor = vendor_elem.first.inner_text().strip()
                        
                        # Price
                        price = 0
                        price_elem = producto.locator(self.selectors['price'])
                        if price_elem.count() > 0:
                            # Usamos JS para excluir el texto de .c-card-product__price-old si está anidado
                            # Pasamos el selector del old price como argumento
                            price_text = price_elem.first.evaluate(f"""(el, oldSelector) => {{
                                const clone = el.cloneNode(true);
                                const old = clone.querySelector(oldSelector);
                                if (old) old.remove();
                                return clone.innerText.trim();
                            }}""", self.selectors['price_old_nested'])
                            
                            # Limpieza y conversión
                            try:
                                clean_price = price_text.replace("$", "").replace("CLP", "").replace(" ", "").replace(".", "").strip()
                                if clean_price:
                                    price = int(clean_price)
                            except Exception as e:
                                print(f"[yellow]Error parsing price '{price_text}': {e}[/yellow]")
                        
                        # Link
                        link = "N/D"
                        link_elem = producto.locator(self.selectors['link']).first
                        if link_elem.count() > 0:
                            href = link_elem.get_attribute("href")
                            if href:
                                link = f"https://allnutrition.cl{href}" if href.startswith("/") else href

                        # Rating
                        rating = "N/D"
                        rating_elem = producto.locator(self.selectors['rating'])
                        if rating_elem.count() > 0:
                            style = rating_elem.first.get_attribute("style")
                            if style:
                                # style="--rating: 5; --rating-max: 5.0; ..."
                                match = re.search(r"--rating:\s*([\d\.]+)", style)
                                if match:
                                    rating = match.group(1)

                        # Reviews
                        reviews = "0"
                        reviews_elem = producto.locator(self.selectors['reviews'])
                        if reviews_elem.count() > 0:
                            reviews_text = reviews_elem.first.inner_text().strip()
                            # "(10)" -> "10"
                            match = re.search(r"\d+", reviews_text)
                            if match:
                                reviews = match.group(0)

                        # Active Discount
                        active_discount = False
                        # Se usa un selector más general
                        discount_elem = producto.locator(self.selectors['active_discount'])
                        if discount_elem.count() > 0:
                            # Verifica si tiene texto dentro
                            if discount_elem.first.inner_text().strip():
                                active_discount = True

                        print(f"  - {title} | {vendor} | {price} | Discount: {active_discount}")
                        
                        yield {
                            'date': current_date,
                            'site_name': self.site_name,
                            'category': category_name,
                            'product_name': title,
                            'brand': vendor,
                            'price': price,
                            'link': link,
                            'rating': rating,
                            'reviews': reviews,
                            'active_discount': active_discount
                        }
                    
                    # Lógica de Paginación
                    next_button = page.locator(self.selectors['next_button'])
                    if next_button.count() > 0:
                        print("Buscando siguiente página...")
                        try:
                            next_url = next_button.get_attribute("href")
                            if next_url:
                                print(f"Navegando a siguiente URL: {next_url}")
                                page.goto(f"https://allnutrition.cl{next_url}" if next_url.startswith("/") else next_url, wait_until="domcontentloaded")
                            else:
                                print("Haciendo click en botón siguiente...")
                                next_button.click()
                                page.wait_for_load_state("domcontentloaded")
                            
                            page_number += 1
                        except Exception as e:
                            print(f"[yellow]Error al intentar ir a la siguiente página: {e}. Terminando categoría.[/yellow]")
                            break
                    else:
                        print("No se encontró botón de siguiente página. Fin de la categoría.")
                        break

            except Exception as e:
                print(f"[red]Error procesando {url}: {e}[/red]")
        
        print("[bold green]Scraping de todas las categorías finalizado.[/bold green]")

if __name__ == "__main__":
    # La URL base inicial puede ser cualquiera, pero el extract_process navegará a las categorías
    base_url = "https://allnutrition.cl" 
    scraper = AllNutritionScraper(base_url=base_url, headless=False)
    scraper.run()
