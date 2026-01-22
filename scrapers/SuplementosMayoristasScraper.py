from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import time
from playwright.sync_api import TimeoutError

class SuplementosMayoristasScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        # Mapeo directo de las URLs proporcionadas
        self.categories_config = {
            "Proteinas": [
                # Subcategorías
                ("Whey Protein", "https://www.suplementosmayoristas.cl/proteinas/whey-protein"),
                ("Whey Isolate", "https://www.suplementosmayoristas.cl/proteinas/whey-Isolate"),
                ("Clear Whey Isolate", "https://www.suplementosmayoristas.cl/proteinas/clear-whey-isolate"),
                ("Proteinas Veganas", "https://www.suplementosmayoristas.cl/proteinas/proteinas-veganas")
            ],
            "Creatinas": [
                ("Micronizada", "https://www.suplementosmayoristas.cl/creatinas/micronizada"),
                ("Monohidratada", "https://www.suplementosmayoristas.cl/creatinas/monohidratada")
            ],
            "Quemadores": [
                ("Quemadores", "https://www.suplementosmayoristas.cl/quemadores")
            ],
            "Pre Entrenos": [
                 ("Pre Entrenos", "https://www.suplementosmayoristas.cl/pre-entreno")
            ],
            "Ganador de Masa": [
                ("Ganador de Masa", "https://www.suplementosmayoristas.cl/ganadores-de-masa-muscular")
            ],
            "Aminoacidos y BCAA": [
                ("BCAA", "https://www.suplementosmayoristas.cl/aminoacidos/bcaa"),
                ("EAA", "https://www.suplementosmayoristas.cl/aminoacidos/eaa"),
                ("HMB y ZMA", "https://www.suplementosmayoristas.cl/aminoacidos/hmb-y-zma"),
                ("Especificos", "https://www.suplementosmayoristas.cl/aminoacidos/especificos")
            ]
        }
        
        # Flatten URLs for BaseScraper init, though we will iterate manually
        all_urls = []
        for cat_list in self.categories_config.values():
            for sub, url in cat_list:
                all_urls.append(url)

        # VTEX Selectors
        selectors = {
            "product_card": "section.vtex-product-summary-2-x-container", # Container más robusto
            "link_container": "a.vtex-product-summary-2-x-clearLink", # Link principal
            
            "brand": ".vtex-product-summary-2-x-brandName, .vtex-product-summary-2-x-productBrandName",
            "name": ".vtex-product-summary-2-x-productBrand",
            "image": "img.vtex-product-summary-2-x-image",
            
            # Precio: a veces está dividido en partes, buscamos el contenedor
            "price_container": ".vtex-product-price-1-x-sellingPriceValue, .vtex-product-price-1-x-currencyContainer",
            
            # Stock: Botón "Agregar al carro" vs "Agotado"
            "add_to_cart_btn": ".vtex-add-to-cart-button-0-x-buttonDataContainer",
            "stock_badge": ".vtex-product-summary-2-x-element", # Textos genéricos en el card
            
            # Paginación "Mostrar más"
            "show_more_btn": ".vtex-search-result-3-x-buttonShowMore button" 
        }

        super().__init__(base_url, headless, category_urls=all_urls, selectors=selectors, site_name="SuplementosMayoristas")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.categories_config)} categorías en Suplementos Mayoristas...[/green]")
        
        for category_name, subcategories in self.categories_config.items():
            for subcategory_name, url in subcategories:
                print(f"\n[bold blue]Procesando:[/bold blue] {category_name} - {subcategory_name} ({url})")
                
                try:
                    page.goto(url, wait_until="networkidle", timeout=60000)
                except Exception as e:
                    print(f"[red]Error cargando {url}: {e}[/red]")
                    continue

                # Lógica de "Mostrar más" (Scroll Infinito manual)
                while True:
                    # Esperar carga inicial de productos
                    try:
                        page.wait_for_selector(self.selectors['link_container'], timeout=10000)
                    except:
                        print("[yellow]No se encontraron productos iniciales.[/yellow]")
                        break

                    # Intentar clic en "Mostrar más" repetidamente hasta que no haya más
                    show_more = page.locator(self.selectors['show_more_btn'])
                    if show_more.count() > 0 and show_more.is_visible():
                        print("Clic en 'Mostrar más'...")
                        try:
                            show_more.click()
                            time.sleep(3) # Esperar carga de nuevos productos
                            page.wait_for_load_state("networkidle")
                            continue # Volver a chequear si hay otro botón
                        except Exception as e:
                            print(f"[yellow]Error clicks mostrar más: {e}, asumiendo fin de lista.[/yellow]")
                            break
                    else:
                        break # Ya no hay botón mostrar más, estamos listos para extraer todo

                # Una vez cargado todo, extraemos
                print("Extrayendo productos cargados...")
                
                # Scroll al fondo para asegurar lazy load de imágenes
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)

                # Selectores a usar
                cards = page.locator(self.selectors['link_container'])
                count = cards.count()
                print(f"  > Encontrados {count} productos totales en la vista.")

                for i in range(count):
                    try:
                        card = cards.nth(i)
                        
                        # Data extraction
                        # Link
                        link = "N/D"
                        href = card.get_attribute("href")
                        if href:
                            link = self.base_url + href if href.startswith('/') else href

                        # Name
                        name = "N/D"
                        name_el = card.locator(self.selectors['name']).first
                        if name_el.count() > 0:
                            name = name_el.inner_text().strip()

                        # Brand
                        brand = "N/D"
                        brand_el = card.locator(self.selectors['brand']).first
                        if brand_el.count() > 0:
                            brand = brand_el.inner_text().strip()

                        # Image
                        image_url = ""
                        img_el = card.locator(self.selectors['image']).first
                        if img_el.count() > 0:
                            src = img_el.get_attribute("src")
                            if src:
                                image_url = src

                        # Price & Stock
                        # VTEX logic: Check availability container first
                        price = 0
                        
                        # Check for "Agotado" text anywhere in the card
                        card_text = card.inner_text().lower()
                        is_out_of_stock = "agotado" in card_text or "sin stock" in card_text or "avísame" in card_text
                        
                        # Also check if price container exists. Sometimes out of stock hides price.
                        price_el = card.locator(self.selectors['price_container']).first
                        
                        if is_out_of_stock:
                            price = 0
                        elif price_el.count() > 0:
                            price_text = price_el.inner_text()
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)
                        else:
                            # No stock msg but no price? treat as 0 or check if it's a layout issue
                            price = 0

                        current_date = datetime.now().strftime("%Y-%m-%d")

                        yield {
                            'date': current_date,
                            'site_name': self.site_name,
                            'category': category_name,
                            'subcategory': subcategory_name,
                            'product_name': name,
                            'brand': brand,
                            'price': price,
                            'link': link,
                            'rating': "0",
                            'reviews': "0",
                            'active_discount': False, # User said no discounts for now
                            'thumbnail_image_url': image_url,
                            'image_url': image_url,
                            'sku': "N/D",
                            'description': "N/D"
                        }

                    except Exception as e:
                        print(f"[red]Error extrayendo producto individual: {e}[/red]")
                        continue

if __name__ == "__main__":
    scraper = SuplementosMayoristasScraper("https://www.suplementosmayoristas.cl", headless=True)
    scraper.run()
