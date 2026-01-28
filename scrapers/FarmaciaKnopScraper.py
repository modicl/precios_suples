from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import time

class FarmaciaKnopScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        # Mapeo de URLs
        self.categories_config = {
            "Proteinas": "https://www.farmaciasknop.com/types/proteinas"
        }

        # Selectores identificados
        selectors = {
            "product_card": "div.product-item",
            "name": "h3.product-name a", # Nombre dentro del link
            "brand": "span.product-vendor", # Marca 
            "image": "a.product-link img", # Imagen dentro del link principal
            
            # Link del producto
            "link": "a.product-link",
            
            # Precios
            # Contenedor precio
            "price_container": ".prices",
            # Precio oferta o normal 
            "current_price": "span.bootic-price", 
            # Precio tachado
            "old_price": "strike.bootic-price-comparison, .old-price",
            
            # Stock: Botón "Notificarme" indica sin stock
            "notify_button": "button.notify-in-stock, button:has-text('Notificarme')",
            
            # Selectores de página de detalle
            "detail_image": ".product-image-container img, img[class*='product']",
            "description": "p:has-text('Beneficios'), .product-description, p:has-text('Presentación')",
            "sku": "p:has-text('SKU:')"
        }

        super().__init__(base_url, headless, category_urls=list(self.categories_config.values()), selectors=selectors, site_name="Farmacia Knopp")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.categories_config)} categorías en Farmacia Knop...[/green]")
        
        for category_name, url in self.categories_config.items():
            print(f"\n[bold blue]Procesando categoría:[/bold blue] {category_name} ({url})")
            
            try:
                page.goto(url, wait_until="networkidle", timeout=60000)
            except Exception as e:
                print(f"[red]Error cargando {url}: {e}[/red]")
                continue

            # Scroll infinito / Carga completa
            # Farmacia Knop parece cargar todo, pero aseguramos con scroll
            last_height = page.evaluate("document.body.scrollHeight")
            while True:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Extracción
            cards = page.locator(self.selectors['product_card'])
            count = cards.count()
            print(f"  > Encontrados {count} productos totales.")
            
            for i in range(count):
                try:
                    card = cards.nth(i)
                    
                    # 1. Extracción de datos básicos
                    name = "N/D"
                    name_el = card.locator(self.selectors['name']).first
                    if name_el.count() > 0:
                        raw_name = name_el.inner_text()
                        name = self.clean_text(raw_name)
                    else:
                        # Fallback: title attribute or text inside h3 a
                        name_backup = card.locator("h3.product-title a").first
                        if name_backup.count() > 0:
                            raw_name = name_backup.get_attribute("title") or name_backup.inner_text()
                            name = self.clean_text(raw_name)

                    brand = "N/D"
                    brand_el = card.locator(self.selectors['brand']).first
                    if brand_el.count() > 0:
                        raw_brand = brand_el.inner_text()
                        brand = self.clean_text(raw_brand)
                        # Limpieza si trae basura como "% dcto" en caso de error de selector

                        if "%" in brand or "$" in brand:
                             brand = "N/D"

                    link = "N/D"
                    link_el = card.locator(self.selectors['link']).first
                    if link_el.count() > 0:
                        href = link_el.get_attribute("href")
                        if href:
                            link = self.base_url + href if href.startswith('/') else href

                    image_url = ""
                    img_el = card.locator(self.selectors['image']).first
                    if img_el.count() > 0:
                        src = img_el.get_attribute("src")
                        if src:
                            image_url = src
                    
                    # 2. Stock Logic
                    price = 0
                    active_discount = False
                    
                    # Si existe botón "Notificarme", precio es 0
                    # Chequear también p.units-in-stock.no-stock
                    if card.locator(self.selectors['notify_button']).count() > 0 or \
                       card.locator("p.units-in-stock.no-stock").count() > 0:
                         price = 0
                    else:
                        # 3. Price Logic
                        # Buscar precio tachado para saber si hay descuento
                        old_price_el = card.locator(self.selectors['old_price']).first
                        has_old_price = old_price_el.count() > 0 and old_price_el.is_visible()
                        
                        active_discount = has_old_price
                        
                        # Precio actual (bootic-price)
                        current_price_el = card.locator(self.selectors['current_price']).first
                        if current_price_el.count() > 0:
                            price_text = current_price_el.inner_text()
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)
                        else:
                            # Fallback raro
                            price = 0

                    # DETAIL EXTRACTION (NEW TAB)
                    detail_image_url = image_url  # Fallback a thumbnail
                    sku = "N/D"
                    description = "N/D"
                    
                    if link != "N/D":
                        context = page.context
                        detail_page = None
                        try:
                            detail_page = context.new_page()
                            detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                            detail_page.wait_for_load_state("networkidle")  # Wait for dynamic content
                            
                            # Extraer imagen full
                            img_selector = self.selectors['detail_image']
                            if detail_page.locator(img_selector).count() > 0:
                                detail_image_url = detail_page.locator(img_selector).first.get_attribute('src')
                            
                            # Extraer descripción - puede estar en una sección colapsable
                            desc_selector = self.selectors['description']
                            if detail_page.locator(desc_selector).count() > 0:
                                description = detail_page.locator(desc_selector).first.inner_text().strip()
                            
                            # Extraer SKU - buscar por texto que contenga "SKU:"
                            sku_selector = self.selectors['sku']
                            if detail_page.locator(sku_selector).count() > 0:
                                sku_text = detail_page.locator(sku_selector).first.inner_text()
                                # Extraer solo el código SKU
                                sku_match = re.search(r'SKU:\s*([A-Z0-9-]+)', sku_text, re.IGNORECASE)
                                if sku_match:
                                    sku = sku_match.group(1)
                            
                            detail_page.close()
                        except Exception as e:
                            print(f"[red]Error extrayendo detalle de {link}: {e}[/red]")
                            if detail_page:
                                detail_page.close()
                    
                    current_date = datetime.now().strftime("%Y-%m-%d")

                    yield {
                        'date': current_date,
                        'site_name': self.site_name,
                        'category': self.clean_text(category_name),
                        'subcategory': self.clean_text(category_name),
                        'product_name': name,
                        'brand': self.enrich_brand(brand, name),
                        'price': price,
                        'link': link,
                        'rating': "0",
                        'reviews': "0",
                        'active_discount': active_discount,
                        'thumbnail_image_url': image_url,
                        'image_url': detail_image_url,
                        'sku': sku,
                        'description': description
                    }

                except Exception as e:
                    print(f"[red]Error extrayendo producto: {e}[/red]")
                    continue

if __name__ == "__main__":
    scraper = FarmaciaKnopScraper("https://www.farmaciasknop.com", headless=True)
    scraper.run()
