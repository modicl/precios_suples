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
                "https://www.suplestore.cl/collection/qm-y-l-carnitina",
                "https://www.suplestore.cl/collection/quemadores",
                "https://www.suplestore.cl/collection/cafeina"
            ],
            "Snacks y Comida": [
                "https://www.suplestore.cl/collection/barras-proteicas",
                "https://www.suplestore.cl/collection/energeticas-batidos-y-geles"
            ]
        }
        
        selectors = {
            "product_grid": ".row", 
            'product_card': '.bs-product', 
            'product_name': '.bs-product-info h6',
            'brand': '.bs-product-info .badge-secondary', 
            'price_final': '.bs-product-final-price', 
            'price_old': '.bs-product-old-price',
            'link': '.bs-product-info a', 
            'next_button': 'a.navegation.next, .pagination .next a',
            'thumbnail': 'img.card-img-top' # More specific
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="SupleStore")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en SupleStore...[/green]")
        
        context = page.context

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
                            
                            # --- Generic Extraction ---
                            
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
                            
                            # Thumbnail
                            thumbnail_url = ""
                            if producto.locator(self.selectors['thumbnail']).count() > 0:
                                t_el = producto.locator(self.selectors['thumbnail']).first
                                # Prioritize data-src for vanilla-lazyload
                                t_src = t_el.get_attribute("data-src") or t_el.get_attribute("src")
                                
                                if t_src and "base64" not in t_src:
                                    thumbnail_url = "https:" + t_src if t_src.startswith('//') else t_src
                                elif not t_src or "base64" in t_src:
                                    # Fallback: sometimes data-src is missing on load?
                                    # Try getting src again and see if it updated (unlikely without scroll)
                                    pass

                            # Price
                            price = 0
                            price_elem = producto.locator(self.selectors['price_final'])
                            if price_elem.count() > 0:
                                price_text = price_elem.first.inner_text() 
                                clean_price = re.sub(r'[^\d]', '', price_text)
                                if clean_price:
                                    price = int(clean_price)
                            
                            # Active Discount
                            active_discount = False
                            if producto.locator(self.selectors['price_old']).count() > 0:
                                active_discount = True

                            # --- Detail Extraction (Multi-tab) ---
                            image_url = ""
                            sku = ""
                            description = ""

                            if link != "N/D":
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)
                                    
                                    # 1. Main Image - Priority: Open Graph > JSON-LD > DOM
                                    
                                    # Open Graph
                                    try:
                                        og_img = detail_page.locator('meta[property="og:image"]').first.get_attribute('content')
                                        if og_img:
                                            image_url = og_img
                                    except: pass

                                    # JSON-LD
                                    if not image_url:
                                        try:
                                            json_img = detail_page.evaluate('''() => {
                                                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                                for (const s of scripts) {
                                                    try {
                                                        const d = JSON.parse(s.innerText);
                                                        if ((d['@type'] === 'Product' || d['@type'] === 'ProductGroup') && d.image) 
                                                            return Array.isArray(d.image) ? d.image[0] : d.image;
                                                    } catch(e){}
                                                }
                                                return null;
                                            }''')
                                            if json_img:
                                                image_url = json_img
                                        except: pass

                                    # DOM Fallback
                                    if not image_url:
                                        img_el = detail_page.locator('img.imagenpe, .bs-product-image img, .bs-img-square img').first
                                        if img_el.count() > 0:
                                            src = img_el.get_attribute("data-zoom") or img_el.get_attribute("src")
                                            if src:
                                                image_url = "https:" + src if src.startswith('//') else src
                                    
                                    # 2. SKU
                                    sku_el = detail_page.locator('[data-bs="product.sku"]').first
                                    if sku_el.count() > 0:
                                        sku_raw = sku_el.inner_text().strip()
                                        sku = sku_raw.replace("SKU:", "").strip()
                                    
                                    # 3. Description
                                    desc_el = detail_page.locator('#home, .bs-product-description').first
                                    if desc_el.count() > 0:
                                        description = desc_el.inner_text().strip()
                                        
                                    detail_page.close()
                                    
                                except Exception as e:
                                    print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass

                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': main_category,
                                'subcategory': subcategory_name,
                                'product_name': title,
                                'brand': brand,
                                'price': price,
                                'link': link,
                                'rating': "0",
                                'reviews': "0",
                                'active_discount': active_discount,
                                'thumbnail_image_url': thumbnail_url,
                                'image_url': image_url,
                                'sku': sku,
                                'description': description
                            }
                        
                        # Paginación
                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href = next_btn.first.get_attribute("href")
                            if href:
                                print(f"  > Avanzando a página {page_number + 1}...")
                                page.goto(self.base_url + href if href.startswith('/') else href)
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
    base_url = "https://www.suplestore.cl"
    scraper = SupleStoreScraper(base_url=base_url, headless=True)
    scraper.run()