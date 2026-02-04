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
            "Vitaminas y Minerales": [
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
            ],
            "Por Objetivo": [
                "https://www2.suples.cl/collections/por-objetivo"
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
            'next_button': '.pagination__next',
            'thumbnail': 'img.product-item__primary-image' 
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="Suples.cl")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en Suples.cl...[/green]")
        
        context = page.context

        for main_category, urls in self.category_urls.items():
            batch_buffer = []
            for url in urls:
                subcategory_name = url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                subcategory_name = self.clean_text(subcategory_name)
                
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {subcategory_name} ({url})")

                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")
                        try:
                            # Increasing wait time slightly for robustness
                            page.wait_for_selector(self.selectors['product_card'], timeout=6000)
                        except:
                            print(f"[red]No se encontraron productos en {url} o tardó demasiado.[/red]")
                            break

                        producto_cards = page.locator(self.selectors['product_card'])
                        count = producto_cards.count()
                        print(f"  > Encontrados {count} productos en esta página.")
                        
                        for i in range(count):
                            producto = producto_cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # --- Grid Extraction ---
                            
                            # Link
                            link = "N/D"
                            if producto.locator(self.selectors['link']).count() > 0:
                                href = producto.locator(self.selectors['link']).first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href

                            # Title
                            title = "N/D"
                            if producto.locator(self.selectors['product_name']).count() > 0:
                                raw_title = producto.locator(self.selectors['product_name']).first.inner_text()
                                title = self.clean_text(raw_title)
                            
                            # Brand
                            brand = "N/D"
                            if producto.locator(self.selectors['brand']).count() > 0:
                                raw_brand = producto.locator(self.selectors['brand']).first.inner_text()
                                brand = self.clean_text(raw_brand)

                            
                            # Thumbnail
                            thumbnail_url = ""
                            if producto.locator(self.selectors['thumbnail']).count() > 0:
                                t_el = producto.locator(self.selectors['thumbnail']).first
                                
                                # Try to find real image in lazy-load attributes
                                t_srcset = t_el.get_attribute("data-srcset")
                                t_src = t_el.get_attribute("data-src") or t_el.get_attribute("src")

                                final_src = ""
                                if t_srcset:
                                    # data-srcset="url1 width1, url2 width2, ..."
                                    # Take the last one (usually highest res)
                                    candidates = t_srcset.split(",")
                                    if candidates:
                                        final_src = candidates[-1].strip().split(" ")[0]
                                elif t_src:
                                    # Often involves {width} template in shopify
                                    if "{width}" in t_src:
                                        final_src = t_src.replace("{width}", "500") # Good enough for thumbnail
                                    else:
                                        final_src = t_src
                                
                                if final_src:
                                    thumbnail_url = "https:" + final_src if final_src.startswith('//') else final_src

                            # Price
                            price = 0
                            active_discount = False
                            
                            price_container = producto.locator(self.selectors['price_container'])
                            if price_container.count() > 0:
                                highlight = price_container.locator(self.selectors['price_highlight'])
                                if highlight.count() > 0:
                                    price_text = highlight.first.inner_text()
                                    active_discount = True
                                else:
                                    normal = price_container.locator(self.selectors['price_default'])
                                    if normal.count() > 0:
                                        price_text = normal.first.inner_text()
                                    else:
                                        price_text = "0"
                            else:
                                general_price = producto.locator(self.selectors['price_default'])
                                if general_price.count() > 0:
                                    price_text = general_price.first.inner_text()
                                else:
                                    price_text = "0"
                                
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

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
                                        # Try .product-gallery__image img first
                                        img_el = detail_page.locator('.product-gallery__image img, .product-gallery__featured-image img').first
                                        if img_el.count() > 0:
                                            # Details page often has src loaded, or data-zoom
                                            src = img_el.get_attribute("src")
                                            if src and "base64" not in src:
                                                image_url = "https:" + src if src.startswith('//') else src
                                            else:
                                                 # Try data indicators if src is placeholder
                                                 data_zoom = img_el.get_attribute("data-zoom")
                                                 if data_zoom:
                                                     image_url = "https:" + data_zoom if data_zoom.startswith('//') else data_zoom
                                    
                                    # 2. SKU
                                    sku_el = detail_page.locator('.product-meta__sku').first
                                    if sku_el.count() > 0:
                                        sku_raw = sku_el.inner_text().strip()
                                        sku = sku_raw.replace("SKU:", "").strip()
                                    
                                    # 3. Description
                                    desc_el = detail_page.locator('.product-description, .rte').first
                                    if desc_el.count() > 0:
                                        description = desc_el.inner_text().strip()
                                        
                                    detail_page.close()
                                    
                                except Exception as e:
                                    print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass

                            # --- IMPLEMENTACIÓN DE DESCARGA ---
                            site_folder = self.site_name.replace(" ", "_").lower()
                            if thumbnail_url:
                                local_thumb = self.download_image(thumbnail_url, subfolder=site_folder)
                                if local_thumb: thumbnail_url = local_thumb
                            if image_url:
                                local_img = self.download_image(image_url, subfolder=site_folder)
                                if local_img: image_url = local_img

                            product_obj = {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(main_category),
                                'subcategory': subcategory_name,
                                'product_name': title,

                                'brand': self.enrich_brand(brand, title),
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

                            if main_category == "Por Objetivo":
                                batch_buffer.append(product_obj)
                                if len(batch_buffer) >= 20:
                                    classified_batch = self.classify_batch(batch_buffer)
                                    for cp in classified_batch:
                                        yield cp
                                    batch_buffer = []
                            else:
                                yield product_obj
                        
                        # Paginación
                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href_next = next_btn.first.get_attribute("href")
                            if href_next:
                                page.goto(self.base_url + href_next if href_next.startswith('/') else href_next)
                                page_number += 1
                                page.wait_for_timeout(2000)
                            else:
                                next_btn.first.click()
                                page.wait_for_timeout(3000)
                                page_number += 1
                        else:
                            break
                            
                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")
            
            # Flush remaining buffer for this category
            if main_category == "Por Objetivo" and batch_buffer:
                classified_batch = self.classify_batch(batch_buffer)
                for cp in classified_batch:
                    yield cp
                batch_buffer = []

if __name__ == "__main__":
    base_url = "https://www2.suples.cl"
    scraper = SuplesScraper(base_url=base_url, headless=True)
    scraper.run()