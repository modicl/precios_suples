# Scraper para la pagina web ChileSuplementos.cl
# Contiene Infinite Scroll y a veces un boton "Cargar más"

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
            "Vitaminas y Minerales": [
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
            'product_card': '.archive-products .porto-tb-item', 
            'product_name': '.post-title',
            'brand': '.tb-meta-pwb-brand',
            'price': '.price',
            'link': '.post-title a',
            'rating': '.star-rating',
            'active_discount': '.onsale',
            'next_button': '.archive-products .next.page-numbers',
            'thumbnail': '.porto-tb-featured-image img, .porto-tb-woo-link img' # Better grid selectors
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="ChileSuplementos")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en ChileSuplementos...[/green]")
        context = page.context
        
        for main_category, urls in self.category_urls.items():
            for url in urls:
                subcategory_name = url.rstrip('/').split('/')[-1].replace('-', ' ').title()
                subcategory_name = self.clean_text(subcategory_name)
                
                if not subcategory_name or not subcategory_name.strip():
                    subcategory_name = "N/D"

                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {subcategory_name} ({url})")

                
                try:
                    page.goto(url, wait_until="load", timeout=60000)
                    
                    last_product_count = 0
                    no_change_counter = 0
                    
                    while True:
                        try:
                            if last_product_count == 0: 
                                print("  > Esperando selector de productos...")
                                page.wait_for_selector(self.selectors['product_card'], state="attached", timeout=30000)
                        except:
                            print(f"[red]No se encontraron productos en {url}.[/red]")
                            break

                        producto_cards = page.locator(self.selectors['product_card'])
                        current_product_count = producto_cards.count()
                        
                        if current_product_count > last_product_count:
                            print(f"Indexando productos del {last_product_count + 1} al {current_product_count}...")
                            
                            for i in range(last_product_count, current_product_count):
                                producto = producto_cards.nth(i)
                                current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                
                                # Title
                                title = "N/D"
                                title_elem = producto.locator(self.selectors['product_name']) 
                                if title_elem.count() > 0:
                                    raw_title = title_elem.first.inner_text()
                                    title = self.clean_text(raw_title)
                                
                                # Brand
                                brand = "N/D"
                                brand_elem = producto.locator(self.selectors['brand'])
                                if brand_elem.count() > 0:
                                    raw_brand = brand_elem.first.inner_text()
                                    brand = self.clean_text(raw_brand)

                                    
                                # Link
                                link = "N/D"
                                link_elem = producto.locator(self.selectors['link']).first
                                if link_elem.count() > 0:
                                    href = link_elem.get_attribute("href")
                                    if href:
                                        link = href

                                # Thumbnail
                                thumbnail_url = ""
                                thumb_elem = producto.locator(self.selectors['thumbnail']).first
                                if thumb_elem.count() > 0:
                                    # Prioritize lazy loading attributes
                                    possible_srcs = [
                                        thumb_elem.get_attribute("data-src"),
                                        thumb_elem.get_attribute("data-lazy-src"),
                                        thumb_elem.get_attribute("srcset"),
                                        thumb_elem.get_attribute("src")
                                    ]
                                    
                                    for src in possible_srcs:
                                        if src:
                                            # Handle srcset
                                            if "," in src:
                                                src = src.split(",")[0].split(" ")[0]
                                            
                                            # Filter placeholders
                                            lower_src = src.lower()
                                            if "placeholder" in lower_src or "logo" in lower_src or ".svg" in lower_src or "data:image" in lower_src:
                                                continue
                                                
                                            thumbnail_url = src
                                            break
                                
                                # Price
                                price = 0
                                price_elem = producto.locator(self.selectors['price'])
                                if price_elem.count() > 0:
                                    amounts = price_elem.locator(".woocommerce-Price-amount")
                                    if amounts.count() > 0:
                                        price_text = amounts.last.inner_text()
                                    else:
                                        price_text = price_elem.first.inner_text()
                                    
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
                                
                                # Active Discount
                                active_discount = False
                                if producto.locator(self.selectors['active_discount']).count() > 0:
                                    active_discount = True

                                # --- Detail Extraction (Multi-tab) ---
                                image_url = ""
                                sku = ""
                                description = ""
                                
                                if link != "N/D":
                                    detail_page = None
                                    try:
                                        detail_page = context.new_page()
                                        detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)
                                        
                                        # 1. Main Image
                                        # Priority: Open Graph > JSON-LD > DOM
                                        
                                        # Open Graph
                                        try:
                                            og_img = detail_page.locator('meta[property="og:image"]').first.get_attribute('content')
                                            if og_img:
                                                image_url = og_img
                                        except: pass

                                        # JSON-LD (if OG failed)
                                        if not image_url:
                                            try:
                                                json_img = detail_page.evaluate('''() => {
                                                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                                    for (const s of scripts) {
                                                        try {
                                                            const d = JSON.parse(s.innerText);
                                                            if (d['@type'] === 'Product' && d.image) return Array.isArray(d.image) ? d.image[0] : d.image;
                                                            if (d['@graph']) {
                                                                const p = d['@graph'].find(i => i['@type'] === 'Product');
                                                                if (p && p.image) return p.image;
                                                            }
                                                        } catch(e){}
                                                    }
                                                    return null;
                                                }''')
                                                if json_img:
                                                    image_url = json_img
                                            except: pass

                                        # DOM Fallback
                                        if not image_url:
                                            img_selectors = ['.woocommerce-product-gallery__image img', '.porto-product-main-image img', '.product-images img']
                                            for sel in img_selectors:
                                                img_el = detail_page.locator(sel).first
                                                if img_el.count() > 0:
                                                    # Prioritize high-res attributes
                                                    possible_srcs = [
                                                        img_el.get_attribute("data-large_image"),
                                                        img_el.get_attribute("data-src"),
                                                        img_el.get_attribute("href"),
                                                        img_el.get_attribute("src")
                                                    ]
                                                    
                                                    for src in possible_srcs:
                                                        if src:
                                                            lower_src = src.lower()
                                                            if "placeholder" in lower_src or "logo" in lower_src or ".svg" in lower_src or "data:image" in lower_src:
                                                                continue
                                                            image_url = src
                                                            break
                                                    if image_url:
                                                        break
                                        
                                        # 2. SKU
                                        sku_el = detail_page.locator('.sku, [itemprop="sku"]').first
                                        if sku_el.count() > 0:
                                            sku = sku_el.inner_text().strip()
                                        
                                        # 3. Description
                                        desc_el = detail_page.locator('.woocommerce-product-details__short-description').first
                                        if desc_el.count() > 0:
                                            description = desc_el.inner_text().strip()
                                        else:
                                            # Fallback to description tab
                                            desc_el = detail_page.locator('#tab-description, .description').first
                                        if desc_el.count() > 0:
                                            description = desc_el.inner_text().strip()

                                        detail_page.close()
                                    except Exception as e:
                                        print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                        try: detail_page.close()
                                        except: pass

                                # --- IMPLEMENTACIÓN DE DESCARGA ---
                                # Definir una subcarpeta limpia basada en el nombre del sitio
                                site_folder = self.site_name.replace(" ", "_").lower()

                                # Descargar Thumbnail
                                if thumbnail_url:
                                    local_thumb = self.download_image(thumbnail_url, subfolder=site_folder)
                                    if local_thumb:
                                        thumbnail_url = local_thumb

                                # Descargar Imagen Principal
                                if image_url:
                                    local_img = self.download_image(image_url, subfolder=site_folder)
                                    if local_img:
                                        image_url = local_img

                                yield {
                                    'date': current_date,
                                    'site_name': self.site_name,
                                    'category': self.clean_text(main_category),
                                    'subcategory': subcategory_name,
                                    'product_name': title,
                                    'brand': self.enrich_brand(brand, title),
                                    'price': price,
                                    'link': link,
                                    'rating': rating,
                                    'reviews': "0",
                                    'active_discount': active_discount,
                                    'thumbnail_image_url': thumbnail_url,
                                    'image_url': image_url,
                                    'sku': sku,
                                    'description': description
                                }
                            
                            last_product_count = current_product_count
                            no_change_counter = 0 
                        else:
                            no_change_counter += 1
                        
                        # Check if we should stop before trying to load more
                        if no_change_counter >= 2:
                            print("Se ha llegado al final del scroll.")
                            break

                        # Pagination logic
                        load_more_btn = page.locator(self.selectors['next_button'])
                        clicked = False
                        
                        if load_more_btn.count() > 0 and load_more_btn.first.is_visible():
                            print(f"  > Botón 'Cargar más' detectado.")
                            try:
                                load_more_btn.first.click(force=True)
                                clicked = True
                            except:
                                print(f"[yellow]  > Falló click normal. Intentando click JS...[/yellow]")
                                try:
                                    page.evaluate("arguments[0].click();", load_more_btn.first.element_handle())
                                    clicked = True
                                except Exception as e_js:
                                    print(f"[red]  > Falló click JS también: {e_js}[/red]")
                        
                        if not clicked:
                            # Scroll if button wasn't clicked
                            print(f"  > Buscando scroll...")
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        
                        # Smart wait for products to increase
                        try:
                            # Wait up to 3 seconds for new products to appear
                            # This is much faster than static sleep(5) or sleep(2)
                            page.wait_for_function(
                                f"document.querySelectorAll('{self.selectors['product_card']}').length > {last_product_count}",
                                timeout=3000
                            )
                        except:
                            # Timeout is expected when we reach the end
                            pass
                    
                except Exception as e:
                    print(f"[red]Error categoría {url}: {e}[/red]")

if __name__ == "__main__":
    scraper = ChileSuplementosScraper(headless=True)
    scraper.run()
