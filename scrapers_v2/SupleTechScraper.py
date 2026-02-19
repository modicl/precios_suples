# Scraper para la pagina web SupleTech.cl

from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from rich import print
from datetime import datetime
import re

class SupleTechScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        # Structure: "Category Name": [ { "url": "...", "subcategory": "Exact DB Subcategory" } ]
        category_urls = {
            "Proteinas": [
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/whey-protein/concentradas", "subcategory": "Proteína de Whey" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/whey-protein/isolate-protein", "subcategory": "Proteína Aislada" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/whey-protein/hidrolizadas", "subcategory": "Proteína Hidrolizada" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/whey-protein/clear-whey-isolate", "subcategory": "Proteína Aislada" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/proteinas-veganas", "subcategory": "Proteína Vegana" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/proteinas-de-carne", "subcategory": "Proteína de Carne" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/caseina", "subcategory": "Caseína" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/otras-proteinas", "subcategory": "Otros Proteinas" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/blend", "subcategory": "Proteína de Whey" }
            ],
            "Creatinas": [
                { "url": "https://www.supletech.cl/suplementos-alimenticios/creatinas/micronizada", "subcategory": "Micronizada" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/creatinas/monohidratada", "subcategory": "Creatina Monohidrato" }
            ],
            "Vitaminas y Minerales": [
                { "url": "https://www.supletech.cl/bienestar/vitaminas-y-minerales", "subcategory": "Vitaminas y Minerales" },
                { "url": "https://www.supletech.cl/bienestar/probioticos-y-prebioticos", "subcategory": "Probióticos" },
                { "url": "https://www.supletech.cl/bienestar/omega", "subcategory": "Omega 3 y Aceites" },
                { "url": "https://www.supletech.cl/bienestar/biotina-y-colageno", "subcategory": "Vitaminas y Minerales" },
                { "url": "https://www.supletech.cl/bienestar/descanso-y-sueno", "subcategory": "Bienestar General" },
                { "url": "https://www.supletech.cl/bienestar/gummies", "subcategory": "Gummies" }
            ],
            "Pre Entrenos": [
                { "url": "https://www.supletech.cl/quemadores-y-pre-entrenos/pre-entreno", "subcategory": "Pre Entreno" }
            ],
            "Ganadores de Peso": [
                { "url": "https://www.supletech.cl/suplementos-alimenticios/aumento-de-masa-corporal", "subcategory": "Ganadores de Peso" }
            ],
            "Aminoacidos y BCAA": [
                { "url": "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/bcaa", "subcategory": "BCAAs" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/eaa", "subcategory": "EAAs (Esenciales)" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/especificos", "subcategory": "Otros Aminoacidos y BCAA" },
                { "url": "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/hmb-y-zma", "subcategory": "DETECTAR_HMB_ZMA" }
            ],
            "Perdida de Grasa": [
                { "url": "https://www.supletech.cl/quemadores-y-pre-entrenos/quemadores", "subcategory": "Quemadores" }
            ],
            "Snacks y Comida": [
                { "url": "https://www.supletech.cl/barritas-geles-y-liquidos/barritas-proteicas", "subcategory": "Barritas Y Snacks Proteicas" },
                { "url": "https://www.supletech.cl/barritas-geles-y-liquidos/shake-proteicos", "subcategory": "Shake Proteicos" }
            ],
            "Colageno": [
                { "url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/colageno", "subcategory": "Colageno" }
            ],
            "Bebidas Nutricionales": [
                { "url": "https://www.supletech.cl/barritas-geles-y-liquidos/bebidas-energeticas", "subcategory": "Bebidas Nutricionales" }
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
            'next_button': '.vtex-search-result-3-x-buttonShowMore button, .vtex-search-result-3-x-buttonShowMore a',
            'thumbnail': '.vtex-product-summary-2-x-imageNormal'
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="SupleTech")
        self.classifier = CategoryClassifier()

    def extract_process(self, page):
        print(f"[green]Iniciando scraping Determinista (V2) de SupleTech...[/green]")
        context = page.context
        
        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                deterministic_sub = item['subcategory']
                
                print(f"\n[bold blue]Procesando:[/bold blue] {main_category} -> {deterministic_sub} ({url})")

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_timeout(3000) 
                    
                    last_product_count = 0
                    retry_scrolls = 0
                    
                    while True:
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
                                current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                
                                # --- Grid Extraction ---
                                
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

                                # Brand fallback
                                if brand == "N/D" or len(brand) < 2: 
                                    title_normalized = title.replace("–", "-").replace("—", "-")
                                    if "-" in title_normalized:
                                        potential_brand = title_normalized.split("-")[-1].strip()
                                        if len(potential_brand) > 1:
                                            brand = self.clean_text(potential_brand)

                                # Link
                                link = "N/D"
                                link_elem = producto.locator(self.selectors['link'])
                                if link_elem.count() > 0:
                                    href = link_elem.first.get_attribute("href")
                                    if href:
                                        link = self.base_url + href if href.startswith('/') else href

                                # Deduplication Check
                                if link != "N/D" and link in self.seen_urls:
                                    print(f"[yellow]  >> Producto duplicado omitido: {title}[/yellow]")
                                    continue
                                if link != "N/D":
                                    self.seen_urls.add(link)

                                # Thumbnail
                                thumbnail_url = ""
                                if producto.locator(self.selectors['thumbnail']).count() > 0:
                                    t_src = producto.locator(self.selectors['thumbnail']).first.get_attribute("src")
                                    if t_src:
                                        thumbnail_url = t_src

                                # Price
                                price = 0
                                price_elems = producto.locator(self.selectors['price'])
                                price_count = price_elems.count()
                                if price_count > 0:
                                    final_price_elem = price_elems.first 
                                    price_text = final_price_elem.inner_text() 
                                    try:
                                        clean_price = re.sub(r'[^\d]', '', price_text)
                                        if clean_price:
                                            price = int(clean_price)
                                    except:
                                        pass
                                
                                # Active Discount
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
                                            for _ in range(3):
                                                page.wait_for_timeout(500)
                                                filled_stars = rating_container.locator('.vtex-reviews-and-ratings-3-x-star--filled').count()
                                                if filled_stars > 0: break
                                        if filled_stars > 0:
                                            rating = str(filled_stars)
                                except:
                                    pass
                                
                                # --- Detail Extraction (Multi-tab) ---
                                image_url = ""
                                sku = ""
                                description = ""
                                
                                if link != "N/D":
                                    try:
                                        detail_page = context.new_page()
                                        detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)
                                        
                                        # 1. Main Image (HD)
                                        img_el = detail_page.locator('.vtex-store-components-3-x-productImageTag--main').first
                                        if img_el.count() > 0:
                                            src = img_el.get_attribute("src")
                                            if src:
                                                image_url = src
                                        
                                        # 2. SKU
                                        # Try element first
                                        sku_el = detail_page.locator('.vtex-product-identifier-0-x-product-identifier__value').first
                                        if sku_el.count() > 0:
                                            sku = sku_el.inner_text().strip()
                                        else:
                                            # Try meta
                                            meta_sku = detail_page.locator('meta[property="product:retailer_item_id"]').first
                                            if meta_sku.count() > 0:
                                                sku = meta_sku.get_attribute("content")
                                        
                                        # 3. Description
                                        desc_el = detail_page.locator('.vtex-store-components-3-x-productDescriptionText')
                                        if desc_el.count() > 0:
                                            description = desc_el.first.inner_text().strip()
                                            
                                        detail_page.close()
                                    except Exception as e:
                                        print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                        try: detail_page.close()
                                        except: pass

                                # Temp vars to avoid polluting the loop state
                                final_category = main_category
                                final_sub = deterministic_sub

                                # Clasificación usando CategoryClassifier para DETECTAR_HMB_ZMA y Bienestar General
                                final_category, final_sub = self.classifier.classify(
                                    title, description, main_category, deterministic_sub, brand
                                )

                                yield {
                                    'date': current_date,
                                    'site_name': self.site_name,
                                    'category': self.clean_text(final_category),
                                    'subcategory': final_sub, # EXPLICIT
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
                            retry_scrolls = 0
                        else:
                            retry_scrolls += 1
                        
                        # Load More / Infinite Scroll Logic
                        load_more = page.locator(self.selectors['next_button'])
                        if load_more.count() > 0:
                            print("  > Botón 'Mostrar más' detectado.")
                            try:
                                load_more.first.scroll_into_view_if_needed(timeout=3000)
                                page.evaluate("arguments[0].click();", load_more.first.element_handle())
                                page.wait_for_timeout(3000) 
                            except Exception as e:
                                print(f"  [yellow]Fallo click en 'Mostrar más': {e}, intentando click normal...[/yellow]")
                                load_more.first.click(force=True, timeout=3000)
                                page.wait_for_timeout(3000)
                        else:
                            if retry_scrolls >= 3:
                                print(f"  > Fin del scroll en {url} (Sin cambios tras 3 intentos).")
                                break
                            
                            print("  > Scroll 'Natural' para cargar más...")
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            page.wait_for_timeout(2000)
                            page.evaluate("window.scrollBy(0, -700)")
                            page.wait_for_timeout(1000)
                            page.wait_for_timeout(3000)
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            page.wait_for_timeout(1000)

                except Exception as e:
                    print(f"[red]Error en categoría {url}: {e}[/red]")

if __name__ == "__main__":
    base_url = "https://www.supletech.cl"
    scraper = SupleTechScraper(base_url=base_url, headless=True)
    scraper.run()
