from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import unicodedata

class SuplementosBullChileScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        # Configuración de URLs y subcategorías por defecto
        # Nota: Como BullChile tiene URLs genéricas para categorías grandes,
        # usaremos heurística para refinar la subcategoría cuando sea posible.
        self.category_urls = {
            "Proteinas": [
                { "url": "https://www.suplementosbullchile.cl/proteinas", "subcategory": "Proteína de Whey" }
            ],
            "Creatinas": [
                { "url": "https://www.suplementosbullchile.cl/creatina", "subcategory": "Creatina Monohidrato" }
            ],
            "Pre Entrenos": [
                { "url": "https://www.suplementosbullchile.cl/pre-entreno", "subcategory": "Pre Entreno" }
            ],
            "Perdida de Grasa": [
                { "url": "https://www.suplementosbullchile.cl/quemador-energetico-1", "subcategory": "Quemadores" }
            ],
            "Packs": [
                { "url": "https://www.suplementosbullchile.cl/packs", "subcategory": "Packs" }
            ],
        }

        # Selectores identificados
        selectors = {
            "product_card": ".product-block",
            "product_name": ".product-block__name",
            "price_container": ".product-block__price", 
            "price_new": ".product-block__price--new", # Corrected BEM selector
            "price_old": ".product-block__price--old", # Corrected BEM selector
            "link": ".product-block__anchor",
            "image": ".product-block__image img",
            
            # Detail page
            "detail_sku": ".product-page__sku",
            "detail_desc": ".product-page__description",
            "detail_image": ".product-gallery__slide img",
            "detail_brand": ".product-page__brand", # Sometimes present
            "detail_title": "h1.product-page__title",
            # Pagination (Jumpseller standard often uses .pagination)
            "next_button": ".pagination .next a, a.next, .pagination .next, a:has-text('»')" 
        }

        super().__init__(base_url, headless, category_urls=self.category_urls, selectors=selectors, site_name="SuplementosBullChile")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping Determinista (V2) de SuplementosBullChile...[/green]")
        context = page.context

        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                deterministic_sub = item['subcategory']
                
                print(f"\n[bold blue]Procesando:[/bold blue] {main_category} -> {deterministic_sub} ({url})")
                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")
                        try:
                            page.wait_for_selector(self.selectors['product_card'], timeout=5000)
                        except:
                            print(f"[red]No se encontraron productos en la página {page_number} de {url}[/red]")
                            break
                        
                        cards = page.locator(self.selectors['product_card'])
                        count = cards.count()
                        print(f"  > Encontrados {count} productos.")

                        for i in range(count):
                            card = cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            # 1. Basic Info from Grid
                            title = "N/D"
                            if card.locator(self.selectors['product_name']).count() > 0:
                                raw_title = card.locator(self.selectors['product_name']).first.inner_text()
                                title = self.clean_text(raw_title)

                            link = "N/D"

                            if card.locator(self.selectors['link']).count() > 0:
                                href = card.locator(self.selectors['link']).first.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href

                            # Deduplication Check
                            if link != "N/D" and link in self.seen_urls:
                                print(f"[yellow]  >> Producto duplicado omitido: {title}[/yellow]")
                                continue
                            if link != "N/D":
                                self.seen_urls.add(link)

                            # Price extraction
                            price = 0
                            active_discount = False
                            price_text = "0"

                            # Try new price (sale price) - this is the final price
                            new_price_el = card.locator(self.selectors['price_new']).first
                            old_price_el = card.locator(self.selectors['price_old']).first

                            if new_price_el.count() > 0:
                                price_text = new_price_el.inner_text()
                                # IF there is also an old price, it's a discount
                                if old_price_el.count() > 0:
                                    active_discount = True
                            
                            # Fallback: if no specific new/old classes, try generic or old price as main if only that exists (unlikely in this theme)
                            elif old_price_el.count() > 0:
                                # Weird case, but if only old price exists, maybe it's not a discount? 
                                # Usually old price implies there is a new price. 
                                # Let's fallback to container if new price missing
                                if card.locator(self.selectors['price_container']).count() > 0:
                                     price_text = card.locator(self.selectors['price_container']).first.inner_text()
                            elif card.locator(self.selectors['price_container']).count() > 0:
                                price_text = card.locator(self.selectors['price_container']).first.inner_text()
                            
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

                            # Thumbnail
                            thumbnail_url = ""
                            if card.locator(self.selectors['image']).count() > 0:
                                src = card.locator(self.selectors['image']).first.get_attribute("src")
                                if src:
                                    thumbnail_url = "https:" + src if src.startswith('//') else src

                            # 2. Detail Extraction
                            image_url = ""
                            sku = ""
                            description = ""
                            brand = "N/D" # Brand often not in grid
                            detail_page = None

                            if link != "N/D":
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                                    
                                    # SKU and Brand from SKU text
                                    if detail_page.locator(self.selectors['detail_sku']).count() > 0:
                                        sku_text = detail_page.locator(self.selectors['detail_sku']).first.inner_text()
                                        if "|" in sku_text:
                                            parts = sku_text.split("|")
                                            sku = parts[0].replace('SKU:', '').strip()
                                            brand = parts[1].strip()
                                        else:
                                            sku = sku_text.replace('SKU:', '').strip()

                                    # Description con espera explícita
                                    try:
                                        detail_page.wait_for_selector(self.selectors['detail_desc'], timeout=5000)
                                        if detail_page.locator(self.selectors['detail_desc']).count() > 0:
                                            description = detail_page.locator(self.selectors['detail_desc']).first.inner_text().strip()
                                    except:
                                        pass

                                    # Brand (Try selector if not found in SKU)
                                    if brand == "N/D" and detail_page.locator(self.selectors['detail_brand']).count() > 0:
                                        raw_brand = detail_page.locator(self.selectors['detail_brand']).first.inner_text()
                                        brand = self.clean_text(raw_brand)
                                    
                                    # Main Image
                                    if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                        # Often these are sliders, take first
                                        img_src = detail_page.locator(self.selectors['detail_image']).first.get_attribute("src")
                                        if img_src:
                                            image_url = "https:" + img_src if img_src.startswith('//') else img_src
                                    
                                    # Fallback if no specific main image found, use thumbnail high res
                                    if not image_url and thumbnail_url:
                                        image_url = thumbnail_url.replace('240x240', '1024x1024') # Jumpseller/Shopify common pattern logic attempt

                                    detail_page.close()

                                except Exception as e:
                                    print(f"[yellow]Error consiguiendo detalles de {link}: {e}[/yellow]")
                                    if detail_page:
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

                            # --- HEURÍSTICAS DE CLASIFICACIÓN ---
                            final_category = main_category
                            final_sub = deterministic_sub
                            
                            def _normalize(text):
                                nfd = unicodedata.normalize('NFD', text)
                                return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')

                            # 1. Packs (Global)
                            title_lower = _normalize(title.lower())

                            # 0. Llaveros / Accesorios (Filtro Usuario)
                            if "llavero" in title_lower:
                                final_category = "OTROS"
                                final_sub = "Otros"

                            elif "pack" in title_lower or "paquete" in title_lower or "combo" in title_lower:
                                final_category = "Packs"
                                final_sub = "Packs"

                            # 1.5 Bebidas / RTD (Global)
                            # Detecta bebidas listas para tomar, shakes, etc.
                            # REGLA MEJORADA: 
                            # - Si dice explícitamente "RTD" o "Ready to Drink", es bebida.
                            # - Si dice "Batido" o "Shake" o "Bebida", DEBE tener indicador de volumen (ml, lt) O decir "Ready".
                            # - EXCLUSIÓN: Si tiene peso en LIBRAS (lb) o KILOS (kg) o GRAMOS (gr/g), ASUMIMOS POLVO y NO lo marcamos como bebida, 
                            #   a menos que diga explícitamente RTD.
                            
                            is_liquid = "ml" in title_lower or "lt" in title_lower or "cc" in title_lower or "botella" in title_lower
                            is_powder_weight = "lb" in title_lower or "kg" in title_lower or "gr" in title_lower or "servicios" in title_lower
                            
                            if ("rtd" in title_lower or "ready to drink" in title_lower):
                                final_category = "Bebidas Nutricionales"
                                final_sub = "Batidos de proteína"
                                
                            elif ("shake" in title_lower and "shaker" not in title_lower) or \
                                 "batido" in title_lower or \
                                 "bebida" in title_lower or \
                                 "hydration" in title_lower:
                                 
                                 # Si es un "Batido/Shake" pero tiene peso de polvo (2lb, 5kg), LO IGNORAMOS (es proteina en polvo)
                                 if not is_powder_weight:
                                     final_category = "Bebidas Nutricionales"
                                     final_sub = "Batidos de proteína"
                                 elif is_liquid:
                                     # Caso raro: Dice 2lb pero tambien 500ml? Priorizamos liquido si tiene ml explícito
                                     final_category = "Bebidas Nutricionales"
                                     final_sub = "Batidos de proteína"

                            # 2. Heurística para Proteínas (Ya que la categoría es genérica)
                            elif final_category == "Proteinas":
                                # Usamos Título + Descripción para mejor contexto
                                text_to_search = _normalize((title + " " + description).lower())
                                
                                # Palabras clave expandidas (Inglés y Español)
                                if re.search(r'\biso\b', text_to_search) or "isolate" in text_to_search or "aislada" in text_to_search or "isolated" in text_to_search or "isofit" in text_to_search or "isolatada" in text_to_search:
                                    final_sub = "Proteína Aislada"
                                elif "hydro" in text_to_search or "hidrolizada" in text_to_search or "hydrolized" in text_to_search or "hydrolyzed" in text_to_search or "hidrolizado" in text_to_search:
                                    final_sub = "Proteína Hidrolizada"
                                elif "vegan" in text_to_search or "plant" in text_to_search or "vegetal" in text_to_search or "vegana" in text_to_search or "vegano" in text_to_search or "plant based" in text_to_search:
                                    final_sub = "Proteína Vegana"
                                elif "beef" in text_to_search or "carne" in text_to_search or "vacuno" in text_to_search:
                                    final_sub = "Proteína de Carne"
                                elif "casein" in text_to_search or "caseina" in text_to_search or "micelar" in text_to_search or "micellar" in text_to_search:
                                    final_sub = "Caseína"
                                else:
                                    final_sub = "Proteína de Whey"

                            # 3. Heurística para Creatinas
                            elif final_category == "Creatinas":
                                text_to_search = _normalize((title + " " + (description or "")).lower())
                                
                                if "hcl" in text_to_search or "clorhidrato" in text_to_search or "hydrochloride" in text_to_search or "hidrocloruro" in text_to_search:
                                    final_sub = "Clorhidrato"
                                elif "malato" in text_to_search or "magnesio" in text_to_search or "magnapower" in text_to_search:
                                    final_sub = "Malato y Magnesio"
                                elif "nitrato" in text_to_search or "nitrate" in text_to_search:
                                    final_sub = "Nitrato"
                                elif "alkalyn" in text_to_search or "alcalina" in text_to_search:
                                    final_sub = "Otros Creatinas"
                                elif "monohidrat" in text_to_search or "monohydrate" in text_to_search or "creapure" in text_to_search:
                                    final_sub = "Creatina Monohidrato"
                                elif "micronizad" in text_to_search or "micronized" in text_to_search:
                                    final_sub = "Micronizada"
                                else:
                                    final_sub = "Creatina Monohidrato"

                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(final_category),
                                'subcategory': final_sub,
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
                        
                        # Pagination Logic
                        next_btn = page.locator(self.selectors['next_button'])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href_next = next_btn.first.get_attribute("href")
                            if href_next:
                                print(f"Navegando a página siguiente: {href_next}")
                                page.goto(href_next if href_next.startswith('http') else self.base_url + href_next)
                                page_number += 1
                            else:
                                break
                        else:
                            break

                except Exception as e:
                    print(f"[red]Error procesando categoría {main_category}: {e}[/red]")

if __name__ == "__main__":
    scraper = SuplementosBullChileScraper("https://www.suplementosbullchile.cl", headless=True)
    scraper.run()
