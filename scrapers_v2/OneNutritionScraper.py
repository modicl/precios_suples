# Scraper para la pagina web OneNutrition.cl
from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import unicodedata

class OneNutritionScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        
        category_urls = {
            "Proteinas": [
                {"url": "https://onenutrition.cl/tienda/proteinas", "subcategory": "Proteína de Whey"}
            ],
            "Creatinas": [
                {"url": "https://onenutrition.cl/tienda/creatinas", "subcategory": "Creatina Monohidrato"}
            ],
            "Vitaminas y Minerales": [
                {"url": "https://onenutrition.cl/tienda/vitaminas-salud", "subcategory": "Vitaminas y Minerales"}
            ],
            "Pre Entrenos": [
                {"url": "https://onenutrition.cl/tienda/energia-resistencia", "subcategory": "Pre Entreno"}
            ],
            "Ganadores de Peso": [
                {"url": "https://onenutrition.cl/tienda/ganadores-de-masa-", "subcategory": "Ganadores De Peso"}
            ],
            "Aminoacidos y BCAA": [
                {"url": "https://onenutrition.cl/tienda/aminoacidos", "subcategory": "Otros Aminoacidos y BCAA"},
            ],
            "Perdida de Grasa": [
                {"url": "https://onenutrition.cl/tienda/dieta-quemadores", "subcategory": "Dieta Quemadores"}
            ],
            "Snacks y Comida": [
                {"url": "https://onenutrition.cl/tienda/barras-snack", "subcategory": "Barras Snack"}
            ]
        }
        
        selectors = {
            "product_grid": "#js-product-list", 
            'product_card': '#js-product-list .product-miniature', 
            'product_name': '.product-title a',
            'brand': '.product-title a', 
            'price_container': '.product-price-and-shipping', 
            'price_current': '.price', 
            'price_regular': '.regular-price', 
            'link': '.product-title a', 
            'next_button': '.pagination .next',
            'thumbnail': 'a.thumbnail.product-thumbnail img'
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="OneNutrition")

    def _classify_product(self, title, description, main_category, deterministic_subcategory, brand):
        """
        Aplica heurísticas para determinar la categoría y subcategoría final.
        Adapta lógica de StrongestScraper pero focalizada en 'CATEGORIZAR_PROTEINA'.
        """
        final_category = main_category
        final_subcategory = deterministic_subcategory
        
        def _normalize(text):
            nfd = unicodedata.normalize('NFD', text)
            return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')

        # 1. Packs (Global Check - Good practice to have)
        title_lower = _normalize(title.lower())

        if "pack" in title_lower or "paquete" in title_lower or "combo" in title_lower or "+2" in title_lower:
            final_category = "Packs"
            final_subcategory = "Packs"
            return final_category, final_subcategory

        # 2. Heurística para Proteínas
        if final_category == "Proteinas":
            # Usamos Título + Descripción
            text_to_search = _normalize((title + " " + (description or "")).lower())

            # Estructura idéntica a StrongestScraper (con soporte para Dymatize y Purity Rule sanitizada)
            
            # Logica para productos Dymatize
            if brand.lower() == "dymatize":
                if "iso" in text_to_search or "isolate" in text_to_search or "aislada" in text_to_search or "isolated" in text_to_search or "isofit" in text_to_search:
                    final_subcategory = "Proteína Aislada"
                elif "hydro" in text_to_search or "hidrolizada" in text_to_search or "hydrolized" in text_to_search or "hydrolyzed" in text_to_search or "hidrolizado" in text_to_search:
                    final_subcategory = "Proteína Hidrolizada"
                else:
                    final_subcategory = "Proteína de Whey"

            # Si la marca no era Dymatize usamos para los genericos
            else:
                if "vegan" in text_to_search or "plant" in text_to_search or "vegana" in text_to_search or "vegano" in text_to_search or "plant based" in text_to_search:
                    final_subcategory = "Proteína Vegana"
                elif "beef" in text_to_search or "carne" in text_to_search or "vacuno" in text_to_search:
                    final_subcategory = "Proteína de Carne"
                elif "casein" in text_to_search or "caseina" in text_to_search or "micelar" in text_to_search or "micellar" in text_to_search:
                    final_subcategory = "Caseína"
                
                else:
                    # Regla de Pureza: Si tiene concentrado o blend, es Whey Estándar (aunque diga Isolate/Hydro)
                    # EXCEPCIONES: Limpiamos frases benignas
                    purity_check_text = text_to_search
                    benign_phrases = [
                        "se mezcla", "fácil mezcla", "facil mezcla", "mezclabilidad", "mezcla instantánea", "mezcla instantanea",
                        "combinación perfecta", "perfecta combinación", "excelente combinación",
                        "combinacion perfecta", "perfecta combinacion", "excelente combinacion",
                        "mezclar", "mezclado", "mezclando",
                        "mezcla 1 scoop", "mezcla un scoop", "mezcla 1 porción", "mezcla una porción", "mezcla 1 serv", "mezcla un serv",
                        "mezcla el polvo", "mezcla con agua", "mezcla con leche"
                    ]
                    for phrase in benign_phrases:
                        purity_check_text = purity_check_text.replace(phrase, "")

                    if "concentrado" in purity_check_text or "combinación" in purity_check_text or "combinacion" in purity_check_text or "concentrate" in purity_check_text or "blend" in purity_check_text or "mezcla" in purity_check_text:
                        final_subcategory = "Proteína de Whey"
                    elif re.search(r'\biso\b', text_to_search) or "isolate" in text_to_search or "aislada" in text_to_search or "isolated" in text_to_search or "isofit" in text_to_search or "isolatada" in text_to_search:
                        final_subcategory = "Proteína Aislada"
                    elif "hydro" in text_to_search or "hidrolizada" in text_to_search or "hydrolized" in text_to_search or "hydrolyzed" in text_to_search or "hidrolizado" in text_to_search:
                        final_subcategory = "Proteína Hidrolizada"
                    else:
                        final_subcategory = "Proteína de Whey"

            return final_category, final_subcategory


        # 3. Heurística para Creatinas
        elif final_category == "Creatinas":
            text_to_search = _normalize((title + " " + (description or "")).lower())
            
            if "hcl" in text_to_search or "clorhidrato" in text_to_search or "hydrochloride" in text_to_search or "hidrocloruro" in text_to_search:
                final_subcategory = "Creatina HCL"
            elif "malato" in text_to_search or "magnesio" in text_to_search or "magnapower" in text_to_search:
                final_subcategory = "Malato y Magnesio"
            elif "nitrato" in text_to_search or "nitrate" in text_to_search:
                final_subcategory = "Nitrato"
            elif "alkalyn" in text_to_search or "alcalina" in text_to_search:
                final_subcategory = "Otros Creatinas"
            elif "creapure" in text_to_search:
                 final_subcategory = "Sello Creapure"
            elif "monohidrat" in text_to_search or "monohydrate" in text_to_search:
                final_subcategory = "Creatina Monohidrato"
            elif "micronizad" in text_to_search or "micronized" in text_to_search:
                final_subcategory = "Micronizada"
            else:
                final_subcategory = "Otros Creatinas"

            return final_category, final_subcategory

        # 4. Heurística para Vitaminas y Minerales
        elif final_category == "Vitaminas y Minerales":
            text_to_search = _normalize(title.lower())

            # Keywords mapping
            # Order matters: Specific blends > Major Minerals > Major Vitamins > Formats
            
            multi_keywords = ["multivitamin", "multi vitamin", "multivitaminico", "multivitaminicos", "daily pack", "animal pak", "opti-men", "opti-women", "vita stack", "zmar"]
            magnesio_keywords = ["magne", "magnesio", "magnesium", "magnesio d3"]
            zinc_keywords = ["zinc"]
            omega_keywords = ["omega", "fish oil", "krill", "cla", "linoleic", "linoleico", "aceite de", "aceite de pescado"]
            colageno_keywords = ["colageno", "collagen"]
            calcio_keywords = ["calcio", "calcium"]
            probioticos_keywords = ["probiotic", "probiotico", "enzym", "enzim", "digest"]
            complejob_keywords = ["b-complex", "complejo b", "vitamin b", "vitamina b", "b12", "b6", "biotin", "biotina"]
            vitc_keywords = ["vitamin c", "vitamina c", "ascorbic", "ascorbico"]
            vitd_keywords = ["vitamin d", "vitamina d", "d3"]
            vite_keywords = ["vitamin e", "vitamina e"]
            antiox_keywords = ["coq10", "q10", "antioxidant", "antioxidante", "resveratrol", "ala ", "acido alfa", "alpha lipoic", "alfa lipoico", "turmeric", "curcuma", "astaxanthin", "astaxantina", "semilla de uva", "grape seed"]
            bienestar_keywords = ["wellness", "bienestar", "sleep", "dormir", "descanso", "relax", "relajante", "stress", "estres", "liver", "higado", "hepato", "joint", "articulacion", "articulaciones", "soporte", "huesos", "melatonin", "melatonina", "5-htp", "ashwagandha", "maca", "tryptophan", "triptofano"]
            gummies_keywords = ["gummi", "gummy", "gomita","gomitas","gummies"]

            if any(k in text_to_search for k in multi_keywords):
                 final_subcategory = "Multivitamínicos"
            elif any(k in text_to_search for k in magnesio_keywords):
                 final_subcategory = "Magnesio"
            elif any(k in text_to_search for k in zinc_keywords):
                 final_subcategory = "Otros Vitaminas y Minerales"
            elif any(k in text_to_search for k in omega_keywords):
                 final_subcategory = "Omega 3 y Aceites"
            elif any(k in text_to_search for k in colageno_keywords):
                 final_subcategory = "Colágeno"
            elif any(k in text_to_search for k in calcio_keywords):
                 final_subcategory = "Calcio"
            elif any(k in text_to_search for k in probioticos_keywords):
                 final_subcategory = "Probióticos"
            elif any(k in text_to_search for k in complejob_keywords):
                 final_subcategory = "Vitamina B / Complejo B"
            elif any(k in text_to_search for k in vitc_keywords):
                 final_subcategory = "Vitamina C"
            elif any(k in text_to_search for k in vitd_keywords):
                 final_subcategory = "Vitamina D"
            elif any(k in text_to_search for k in vite_keywords):
                 final_subcategory = "Vitamina E"
            elif any(k in text_to_search for k in antiox_keywords):
                 final_subcategory = "Antioxidantes"
            elif any(k in text_to_search for k in bienestar_keywords):
                 final_subcategory = "Bienestar General"
            elif any(k in text_to_search for k in gummies_keywords):
                 final_subcategory = "Gummies"
            else:
                 final_subcategory = "Otros Vitaminas y Minerales"

            return final_category, final_subcategory

        # 5. Heurística para Aminoácidos y BCAA
        elif final_category == "Aminoacidos y BCAA":
            text_to_search = _normalize(title.lower())  # Solo titulo, sin tildes
            
            zma_keywords = ["zma", "zmar"]
            minerales_keywords = ["magnesio", "zinc", "magne"]
            eaas_keywords = ["eaa", "essential amino", "aminoacidos esenciales", "esenciales", "neaa", "full spectrum", "espectro completo"]
            bcaa_keywords = ["bcaa", "branched", "ramificados"]
            glutamina_keywords = ["glutamin"]
            leucina_keywords = ["leucin", "leucine"]
            aminos_keywords = ["amino", "arginin", "citrulin", "beta ala", "beta alanina","taurin", "carnitin", "tirosin", "tyrosin", "lisin", "lysin", "triptop", "metionin", "methionin", "histidin", "treonin", "threonin", "fenilalan", "phenylalan"]

            if any(k in text_to_search for k in zma_keywords):
                 final_category = "Vitaminas y Minerales"
                 final_subcategory = "Multivitamínicos"
            elif any(k in text_to_search for k in minerales_keywords):
                 final_subcategory = "Minerales (Magnesio/ZMA)"
            elif any(k in text_to_search for k in eaas_keywords):
                 final_subcategory = "EAAs (Esenciales)"
            elif any(k in text_to_search for k in bcaa_keywords):
                 final_subcategory = "BCAAs"
            elif any(k in text_to_search for k in glutamina_keywords):
                 final_subcategory = "Glutamina"
            elif any(k in text_to_search for k in leucina_keywords):
                 final_subcategory = "Leucina"
            elif any(k in text_to_search for k in aminos_keywords):
                 final_subcategory = "Aminoácidos"
            else:
                 final_subcategory = "Otros Aminoacidos y BCAA"

            return final_category, final_subcategory

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías principales en OneNutrition...[/green]")
        
        context = page.context

        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                deterministic_subcategory = item['subcategory']
                
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {deterministic_subcategory} ({url})")

                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")
                        try:
                            page.wait_for_selector(self.selectors['product_grid'], timeout=10000)
                        except:
                            print(f"[red]No se encontraron productos en la grilla principal (#js-product-list) de {url}.[/red]")
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
                                    link = href 
                                    if not link.startswith('http'):
                                         link = self.base_url + link

                            # Deduplication Check
                            if link != "N/D" and link in self.seen_urls:
                                print(f"[yellow]  >> Producto duplicado omitido: {title}[/yellow]")
                                continue
                            if link != "N/D":
                                self.seen_urls.add(link)

                            # Title
                            title = "N/D"
                            if producto.locator(self.selectors['product_name']).count() > 0:
                                raw_title = producto.locator(self.selectors['product_name']).first.inner_text()
                                title = self.clean_text(raw_title)
                            
                            # Brand (inferred)
                            brand = "N/D"

                            
                            # Thumbnail
                            thumbnail_url = ""
                            if producto.locator(self.selectors['thumbnail']).count() > 0:
                                t_src = producto.locator(self.selectors['thumbnail']).first.get_attribute("src")
                                if t_src:
                                    thumbnail_url = t_src
                                    
                            # Price
                            price = 0
                            active_discount = False
                            
                            price_container = producto.locator(self.selectors['price_container'])
                            if price_container.count() > 0:
                                regular = price_container.locator(self.selectors['price_regular'])
                                if regular.count() > 0:
                                    active_discount = True
                                    current = price_container.locator(self.selectors['price_current'])
                                    price_text = current.first.inner_text() if current.count() > 0 else "0"
                                else:
                                    current = price_container.locator(self.selectors['price_current'])
                                    price_text = current.first.inner_text() if current.count() > 0 else "0"
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
                                    
                                    # 1. Main Image (HD)
                                    img_el = detail_page.locator('.product-cover img').first
                                    if img_el.count() > 0:
                                        src = img_el.get_attribute("src")
                                        if src:
                                            image_url = src
                                            
                                    # 2. SKU
                                    sku_el = detail_page.locator("span[itemprop='sku'], meta[itemprop='sku']").first
                                    if sku_el.count() > 0:
                                        if sku_el.get_attribute("content"):
                                             sku = sku_el.get_attribute("content")
                                        else:
                                             sku = sku_el.inner_text().strip()
                                    
                                    # 3. Description
                                    try:
                                        detail_page.wait_for_selector('#description', timeout=5000)
                                    except:
                                        pass 

                                    # Prioritize specific class strictly inside #description to avoid carousels/reviews
                                    desc_el = detail_page.locator('#description .product-description').first
                                    if desc_el.count() == 0:
                                         desc_el = detail_page.locator('#description').first
                                         
                                    if desc_el.count() > 0:
                                        description = desc_el.inner_text().strip()
                                        
                                    detail_page.close()
                                    
                                except Exception as e:
                                    print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass

                            # New Categorization Logic
                            # Use Heuristic specifically for "CATEGORIZAR_PROTEINA" as requested
                            final_category, final_subcategory = self._classify_product(title, description, main_category, deterministic_subcategory, brand)

                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(final_category),
                                'subcategory': final_subcategory,
                                'product_name': title,
                                'brand': self.enrich_brand(self.clean_text(brand), title),
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
                                page.goto(href)
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
    base_url = "https://onenutrition.cl"
    scraper = OneNutritionScraper(base_url=base_url, headless=True)
    scraper.run()