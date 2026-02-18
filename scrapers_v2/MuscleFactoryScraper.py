from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import json
import unicodedata

class MuscleFactoryScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        category_urls = {
            "Proteinas": [
                {"url": "https://www.musclefactory.cl/proteinas", "subcategory": "Proteinas"}
            ],
            "Creatinas": [
                {"url": "https://www.musclefactory.cl/productos/creatina", "subcategory": "Creatina"}
            ],
            "Pre Entrenos": [
                {"url": "https://www.musclefactory.cl/productos/pre-entrenamientos", "subcategory": "Pre Entreno"}
            ],
            "Vitaminas y Minerales": [
                {"url": "https://www.musclefactory.cl/vitaminas-y-minerales-🌞", "subcategory": "Vitaminas y Minerales"}
            ],
            "Ofertas": [
                {"url": "https://www.musclefactory.cl/ofertas", "subcategory": "Ofertas"}
            ],
            "Packs": [
                {"url": "https://www.musclefactory.cl/packs", "subcategory": "Packs"}
            ]
        }

        # Selectores Jumpseller
        selectors = {
            "product_card": ".product-block",
            "product_name": ".product-block__name",
            "price_container": ".product-block__price",
            "price_new": ".product-block__price--new", # Final price (with discount)
            "price_old": ".product-block__price--old", # Old price (crossed out)
            "link": ".product-block__anchor",
            "image": ".product-block__image img",
            
            # Detail page
            "detail_sku_json": ".product-json", # Jumpseller standard JSON data
            "detail_desc": ".product-page__body",
            "detail_image": ".product-gallery__image img",
            "detail_brand": ".product-page__brand",
            "detail_title": "h1.product-page__title",
            
            # Pagination
            "next_button": ".pager .next, .pager a:has-text('»')" 
        }

        super().__init__(base_url, headless, category_urls, selectors, site_name="MuscleFactory")
        self.seen_urls = set()

    def _classify_product(self, title, description, main_category, deterministic_subcategory, brand):
        """
        Aplica heurísticas para determinar la categoría y subcategoría final.
        Adapta lógica de StrongestScraper pero focalizada en 'CATEGORIZAR_PROTEINA'.
        """
        final_category = main_category
        final_subcategory = deterministic_subcategory
        
        def _normalize(text):
            """Quita tildes para comparación insensible a acentos."""
            nfd = unicodedata.normalize('NFD', text)
            return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')

        # 1. Packs (Global Check - Good practice to have)
        title_lower = _normalize(title.lower())

        if "pack" in title_lower or "paquete" in title_lower or "combo" in title_lower or "+2" in title_lower or " + " in title_lower:
            final_category = "Packs"
            final_subcategory = "Packs"
            return final_category, final_subcategory

        # 2. Heurística para Proteínas
        if final_category == "Proteinas":
            # Usamos Título + Descripción
            text_to_search = _normalize((title + " " + (description or "")).lower())

            # Heurística para Snacks y Comida dentro de Proteínas
            # Usamos regex con word boundary (\b) para evitar falsos positivos como "bar" en "bariatrix" o "iso" en palabras largas
            
            if "shark up" in text_to_search or "gel" in text_to_search or "isotonic" in text_to_search:
                final_category = "Snacks y Comida"
                final_subcategory = "Otros Snacks y Comida"
                return final_category, final_subcategory
            
            # Check especifico para Bariatrix que dice "bariatrix" y no es barra
            elif "bariatrix" in title_lower:
                 pass # Evitar entrar en el check de barritas
            elif re.search(r'\bbar\b', text_to_search) or re.search(r'\bbarra\b', text_to_search) or "bites" in text_to_search or "whey bar" in text_to_search or "barrita" in text_to_search:
                final_category = "Snacks y Comida"
                final_subcategory = "Barritas Y Snacks Proteicas"
                return final_category, final_subcategory
            elif "alfajor" in text_to_search:
                final_category = "Snacks y Comida"
                final_subcategory = "Snacks Dulces"
                return final_category, final_subcategory

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
                # Caso extremadamente específico: Revitta Femme -> Isolate (aunque no lo diga explícitamente)
                if "revitta" in brand.lower() and "femme" in text_to_search:
                     final_subcategory = "Proteína Aislada"
                
                # Caso extremadamente específico: Winkler Cooking Protein -> Whey (evitar Isolate si se coló)
                elif "cooking" in text_to_search and "winkler" in text_to_search:
                     final_subcategory = "Proteína de Whey"

                elif "vegan" in text_to_search or "plant" in text_to_search or "vegana" in text_to_search or "vegano" in text_to_search or "plant based" in text_to_search:
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

                    if "concentrado" in purity_check_text or "combinación" in purity_check_text or "combinacion" in purity_check_text or "concentrate" in purity_check_text or "blend" in purity_check_text or "mezcla" in purity_check_text or "wpc" in text_to_search:
                        final_subcategory = "Proteína de Whey"
                    
                    # Usar word boundary para 'iso' para evitar falsos positivos
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

            # Caso específico: Greatlhete Crea Pro -> Monohidrato
            if "greatlhete" in text_to_search and "crea pro" in text_to_search:
                final_subcategory = "Creatina Monohidrato"
                return final_category, final_subcategory
            
            if "hcl" in text_to_search or "clorhidrato" in text_to_search or "hydrochloride" in text_to_search or "hidrocloruro" in text_to_search:
                final_subcategory = "Clorhidrato"
            elif "malato" in text_to_search or "magnesio" in text_to_search or "magnapower" in text_to_search:
                final_subcategory = "Malato y Magnesio"
            elif "nitrato" in text_to_search or "nitrate" in text_to_search:
                final_subcategory = "Nitrato"
            elif "alkalyn" in text_to_search or "alcalina" in text_to_search:
                final_subcategory = "Otros Creatinas"
            elif "monohidrat" in text_to_search or "monohydrate" in text_to_search or "creapure" in text_to_search:
                final_subcategory = "Creatina Monohidrato"
            elif "micronizad" in text_to_search or "micronized" in text_to_search:
                final_subcategory = "Micronizada"
            else:
                final_subcategory = "Otros Creatinas"

            return final_category, final_subcategory

        # 4. Heurística para Vitaminas y Minerales
        elif final_category == "Vitaminas y Minerales":
            raw_text = (title + " " + (description or "")).lower()
            text_to_search = _normalize(raw_text)

            # Keywords mapping
            # Order matters: Specific blends > Major Minerals > Major Vitamins > Formats
            
            multi_keywords = ["multivitamin", "multi vitamin", "multivitaminico", "multivitaminicos", "daily pack", "animal pak", "opti-men", "opti-women", "vita stack", "zmar"]
            magnesio_keywords = ["magne", "magnesio", "magnesium", "magnesio d3"]
            zinc_keywords = ["zinc"]
            omega_keywords = ["omega", "fish oil", "krill", "cla", "linoleic", "linoleico", "aceite de", "aceite de pescado"]
            colageno_keywords = ["colageno", "collagen"]
            calcio_keywords = ["calcio", "calcium"]
            probioticos_keywords = ["probiotic", "probiotico", "enzym", "enzim", "digest"]
            complejob_keywords = ["b-complex", "complejo b", "vitamin b", "vitamina b", "b12", "b6", "biotin", "biotina", "vitb", "vitta-b"]
            vitc_keywords = ["vitamin c", "vitamina c", "ascorbic", "ascorbico", "vitc", "vitta-c"]
            vitd_keywords = ["vitamin d", "vitamina d", "d3", "vitd", "vitta-d"]
            vite_keywords = ["vitamin e", "vitamina e", "vite", "vitta-e"]
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
            aminos_keywords = ["amino", "arginin", "citrulin", "beta ala","beta alanina",  "taurin", "carnitin", "tirosin", "tyrosin", "lisin", "lysin", "triptop", "metionin", "methionin", "histidin", "treonin", "threonin", "fenilalan", "phenylalan"]

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
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en MuscleFactory...[/green]")
        context = page.context

        for main_category, items in self.category_urls.items():
            batch_buffer = []
            for item in items:
                url = item['url']
                deterministic_subcategory = item['subcategory']
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} ({url})")
                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    
                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")
                        try:
                            # Sometimes product-block might load a bit slowly
                            page.wait_for_selector(self.selectors['product_card'], timeout=6000)
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
                            if link in self.seen_urls:
                                print(f"[yellow]  >> Producto duplicado omitido (encontrado previamente): {title}[/yellow]")
                                continue
                            self.seen_urls.add(link)

                            # Price extraction
                            price = 0
                            active_discount = False
                            price_text = "0"

                            # Logic: price--new is usually the final price. price--old exists if there is a discount.
                            # If only standard price exists, it might be in price--new or just generic price container logic.
                            
                            new_price_el = card.locator(self.selectors['price_new']).first
                            old_price_el = card.locator(self.selectors['price_old']).first
                            
                            if new_price_el.count() > 0:
                                price_text = new_price_el.inner_text()
                                if old_price_el.count() > 0:
                                    active_discount = True
                            elif card.locator(self.selectors['price_container']).count() > 0:
                                # Fallback if specific classes aren't used
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
                            brand = "N/D" 

                            if link != "N/D":
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                                    
                                    # SKU Strategy: Try JSON first (Jumpseller standard), then DOM
                                    # 1. JSON
                                    try:
                                        json_el = detail_page.locator(self.selectors['detail_sku_json']).first
                                        if json_el.count() > 0:
                                            json_data = json.loads(json_el.inner_text())
                                            if isinstance(json_data, list) and len(json_data) > 0:
                                                sku = json_data[0].get('variant', {}).get('sku', '')
                                    except:
                                        pass
                                    
                                    # 2. DOM fallback
                                    if not sku:
                                        # Often SKU is not easily selector-able in DOM on some templates, 
                                        # but let's try a common pattern if needed
                                        pass

                                    # Description
                                    if detail_page.locator(self.selectors['detail_desc']).count() > 0:
                                        description = detail_page.locator(self.selectors['detail_desc']).first.inner_text().strip()

                                    # Brand 
                                    if detail_page.locator(self.selectors['detail_brand']).count() > 0:
                                        raw_brand = detail_page.locator(self.selectors['detail_brand']).first.inner_text()
                                        brand = self.clean_text(raw_brand)
                                    
                                    # Main Image - Priority: Open Graph > JSON-LD > DOM

                                    
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
                                        if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                            img_src = detail_page.locator(self.selectors['detail_image']).first.get_attribute("src")
                                            if img_src:
                                                image_url = "https:" + img_src if img_src.startswith('//') else img_src
                                    
                                    if not image_url and thumbnail_url:
                                        image_url = thumbnail_url.replace('240x240', '1024x1024') 

                                    detail_page.close()

                                except Exception as e:
                                    print(f"[yellow]Error consiguiendo detalles de {link}: {e}[/yellow]")
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

                            # New Categorization Logic
                            final_category, final_subcategory = self._classify_product(title, description, main_category, deterministic_subcategory, brand)

                            product_obj = {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': self.clean_text(final_category),
                                'subcategory': final_subcategory, 
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

                            yield product_obj
                        
                        # Pagination Logic
                        # Jumpseller usually has a 'next' class or we look for text
                        next_btn = page.locator(".pager a").filter(has_text="»").first
                        if next_btn.count() == 0:
                                next_btn = page.locator(".next").first

                        if next_btn.count() > 0 and next_btn.is_visible():
                            href_next = next_btn.get_attribute("href")
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
            
            # End of category loop - No buffer needed

if __name__ == "__main__":
    scraper = MuscleFactoryScraper("https://www.musclefactory.cl", headless=True)
    scraper.run()
