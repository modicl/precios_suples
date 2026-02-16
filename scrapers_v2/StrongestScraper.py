# Scraper para la pagina web Strongest.cl
from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class StrongestScraper(BaseScraper):
    def __init__(self):
        super().__init__(
            base_url="https://www.strongest.cl",
            site_name="Strongest",
            headless=True
        )
        
        # URLs Categorias - Subcategorias
        self.category_urls = {
            "Proteinas": [{"url": "https://www.strongest.cl/collection/proteinas", "subcategory": "Proteinas"}],
            "Creatinas": [{"url": "https://www.strongest.cl/collection/creatinas", "subcategory": "Creatinas"}],
            "Vitaminas y Minerales": [{"url": "https://www.strongest.cl/collection/salud-y-bienestar", "subcategory": "Bienestar General"}],
            "Pre Entrenos": [{"url": "https://www.strongest.cl/collection/pre-entrenos", "subcategory": "Pre Entrenos"}],
            "Ganadores de Peso": [{"url": "https://www.strongest.cl/collection/ganadores-de-masa", "subcategory": "Ganadores De Peso"}],
            "Aminoacidos y BCAA": [{"url": "https://www.strongest.cl/collection/aminoacidos-bcaa", "subcategory": "Otros Aminoacidos y BCAA"}],
            "Perdida de Grasa": [{"url": "https://www.strongest.cl/collection/termogenicos", "subcategory": "Quemadores Termogenicos"}],
            "Snacks y Comida": [{"url": "https://www.strongest.cl/collection/snacks", "subcategory": "Otros Snacks y Comida"}]
        }
        
        self.selectors = {
            "product_card": ".bs-collection__product",
            "product_name": ".bs-collection__product-title",
            "price": ".bs-collection__product-final-price", 
            "old_price": ".bs-collection__old-price", 
            "link": "a.bs-collection__product-info",
            "thumbnail": ".bs-collection__product__img img, .bs-collection__product-image img",
            "brand": ".bs-collection__product-brand"
        }

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en Strongest...[/green]")
        context = page.context

        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                deterministic_subcategory = item['subcategory']
            
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {deterministic_subcategory} ({url})")
            
            try:
                page.goto(url, timeout=60000, wait_until='domcontentloaded')
                
                page_num = 1
                while True:
                    print(f"--- Página {page_num} ---")
                    
                    try:
                        page.wait_for_selector(self.selectors['product_card'], timeout=5000)
                    except:
                        print(f"[red]No se encontraron productos en la página {page_num} o fin de lista.[/red]")
                        break
                    
                    cards = page.locator(self.selectors['product_card'])
                    count = cards.count()
                    
                    if count == 0:
                        print("  Grilla vacía.")
                        break
                        
                    print(f"  > Encontrados {count} productos.")
                    
                    for i in range(count):
                        card = cards.nth(i)
                        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        # --- Grid Extraction ---
                        
                        # Title
                        title = "N/D"
                        if card.locator(self.selectors['product_name']).count() > 0:
                            raw_title = card.locator(self.selectors['product_name']).first.inner_text()
                            title = self.clean_text(raw_title)
                        
                        # Link

                        link = "N/D"
                        if card.locator(self.selectors['link']).count() > 0:
                            href = card.locator(self.selectors['link']).first.get_attribute("href")
                            if href:
                                link = href if href.startswith('http') else f"https://www.strongest.cl{href}"
                        
                        # Thumbnail
                        thumbnail_url = ""
                        if card.locator(self.selectors['thumbnail']).count() > 0:
                            t_src = card.locator(self.selectors['thumbnail']).first.get_attribute("src")
                            if t_src:
                                thumbnail_url = t_src if t_src.startswith('http') else f"https:{t_src}"

                        # Price
                        price = 0
                        price_elem = card.locator(self.selectors['price'])
                        if price_elem.count() > 0:
                            price_text = price_elem.first.inner_text()
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)
                        
                        # Active Discount
                        active_discount = False
                        if card.locator(self.selectors['old_price']).count() > 0:
                            active_discount = True

                        # Brand Extraction
                        brand = "N/D"
                        if card.locator(self.selectors['brand']).count() > 0:
                            raw_brand = card.locator(self.selectors['brand']).first.inner_text()
                            brand = self.clean_text(raw_brand)
                        
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
                                    img_el = detail_page.locator('.bs-product-gallery__img img, .bs-product__image img, .bs-img-square img').first
                                    if img_el.count() > 0:
                                        src = img_el.get_attribute("src")
                                        if src:
                                            image_url = src if src.startswith('http') else f"https:{src}"
                                
                                # 2. SKU
                                sku_el = detail_page.locator('.bs-product__sku').first
                                if sku_el.count() > 0:
                                    sku_raw = sku_el.inner_text().strip()
                                    sku = sku_raw.replace("SKU:", "").strip()
                                
                                # 3. Description
                                # Try to wait for description, it might be lazy loaded
                                try:
                                    detail_page.wait_for_selector('.bs-product-description, .product-description, #description', timeout=2000)
                                except: pass

                                desc_el = detail_page.locator('.bs-product-description, .product-description, #description').first
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

                        # --- HEURÍSTICAS DE CLASIFICACIÓN (Refactored) ---
                        final_category, final_subcategory = self._classify_product(title, description, main_category, deterministic_subcategory)

                        yield {
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

                    # Pagination
                    # Strongest uses ?page=N
                    # We can click next if exists or just navigate URL
                    
                    next_btn = page.locator('a[aria-label="Next page"], .pagination .next, a:has-text("Siguiente")')
                    # Or simpler: check if we just processed less than expected or check button
                    
                    # Safer: Construct next URL or look for next button href
                    # The original scraper used URL logic, let's try to stick to button if possible or URL
                    
                    # Try to find next button href
                    next_href = None
                    if next_btn.count() > 0:
                         href = next_btn.first.get_attribute("href")
                         if href: next_href = href
                    
                    if next_href:
                        print(f"  > Avanzando a página {page_num + 1}...")
                        page.goto(self.base_url + next_href if next_href.startswith('/') else next_href)
                        page_num += 1
                        page.wait_for_timeout(2000)
                    else:
                        # Fallback to URL increment if no button found but maybe it's infinite scroll or js?
                        # Strongest is usually paginated. If no next button, we assume end.
                        print("  > No hay más páginas (o botón no encontrado).")
                        break

            except Exception as e:
                print(f"[red]Error procesando categoría {main_category}: {e}[/red]")
            except Exception as e:
                print(f"[red]Error procesando categoría {main_category}: {e}[/red]")

    def _classify_product(self, title, description, main_category, deterministic_subcategory):
        """
        Aplica heurísticas para determinar la categoría y subcategoría final.
        """
        final_category = main_category
        final_subcategory = deterministic_subcategory
        
        # 1. Packs (Global)
        title_lower = title.lower()

        # 0. Llaveros / Accesorios (Filtro Usuario)
        if "llavero" in title_lower:
            final_category = "OTROS"
            final_subcategory = "Otros"

        elif "pack" in title_lower or "paquete" in title_lower or "combo" in title_lower:
            final_category = "Packs"
            final_subcategory = "Packs"

        # 1.5 Bebidas / RTD (Global)
        is_liquid = "ml" in title_lower or "lt" in title_lower or "cc" in title_lower or "botella" in title_lower
        is_powder_weight = "lb" in title_lower or "kg" in title_lower or "gr" in title_lower or "servicios" in title_lower
        
        if ("rtd" in title_lower or "ready to drink" in title_lower):
            final_category = "Bebidas Nutricionales"
            final_subcategory = "Batidos de proteína"
            
        elif ("shake" in title_lower and "shaker" not in title_lower) or \
                "batido" in title_lower or \
                "bebida" in title_lower or \
                "hydration" in title_lower:
                
                # Si es un "Batido/Shake" pero tiene peso de polvo (2lb, 5kg), LO IGNORAMOS (es proteina en polvo)
                if not is_powder_weight:
                    final_category = "Bebidas Nutricionales"
                    final_subcategory = "Batidos de proteína"
                elif is_liquid:
                    # Caso raro: Dice 2lb pero tambien 500ml? Priorizamos liquido si tiene ml explícito
                    final_category = "Bebidas Nutricionales"
                    final_subcategory = "Batidos de proteína"

        # 2. Heurística para Proteínas
        elif final_category == "Proteinas":
            # Usamos Título + Descripción
            text_to_search = (title + " " + (description or "")).lower()
            
            if "vegan" in text_to_search or "plant" in text_to_search or "vegana" in text_to_search or "vegano" in text_to_search or "plant based" in text_to_search:
                final_subcategory = "Proteína Vegana"
            elif "beef" in text_to_search or "carne" in text_to_search or "vacuno" in text_to_search:
                final_subcategory = "Proteína de Carne"
            elif "casein" in text_to_search or "caseina" in text_to_search or "micelar" in text_to_search or "micellar" in text_to_search:
                final_subcategory = "Caseína"
            
            # Regla de Pureza: Si tiene concentrado o blend, es Whey Estándar (aunque diga Isolate/Hydro)
            # EXCEPCIONES: Limpiamos frases benignas donde "mezcla" o "combinación" no se refieren a ingredientes
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
            elif "iso" in text_to_search or "isolate" in text_to_search or "aislada" in text_to_search or "isolated" in text_to_search or "isofit" in text_to_search:
                final_subcategory = "Proteína Aislada"
            elif "hydro" in text_to_search or "hidrolizada" in text_to_search or "hydrolized" in text_to_search or "hydrolyzed" in text_to_search or "hidrolizado" in text_to_search:
                final_subcategory = "Proteína Hidrolizada"
            else:
                final_subcategory = "Proteína de Whey"

        # 3. Heurística para Creatinas
        elif final_category == "Creatinas":
            text_to_search = (title + " " + (description or "")).lower()

            if "monohidrat" in text_to_search or "monohydrate" in text_to_search or "creapure" in text_to_search:
                final_subcategory = "Creatina Monohidrato"
            elif "hcl" in text_to_search or "clorhidrato" in text_to_search or "hydrochloride" in text_to_search or "hidrocloruro" in text_to_search:
                final_subcategory = "Clorhidrato"
            elif "malato" in text_to_search or "magnesio" in text_to_search or "magnapower" in text_to_search:
                final_subcategory = "Malato y Magnesio"
            elif "nitrato" in text_to_search or "nitrate" in text_to_search:
                final_subcategory = "Nitrato"
            elif "alkalyn" in text_to_search or "alcalina" in text_to_search:
                final_subcategory = "Otros Creatinas"
            
            elif "micronizad" in text_to_search or "micronized" in text_to_search:
                final_subcategory = "Micronizada"
            else:
                final_subcategory = "Otros Creatinas"

        # 4. Heurística para Bienestar General / Vitaminas
        elif final_category == "Vitaminas y Minerales":
            text_to_search = (title + " " + (description or "")).lower()

            if "collagen" in text_to_search or "colageno" in text_to_search or "colágeno" in text_to_search: # Es mas probable que colageno este solo que la vitamina c
                final_subcategory = "Colágeno"
            elif "vitamin c" in text_to_search or "vitamina c" in text_to_search: 
                final_subcategory = "Vitamina C"
            else:
                final_subcategory = "Bienestar General"

        # 5. Heurística para Aminoacidos y BCAA
        elif final_category == "Aminoacidos y BCAA":
            text_to_search = (title + " " + (description or "")).lower()
            if "bcaa" in text_to_search:
                final_subcategory = "BCAAs"
            else:
                final_subcategory = "Otros Aminoacidos y BCAA"

        # 6. Heurística para Snacks y Comida
        elif final_category == "Snacks y Comida":
            text_to_search = (title + " " + (description or "")).lower()
            if "shark up" in text_to_search or "gel" in text_to_search or "isotonic" in text_to_search:
                final_subcategory = "Otros Snacks y Comida"
            elif "bar" in text_to_search or "bites" in text_to_search or "whey bar" in text_to_search or "barra" in text_to_search:
                final_subcategory = "Barritas Y Snacks Proteicas"
            elif "alfajor" in text_to_search:
                final_subcategory = "Snacks Dulces"
        
        return final_category, final_subcategory



if __name__ == "__main__":
    scraper = StrongestScraper()
    scraper.run()