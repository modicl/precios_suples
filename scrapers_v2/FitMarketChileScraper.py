from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier, normalize
from rich import print
from datetime import datetime
import re
import csv
import os
import unicodedata

class FitMarketChileScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        category_urls = {
            "Proteinas": [
                {"url": "https://fitmarketchile.cl/categoria-producto/proteinas", "subcategory": "Proteinas"}
            ],
            "Creatinas": [
                {"url": "https://fitmarketchile.cl/categoria-producto/creatina", "subcategory": "Creatina"}
            ],
            "Aminoacidos y BCAA": [
                {"url": "https://fitmarketchile.cl/categoria-producto/aminoacidos-bcaa", "subcategory": "Aminoacidos Bcaa"}
            ],
            "Perdida de Grasa": [
                {"url": "https://fitmarketchile.cl/categoria-producto/quemador-de-grasa", "subcategory": "Quemadores"}
            ],
            "Pre Entrenos": [
                {"url": "https://fitmarketchile.cl/categoria-producto/pre-entrenos", "subcategory": "Pre Entrenos"}
            ],
            "Ganadores de Peso": [
                {"url": "https://fitmarketchile.cl/categoria-producto/ganador-de-masa", "subcategory": "Ganadores De Peso"}
            ],
            "Vitaminas y Minerales": [
                {"url": "https://fitmarketchile.cl/categoria-producto/vitaminas", "subcategory": "Vitaminas"}
            ],           
            "Snacks y Comida": [
                {"url": "https://fitmarketchile.cl/categoria-producto/barras-de-proteina-snack", "subcategory": "Barras De Proteina Snack"}
            ]
        }

        # Selectores WooCommerce / WoodMart
        selectors = {
            "product_card": ".product-grid-item", 
            "product_name": ".wd-entities-title a",
            "price_container": ".price",
            # Price logic:
            # .price > .woocommerce-Price-amount bdi (Standard)
            # .price > ins > .woocommerce-Price-amount bdi (Sale Price / Final)
            # .price > del > .woocommerce-Price-amount bdi (Old Price)
            
            "link": ".product-image-link", 
            "image": ".product-image-link img",
            
            # Detail page
            "detail_sku": ".sku",
            "detail_desc": "#tab-description",
            "detail_short_desc": ".woocommerce-product-details__short-description",
            "detail_image": ".woocommerce-product-gallery__image img",
            "detail_brand": "N/D", # Not explicitly standardized
            "detail_title": "h1.product_title",
            
            # Out of stock
            "out_of_stock": ".out-of-stock",

            # Pagination
            "next_button": "a.next.page-numbers"
        }

        super().__init__(base_url, headless, category_urls, selectors, site_name="FitMarketChile")
        self.known_brands = self._load_brands()
        self.seen_urls = set()
        self.classifier = CategoryClassifier()

    def _load_brands(self):
        """Carga el diccionario de marcas desde el CSV en la raíz del proyecto."""
        brands = []
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'marcas_dictionary.csv')
        try:
            with open(csv_path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    brand_name = row.get('nombre_marca', '').strip()
                    if brand_name:
                        brands.append(brand_name)
            # Sort by length descending so longer/more specific brands match first
            brands.sort(key=len, reverse=True)
            print(f"[green]  >> {len(brands)} marcas cargadas desde marcas_dictionary.csv[/green]")
        except Exception as e:
            print(f"[yellow]  >> No se pudo cargar marcas_dictionary.csv: {e}[/yellow]")
        return brands

    def _extract_brand(self, title, description):
        """Busca una marca conocida en el título y descripción del producto."""
        text_to_search = (title + " " + (description or "")).lower()
        for brand in self.known_brands:
            if brand.lower() in text_to_search:
                return brand
        return "N/D"

    def _classify_product(self, title, description, main_category, deterministic_subcategory, brand):
        """
        Clasificación de productos para FitMarketChile.
        Usa CategoryClassifier como base y aplica lógica extra específica de FitMarket.
        """
        title_lower = normalize(title.lower())
        text = title_lower + " " + normalize((description or "").lower())

        # ── Lógica extra FitMarket: Pack con " + " en Aminoácidos ─────────────
        # Para Aminoácidos, " + " NO indica pack salvo que empiece con número
        is_amino_category = main_category == "Aminoacidos y BCAA"
        starts_with_number = bool(re.match(r'^\d', title_lower.strip()))
        plus_is_pack = (" + " in title_lower) and (not is_amino_category or starts_with_number)
        if plus_is_pack:
            return "Packs", "Packs"

        # ── Lógica extra FitMarket: Snacks especiales en Proteínas ───────────
        if main_category == "Proteinas":
            if "shark up" in text or "gel" in text or "isotonic" in text:
                return "Snacks y Comida", "Otros Snacks y Comida"
            elif "bariatrix" in title_lower:
                pass  # Excepción: Bariatrix no es barra
            elif re.search(r'\bbar\b', text) or re.search(r'\bbarra\b', text) or "bites" in text or "whey bar" in text or "barrita" in text:
                return "Snacks y Comida", "Barritas Y Snacks Proteicas"
            elif "alfajor" in text:
                return "Snacks y Comida", "Snacks Dulces"

        # ── Lógica extra FitMarket: Perdida de Grasa (no cubierta por CategoryClassifier) ──
        if main_category == "Perdida de Grasa":
            cafeina_kw = ["cafeina", "caffeine", "cafein"]
            crema_kw = ["crema", "cream", "gel reductor", "gel reduct"]
            retencion_kw = ["retencion", "retencion", "diuretic", "diuretico", "drenante", "drain"]
            liquido_kw = ["liquid", "liquido", "liquido", "shot", "ampolla"]
            localizado_kw = ["localizado", "localizada", "abdomin", "belly", "zona"]
            natural_kw = ["natural", "verde", "green tea", "te verde", "garcinia", "raspberry", "frambuesa", "cla", "linoleic", "linoleico", "l-carnitin", "carnitina", "carnitine"]
            termogenico_kw = ["termogen", "thermogen", "thermo", "termo"]
            quemador_kw = ["quemador", "fat burn", "fat burner", "burner", "quemagras", "fat loss"]
            if any(k in text for k in cafeina_kw) and not any(k in text for k in quemador_kw + termogenico_kw):
                return "Perdida de Grasa", "Cafeína"
            elif any(k in text for k in crema_kw):
                return "Perdida de Grasa", "Cremas Reductoras"
            elif any(k in text for k in retencion_kw):
                return "Perdida de Grasa", "Eliminadores De Retencion"
            elif any(k in text for k in liquido_kw):
                return "Perdida de Grasa", "Quemadores Liquidos"
            elif any(k in text for k in localizado_kw):
                return "Perdida de Grasa", "Quemadores Localizados"
            elif any(k in text for k in natural_kw):
                return "Perdida de Grasa", "Quemadores Naturales"
            elif any(k in text for k in termogenico_kw):
                return "Perdida de Grasa", "Quemadores Termogenicos"
            elif any(k in text for k in quemador_kw):
                return "Perdida de Grasa", "Quemadores De Grasa"
            else:
                return "Perdida de Grasa", "Quemadores"

        # ── Lógica extra FitMarket: Pre Entrenos (no cubierta por CategoryClassifier) ───
        if main_category == "Pre Entrenos":
            sin_estim_kw = ["sin estimulante", "sin cafeina", "caffeine free", "stimulant free", "no stimulant", "pump", "non-stim"]
            guarana_kw = ["guarana", "guarana"]
            cafeina_kw = ["cafeina", "caffeine", "cafein"]
            beta_ala_kw = ["beta ala", "beta-ala", "beta alanina", "beta-alanina"]
            arginina_kw = ["arginin", "arginina"]
            bcaa_kw = ["bcaa", "branched", "ramificados"]
            energia_kw = ["gel", "gel energetico", "gel energetico", "energy gel", "cafe", "cafe", "coffee", "energy drink", "bebida energetica"]
            estimulantes_kw = ["estimulante", "stimulant", "pre-workout", "preworkout", "pre workout", "energia", "energia", "energy"]
            if any(k in text for k in sin_estim_kw):
                return "Pre Entrenos", "Pre-Entreno Sin Estimulantes"
            elif any(k in text for k in guarana_kw):
                return "Pre Entrenos", "Guarana"
            elif any(k in text for k in energia_kw):
                return "Pre Entrenos", "Energía (Geles/Café)"
            elif any(k in text for k in beta_ala_kw):
                return "Pre Entrenos", "Beta Alanina"
            elif any(k in text for k in arginina_kw):
                return "Pre Entrenos", "Arginina"
            elif any(k in text for k in bcaa_kw):
                return "Pre Entrenos", "BCAAs"
            elif any(k in text for k in cafeina_kw):
                return "Pre Entrenos", "Cafeína"
            elif any(k in text for k in estimulantes_kw):
                return "Pre Entrenos", "Pre-Entreno con Estimulantes"
            else:
                return "Pre Entrenos", "Pre Entreno"

        # ── Base: CategoryClassifier ─────────────────────────────────────────
        final_category, final_subcategory = self.classifier.classify(
            title, description, main_category, deterministic_subcategory, brand
        )

        # ── Post-clasificación FitMarket: overrides específicos de proteínas ───────
        if main_category == "Proteinas" and final_category == "Proteinas":
            if "revitta" in brand.lower() and "femme" in text:
                final_subcategory = "Proteína Aislada"
            elif "cooking" in text and "winkler" in text:
                final_subcategory = "Proteína de Whey"

        return final_category, final_subcategory

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en FitMarketChile...[/green]")
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
                            # Wait for grid
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
                                    link = href

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

                            # Logic for WooCommerce with ins/del
                            price_container = card.locator(self.selectors['price_container']).first
                            
                            # Check for discount (ins tag exists?)
                            ins_el = price_container.locator("ins .woocommerce-Price-amount bdi")
                            del_el = price_container.locator("del .woocommerce-Price-amount bdi")
                            
                            if ins_el.count() > 0:
                                active_discount = True
                                price_text = ins_el.first.inner_text()
                                # Check old price presence mostly to confirm discount logic, but ins is enough usually
                            else:
                                # Regular price (no discount)
                                # Often direct child or just one amount
                                reg_el = price_container.locator(".woocommerce-Price-amount bdi").first
                                if reg_el.count() > 0:
                                    price_text = reg_el.inner_text()

                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

                            # Out of stock check: if 'agotado' badge is present, price is 0
                            if card.locator(self.selectors['out_of_stock']).count() > 0:
                                price = 0
                                active_discount = False

                            # Thumbnail
                            thumbnail_url = ""
                            if card.locator(self.selectors['image']).count() > 0:
                                src = card.locator(self.selectors['image']).first.get_attribute("src")
                                if src:
                                    thumbnail_url = src

                            # 2. Detail Extraction
                            image_url = ""
                            sku = ""
                            description = ""
                            brand = "N/D" 

                            if link != "N/D":
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                                    
                                    # SKU
                                    if detail_page.locator(self.selectors['detail_sku']).count() > 0:
                                        sku_text = detail_page.locator(self.selectors['detail_sku']).first.inner_text()
                                        sku = sku_text.replace('SKU:', '').strip()

                                    # Description
                                    description = ""
                                    # Prioritize strict tab description
                                    if detail_page.locator(self.selectors['detail_desc']).count() > 0:
                                        description = detail_page.locator(self.selectors['detail_desc']).first.inner_text().strip()
                                    
                                    # Fallback to short description if main is empty or not found
                                    if not description and detail_page.locator(self.selectors['detail_short_desc']).count() > 0:
                                        description = detail_page.locator(self.selectors['detail_short_desc']).first.inner_text().strip()

                                    # Brand - Extract from title/description using known brands dictionary
                                    brand = self._extract_brand(title, description)
                                    
                                    # Main Image
                                    if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                        # Use first image
                                        img_src = detail_page.locator(self.selectors['detail_image']).first.get_attribute("src")
                                        if img_src:
                                            image_url = img_src
                                    
                                    if not image_url and thumbnail_url:
                                        image_url = thumbnail_url

                                    detail_page.close()

                                except Exception as e:
                                    print(f"[yellow]Error consiguiendo detalles de {link}: {e}[/yellow]")
                                    try: detail_page.close()
                                    except: pass
                            
                            # Categorization using heuristics
                            final_category, final_subcategory = self._classify_product(
                                title, description, main_category, deterministic_subcategory, brand
                            )

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
                        
                        # Pagination Logic
                        next_btn = page.locator(self.selectors['next_button'])

                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href_next = next_btn.first.get_attribute("href")
                            if href_next:
                                print(f"Navegando a página siguiente: {href_next}")
                                page.goto(href_next)
                                page_number += 1
                            else:
                                break
                        else:
                            break

                except Exception as e:
                    print(f"[red]Error procesando categoría {main_category}: {e}[/red]")

if __name__ == "__main__":
    scraper = FitMarketChileScraper("https://fitmarketchile.cl", headless=True)
    scraper.run()
