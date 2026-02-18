from BaseScraper import BaseScraper
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
        Aplica heurísticas para determinar la categoría y subcategoría final.
        Lógica portada desde MuscleFactoryScraper.
        """
        final_category = main_category
        final_subcategory = deterministic_subcategory

        # 1. Packs (Global Check)
        title_lower = title.lower()
        # Para Aminoácidos y BCAA, el " + " NO indica un pack (ej: "BCAA + Glutamina")
        # EXCEPCIÓN: si empieza con un número, SÍ es un pack (ej: "2 BCAA + Glutamina")
        is_amino_category = main_category == "Aminoacidos y BCAA"
        starts_with_number = bool(re.match(r'^\d', title_lower.strip()))
        
        plus_is_pack = (" + " in title_lower) and (not is_amino_category or starts_with_number)
        
        if "pack" in title_lower or "paquete" in title_lower or "combo" in title_lower or "+2" in title_lower or plus_is_pack:
            final_category = "Packs"
            final_subcategory = "Packs"
            return final_category, final_subcategory

        # 2. Heurística para Proteínas
        if final_category == "Proteinas":
            text_to_search = (title + " " + (description or "")).lower()

            # Snacks dentro de Proteínas (word boundaries para evitar falsos positivos)
            if "shark up" in text_to_search or "gel" in text_to_search or "isotonic" in text_to_search:
                final_category = "Snacks y Comida"
                final_subcategory = "Otros Snacks y Comida"
                return final_category, final_subcategory
            elif "bariatrix" in title_lower:
                pass  # Excepción: Bariatrix no es barra
            elif re.search(r'\bbar\b', text_to_search) or re.search(r'\bbarra\b', text_to_search) or "bites" in text_to_search or "whey bar" in text_to_search or "barrita" in text_to_search:
                final_category = "Snacks y Comida"
                final_subcategory = "Barritas Y Snacks Proteicas"
                return final_category, final_subcategory
            elif "alfajor" in text_to_search:
                final_category = "Snacks y Comida"
                final_subcategory = "Snacks Dulces"
                return final_category, final_subcategory

            # Dymatize logic
            if brand.lower() == "dymatize":
                if re.search(r'\biso\b', text_to_search) or "isolate" in text_to_search or "aislada" in text_to_search or "isolated" in text_to_search or "isofit" in text_to_search:
                    final_subcategory = "Proteína Aislada"
                elif "hydro" in title_lower or "hidrolizada" in title_lower or "hydrolized" in title_lower or "hydrolyzed" in title_lower or "hidrolizado" in title_lower:
                    final_subcategory = "Proteína Hidrolizada"
                else:
                    final_subcategory = "Proteína de Whey"
            else:
                # Casos extremadamente específicos
                if "revitta" in brand.lower() and "femme" in text_to_search:
                    final_subcategory = "Proteína Aislada"
                elif "cooking" in text_to_search and "winkler" in text_to_search:
                    final_subcategory = "Proteína de Whey"
                elif "vegan" in text_to_search or "plant" in text_to_search or "vegana" in text_to_search or "vegano" in text_to_search or "plant based" in text_to_search:
                    final_subcategory = "Proteína Vegana"
                elif "beef" in text_to_search or "carne" in text_to_search or "vacuno" in text_to_search:
                    final_subcategory = "Proteína de Carne"
                elif "casein" in text_to_search or "caseina" in text_to_search or "micelar" in text_to_search or "micellar" in text_to_search:
                    final_subcategory = "Caseína"
                else:
                    # Purity Rule
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
                    elif re.search(r'\biso\b', text_to_search) or "isolate" in text_to_search or "aislada" in text_to_search or "isolated" in text_to_search or "isofit" in text_to_search or "isolatada" in text_to_search:
                        final_subcategory = "Proteína Aislada"
                    elif "hydro" in title_lower or "hidrolizada" in title_lower or "hydrolized" in title_lower or "hydrolyzed" in title_lower or "hidrolizado" in title_lower:
                        final_subcategory = "Proteína Hidrolizada"
                    else:
                        final_subcategory = "Proteína de Whey"

            return final_category, final_subcategory

        # 3. Heurística para Creatinas
        elif final_category == "Creatinas":
            text_to_search = (title + " " + (description or "")).lower()

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

        # 4. Heurística para Aminoácidos y BCAA
        elif final_category == "Aminoacidos y BCAA":
            text_to_search = title_lower  # Solo título para evitar falsos positivos

            zma_keywords = ["zma", "zmar"]
            minerales_keywords = ["magnesio", "zinc", "magne"]
            eaas_keywords = ["eaa", "essential amino", "aminoacidos esenciales", "esenciales", "neaa", "full spectrum", "espectro completo"]
            bcaa_keywords = ["bcaa", "branched", "ramificados"]
            glutamina_keywords = ["glutamin"]
            leucina_keywords = ["leucin", "leucine"]
            aminos_keywords = ["amino", "arginin", "citrulin", "beta ala", "beta alanina", "taurin", "carnitin", "tirosin", "tyrosin", "lisin", "lysin", "triptop", "metionin", "methionin", "histidin", "treonin", "threonin", "fenilalan", "phenylalan"]

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
                final_subcategory = "Aminoácidos"

            return final_category, final_subcategory

        # 5. Heurística para Pérdida de Grasa
        elif final_category == "Perdida de Grasa":
            text_to_search = (title + " " + (description or "")).lower()

            cafeina_keywords = ["cafeina", "caffeine", "cafein"]
            crema_keywords = ["crema", "cream", "gel reductor", "gel reduct"]
            retencion_keywords = ["retencion", "retención", "diuretic", "diuretico", "drenante", "drain"]
            liquido_keywords = ["liquid", "liquido", "líquido", "shot", "ampolla"]
            localizado_keywords = ["localizado", "localizada", "abdomin", "belly", "zona"]
            natural_keywords = ["natural", "verde", "green tea", "te verde", "garcinia", "raspberry", "frambuesa", "cla", "linoleic", "linoleico", "l-carnitin", "carnitina", "carnitine", "cla"]
            termogenico_keywords = ["termogen", "thermogen", "thermo", "termo"]
            quemador_keywords = ["quemador", "fat burn", "fat burner", "burner", "quemagras", "fat loss"]

            if any(k in text_to_search for k in cafeina_keywords) and not any(k in text_to_search for k in quemador_keywords + termogenico_keywords):
                final_subcategory = "Cafeína"
            elif any(k in text_to_search for k in crema_keywords):
                final_subcategory = "Cremas Reductoras"
            elif any(k in text_to_search for k in retencion_keywords):
                final_subcategory = "Eliminadores De Retencion"
            elif any(k in text_to_search for k in liquido_keywords):
                final_subcategory = "Quemadores Liquidos"
            elif any(k in text_to_search for k in localizado_keywords):
                final_subcategory = "Quemadores Localizados"
            elif any(k in text_to_search for k in natural_keywords):
                final_subcategory = "Quemadores Naturales"
            elif any(k in text_to_search for k in termogenico_keywords):
                final_subcategory = "Quemadores Termogenicos"
            elif any(k in text_to_search for k in quemador_keywords):
                final_subcategory = "Quemadores De Grasa"
            else:
                final_subcategory = "Quemadores"

            return final_category, final_subcategory

        # 6. Heurística para Pre Entrenos
        elif final_category == "Pre Entrenos":
            text_to_search = (title + " " + (description or "")).lower()

            sin_estimulantes_keywords = ["sin estimulante", "sin cafeina", "caffeine free", "stimulant free", "no stimulant", "pump", "non-stim"]
            guarana_keywords = ["guarana", "guaraná"]
            cafeina_keywords = ["cafeina", "caffeine", "cafein"]
            beta_alanina_keywords = ["beta ala", "beta-ala", "beta alanina", "beta-alanina"]
            arginina_keywords = ["arginin", "arginina"]
            bcaa_keywords = ["bcaa", "branched", "ramificados"]
            energia_keywords = ["gel", "gel energetico", "gel energético", "energy gel", "cafe", "café", "coffee", "energy drink", "bebida energetica"]
            estimulantes_keywords = ["estimulante", "stimulant", "pre-workout", "preworkout", "pre workout", "energia", "energía", "energy"]

            if any(k in text_to_search for k in sin_estimulantes_keywords):
                final_subcategory = "Pre-Entreno Sin Estimulantes"
            elif any(k in text_to_search for k in guarana_keywords):
                final_subcategory = "Guarana"
            elif any(k in text_to_search for k in energia_keywords):
                final_subcategory = "Energía (Geles/Café)"
            elif any(k in text_to_search for k in beta_alanina_keywords):
                final_subcategory = "Beta Alanina"
            elif any(k in text_to_search for k in arginina_keywords):
                final_subcategory = "Arginina"
            elif any(k in text_to_search for k in bcaa_keywords):
                final_subcategory = "BCAAs"
            elif any(k in text_to_search for k in cafeina_keywords):
                final_subcategory = "Cafeína"
            elif any(k in text_to_search for k in estimulantes_keywords):
                final_subcategory = "Pre-Entreno con Estimulantes"
            else:
                final_subcategory = "Pre Entreno"

            return final_category, final_subcategory

        # 7. Heurística para Vitaminas y Minerales
        elif final_category == "Vitaminas y Minerales":
            raw_text = (title + " " + (description or "")).lower()
            # Normalizar: quitar tildes para que 'colágeno' == 'colageno', etc.
            text_to_search = unicodedata.normalize('NFD', raw_text)
            text_to_search = ''.join(c for c in text_to_search if unicodedata.category(c) != 'Mn')

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
            gummies_keywords = ["gummi", "gummy", "gomita", "gomitas", "gummies"]

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

        # 8. Heurística para Snacks y Comida
        elif final_category == "Snacks y Comida":
            text_to_search = (title + " " + (description or "")).lower()

            if "shark up" in text_to_search or "gel" in text_to_search or "isotonic" in text_to_search:
                final_subcategory = "Otros Snacks y Comida"
            elif re.search(r'\bbar\b', text_to_search) or "bites" in text_to_search or "whey bar" in text_to_search or re.search(r'\bbarra\b', text_to_search) or "barrita" in text_to_search:
                final_subcategory = "Barritas Y Snacks Proteicas"
            elif "alfajor" in text_to_search:
                final_subcategory = "Snacks Dulces"
            else:
                final_subcategory = "Otros Snacks y Comida"

            return final_category, final_subcategory

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
