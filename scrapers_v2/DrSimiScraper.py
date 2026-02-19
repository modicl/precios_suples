from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier, normalize
from rich import print
from datetime import datetime
import re
import json
import os

class DrSimiScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        # Specific category URLs provided by the user
        # Specific category URLs provided by the user
        self.category_urls = {
            "Pronutrition": [{"url": "https://www.drsimi.cl/pronutrition", "subcategory": "Pronutrition"}],
            "Vitaminas y Minerales": [{"url": "https://www.drsimi.cl/colageno?map=specificationFilter_41", "subcategory": "Colágeno"} , 
            {"url": "https://www.drsimi.cl/suplementos-y-alimentos/aceites-y-omegas", "subcategory": "Aceites Y Omegas"},
            {"url": "https://www.drsimi.cl/suplementos-y-alimentos/vitaminas-y-minerales", "subcategory": "Otros Vitaminas y Minerales"}],
            "Superalimento": [{"url": "https://www.drsimi.cl/suplementos-y-alimentos/superalimento", "subcategory": "Otros Superalimento"}],
            "Bebidas Nutricionales": [{"url": "https://www.drsimi.cl/suplementos-y-alimentos/bebidas-nutricionales", "subcategory": "Bebidas Nutricionales"}],
            "OTROS": [{"url": "https://www.drsimi.cl/suplementos-y-alimentos/deportistas", "subcategory": "Otros"}]
        }

        # Selectores VTEX identification
        selectors = {
            "product_card": ".vtex-product-summary-2-x-container",
            "product_name": ".vtex-product-summary-2-x-brandName",
            "price_container": ".vtex-product-price-1-x-sellingPriceValue--summary",
            "link": "a.vtex-product-summary-2-x-clearLink",
            "image": "img.vtex-product-summary-2-x-image",
            
            # Selectores de página de detalle
            "detail_image": ".vtex-store-components-3-x-productImageTag",
            "description": ".vtex-store-components-3-x-productDescriptionText"
        }

        super().__init__(base_url, headless, category_urls=self.category_urls, selectors=selectors, site_name="Dr Simi")
        self.classifier = CategoryClassifier()

    def _classify_product(self, name, description, main_category, deterministic_subcategory, brand=""):
        """
        Clasificación de productos para DrSimi.
        Aplica overrides determinísticos para categorías bien definidas
        y delega al CategoryClassifier para las ambiguas.

        NOTA: La inferencia de categoría principal opera SOLO sobre el título
        normalizado (title_lower) para evitar contaminación por keywords de
        ingredientes que aparecen en la descripción del producto.
        """
        title_lower = normalize(name.lower())
        # text (título + descripción) solo se usa para sub-clasificación dentro
        # de una categoría ya determinada, nunca para inferir la categoría principal.
        text = title_lower + " " + normalize((description or "").lower())

        # ── Vitaminas y Minerales (incluye subcats determinísticas) ───────────
        # Las URLs de Colágeno y Aceites y Omegas ahora caen bajo main_category
        # 'Vitaminas y Minerales', diferenciadas por deterministic_subcategory.
        if main_category == "Vitaminas y Minerales":
            # Subcategorías ya determinadas por URL → devolver directo
            if deterministic_subcategory == "Col\u00e1geno":
                return "Vitaminas y Minerales", "Col\u00e1geno"
            if deterministic_subcategory == "Aceites Y Omegas":
                return "Vitaminas y Minerales", "Omega 3 y Aceites"
            # Resto (Otros Vitaminas y Minerales) → sub-clasificar con classifier
            final_category, final_subcategory = self.classifier.classify(
                name, description, "Vitaminas y Minerales", deterministic_subcategory, brand
            )
            return final_category, final_subcategory

        # Bebidas Nutricionales: sub-clasificar con el classifier
        if main_category == "Bebidas Nutricionales":
            final_category, final_subcategory = self.classifier.classify(
                name, description, "Bebidas Nutricionales", deterministic_subcategory, brand
            )
            return final_category, final_subcategory

        # Superalimento: va a Snacks y Comida (clasificar sub)
        if main_category == "Superalimento":
            final_category, final_subcategory = self.classifier.classify(
                name, description, "Snacks y Comida", "Otros Snacks y Comida", brand
            )
            return final_category, final_subcategory

        # ── Pronutrition, Deportistas y OTROS: categorización completa ───────────
        # "Pronutrition" es una marca, "OTROS" / "Deportistas" son cajones de sastre.
        # Se infiere categoría real SOLO desde el título (no descripción) para evitar
        # contaminación por ingredientes mencionados en la descripción.
        # También capturamos "Proteinas" genérico por si viene de algún path externo.
        if main_category in ("Pronutrition", "Deportistas", "OTROS", "Proteinas"):

            # ── 0. Marcas con categoría propia conocida ─────────────────────
            # Suerox = bebida de hidratación oral → isotónico
            if "suerox" in title_lower:
                return "Bebidas Nutricionales", "Bebidas Nutricionales"

            # ── 1. Snacks y barritas ─────────────────────────────────────────
            if re.search(r'\bbar\b', title_lower) or re.search(r'\bbarra\b', title_lower) \
                    or "bites" in title_lower or "barrita" in title_lower \
                    or "cookie" in title_lower or "galleta" in title_lower:
                return "Snacks y Comida", "Barritas Y Snacks Proteicas"
            elif "alfajor" in title_lower or "brownie" in title_lower or "panqueque" in title_lower:
                return "Snacks y Comida", "Snacks Dulces"
            elif "mantequilla" in title_lower and "mani" in title_lower:
                return "Snacks y Comida", "Mantequilla De Mani"
            elif "cereal" in title_lower or "avena" in title_lower or "granola" in title_lower:
                return "Snacks y Comida", "Cereales"

            # ── 2. Bebidas RTD / isotónicos ──────────────────────────────────
            if "isoton" in title_lower or "electrolit" in title_lower or "hidratacion" in title_lower:
                return "Bebidas Nutricionales", "Isotónicos"
            if re.search(r'\brtd\b', title_lower) or "bebida proteica" in title_lower \
                    or "batido listo" in title_lower or "shake listo" in title_lower:
                return "Bebidas Nutricionales", "Batidos de proteína"
            if "energia" in title_lower and re.search(r'\bml\b', title_lower):
                return "Bebidas Nutricionales", "Bebidas Energéticas"

            # ── 3. Keywords de inferencia de categoría principal ─────────────
            # IMPORTANTE: se evalúa solo title_lower para evitar falsos positivos
            # por keywords en la descripción del producto.
            kw_ganador      = ["gainer", "ganador de peso", "mass gainer", "hipercalorico",
                               "weight gainer", "voluminizador"]
            kw_creatina     = ["creatina", "creatine", "monohidrato", "creapure",
                               "kre-alkalyn", "creatine hcl"]
            kw_pre_entreno  = ["pre-entreno", "pre entreno", "preworkout", "pre workout",
                               "pre-workout", "pre work", "pre-work",
                               "cafeina", "caffeine", "stim", "pump",
                               "fuerza explosiva", "beta alanina",
                               "energy booster", "nano cafeina",
                               "waxy maize", "carbohidrato energetico"]
            kw_perdida_grasa= ["carnitina", "quemador", "termogenico", "fat burner",
                               "cla ", "conjugated", "garcinia", "te verde", "diuretico"]
            kw_aminoacidos  = ["bcaa", "aminoacido", "glutamina", "leucina", "eaa",
                               "hmb", "citrulina", "arginina", "taurina", "lisina"]
            kw_proteina     = ["proteina", "whey", "protein", "caseina", "casein",
                               "albumina", "isolate", "hidrolizada", "concentrada",
                               "plant protein", "soya protein"]
            kw_omega_col    = ["omega", "aceite", "fish oil", "colageno", "collagen"]
            kw_vitaminas    = ["vitamina", "mineral", "magnesio", "zinc", "calcio",
                               "hierro", "potasio", "selenio", "biotina", "curcuma",
                               "curcumin", "melatonin", "multivitamin", "complejo b",
                               "acido folico", "probiotico", "prebiotico"]

            # Orden de prioridad: lo más específico primero.
            # Ganadores y creatinas antes que proteínas para evitar falsos positivos.
            if any(k in title_lower for k in kw_ganador):
                inferred_cat = "Ganadores de Peso"
            elif any(k in title_lower for k in kw_creatina):
                inferred_cat = "Creatinas"
            elif any(k in title_lower for k in kw_pre_entreno):
                inferred_cat = "Pre Entrenos"
            elif any(k in title_lower for k in kw_perdida_grasa):
                inferred_cat = "Perdida de Grasa"
            elif any(k in title_lower for k in kw_aminoacidos):
                inferred_cat = "Aminoacidos y BCAA"
            elif any(k in title_lower for k in kw_proteina):
                inferred_cat = "Proteinas"
            elif any(k in title_lower for k in kw_omega_col):
                inferred_cat = "Vitaminas y Minerales"
            elif any(k in title_lower for k in kw_vitaminas):
                inferred_cat = "Vitaminas y Minerales"
            else:
                # No se pudo determinar la categoría → no inventar, marcar como OTROS
                return "OTROS", "Otros"

            final_category, final_subcategory = self.classifier.classify(
                name, description, inferred_cat, inferred_cat, brand
            )

            # Guard de seguridad: "Pronutrition" nunca debe aparecer en el output
            if final_category == "Pronutrition" or final_subcategory == "Pronutrition":
                final_category = "Proteinas"
                final_subcategory = "Proteína de Whey"

            return final_category, final_subcategory

        # ── Fallback: delegar al classifier con la categoría tal como viene ───
        final_category, final_subcategory = self.classifier.classify(
            name, description, main_category, deterministic_subcategory, brand
        )

        # Guard global: "Pronutrition" nunca debe aparecer en el output
        if final_category == "Pronutrition" or final_subcategory == "Pronutrition":
            final_category = "Proteinas"
            final_subcategory = "Proteína de Whey"

        return final_category, final_subcategory


    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en Dr. Simi...[/green]")
        
        batch_buffer = []  # unused, kept for compatibility

        for main_category, items in self.category_urls.items():
            for item in items:
                base_category_url = item['url']
                deterministic_subcategory = item['subcategory']
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {deterministic_subcategory}")
            
            page_num = 1
            has_more = True
            
            while has_more:
                # Construct URL with pagination
                separator = "&" if "?" in base_category_url else "?"
                url = f"{base_category_url}{separator}page={page_num}"
                
                print(f"  [cyan]Página {page_num}:[/cyan] {url}")
                
                try:
                    # Navigate with a decent timeout
                    page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    # Wait for products to load
                    try:
                        page.wait_for_selector(self.selectors['product_card'], timeout=10000)
                    except:
                        print(f"    [yellow]No se encontraron más productos o la página tardó demasiado en cargar.[/yellow]")
                        has_more = False
                        continue

                    # Get all product cards
                    cards = page.locator(self.selectors['product_card'])
                    count = cards.count()
                    
                    if count == 0:
                        has_more = False
                        continue
                        
                    print(f"    Encontrados {count} productos.")

                    for i in range(count):
                        card = cards.nth(i)
                        
                        # Extract basic info
                        name = "N/D"
                        if card.locator(self.selectors['product_name']).count() > 0:
                            raw_name = card.locator(self.selectors['product_name']).first.inner_text()
                            name = self.clean_text(raw_name)

                        link = "N/D"

                        if card.locator(self.selectors['link']).count() > 0:
                            href = card.locator(self.selectors['link']).first.get_attribute("href")
                            if href:
                                link = self.base_url + href if href.startswith('/') else href

                        # Deduplication Check
                        if link != "N/D" and link in self.seen_urls:
                            print(f"[yellow]  >> Producto duplicado omitido: {name}[/yellow]")
                            continue
                        if link != "N/D":
                            self.seen_urls.add(link)

                        price = 0
                        if card.locator(self.selectors['price_container']).count() > 0:
                            price_text = card.locator(self.selectors['price_container']).first.inner_text()
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

                        image_url = ""
                        if card.locator(self.selectors['image']).count() > 0:
                            image_url = card.locator(self.selectors['image']).first.get_attribute("src")

                        # Brand Logic: 
                        # - If category is "Pronutrition", brand is "Pronutrition"
                        # - Otherwise, Dr Simi doesn't show brand in list. 
                        # - We could check the name for "Simi" or just "Dr Simi" as default or "N/D"
                        brand = "N/D"
                        if main_category == "Pronutrition":
                            brand = "Pronutrition"
                        elif "simi" in name.lower():
                            brand = "Dr Simi"
                        
                        brand = self.clean_text(brand)
                        
                        # DETAIL EXTRACTION (NEW TAB)
                        detail_image_url = image_url  # Fallback a thumbnail
                        sku = "N/D"
                        description = "N/D"
                        
                        if link != "N/D":
                            context = page.context
                            detail_page = None
                            try:
                                detail_page = context.new_page()
                                detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                                
                                # Extraer imagen full
                                if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                    detail_image_url = detail_page.locator(self.selectors['detail_image']).first.get_attribute('src')
                                
                                # Extraer descripción principal
                                desc_parts = []
                                if detail_page.locator(self.selectors['description']).count() > 0:
                                    main_desc = detail_page.locator(self.selectors['description']).first.inner_text().strip()
                                    if main_desc:
                                        desc_parts.append(main_desc)
                                
                                # Extraer especificaciones de la tabla (Modo de uso, Instrucciones, etc.)
                                try:
                                    specs = detail_page.evaluate('''() => {
                                        const rows = document.querySelectorAll('.vtex-store-components-3-x-specificationsTableRow');
                                        if (rows.length > 0) {
                                            return Array.from(rows).map(row => {
                                                const cells = row.querySelectorAll('td');
                                                const prop = cells[0] ? cells[0].innerText.trim() : '';
                                                const val = cells[1] ? cells[1].innerText.trim() : '';
                                                return prop && val ? prop + ': ' + val : null;
                                            }).filter(Boolean);
                                        }
                                        // Fallback: usar selectores de clase directos
                                        const props = document.querySelectorAll('.vtex-store-components-3-x-specificationItemProperty');
                                        const vals = document.querySelectorAll('.vtex-store-components-3-x-specificationItemSpecifications');
                                        return Array.from(props).map((el, i) => {
                                            const prop = el.innerText.trim();
                                            const val = vals[i] ? vals[i].innerText.trim() : '';
                                            return prop && val ? prop + ': ' + val : null;
                                        }).filter(Boolean);
                                    }''')
                                    if specs:
                                        desc_parts.extend(specs)
                                except Exception as spec_err:
                                    print(f"    [yellow]No se pudieron extraer especificaciones: {spec_err}[/yellow]")
                                
                                if desc_parts:
                                    description = " | ".join(desc_parts)
                                
                                # Extraer SKU del script JSON
                                try:
                                    sku_script = detail_page.evaluate('''() => {
                                        const scripts = Array.from(document.querySelectorAll('script'));
                                        for (const script of scripts) {
                                            const text = script.textContent;
                                            if (text.includes('"sku"')) {
                                                const skuMatch = text.match(/"sku"\\s*:\\s*"([^"]+)"/);
                                                if (skuMatch) return skuMatch[1];
                                            }
                                        }
                                        return null;
                                    }''')
                                    if sku_script:
                                        sku = sku_script
                                except:
                                    pass
                                
                                # Extraer brand de la página de detalle si no se encontró
                                if brand == "N/D":
                                    try:
                                        brand_elem = detail_page.locator('.vtex-store-components-3-x-productBrandName, .brand').first
                                        if brand_elem.count() > 0:
                                            raw_brand = brand_elem.inner_text()
                                            brand = self.clean_text(raw_brand)
                                    except:
                                        pass
                                
                                detail_page.close()
                            except Exception as e:
                                print(f"    [red]Error extrayendo detalle de {link}: {e}[/red]")
                                if detail_page:
                                    detail_page.close()
                        
                        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # ── Reglas de marca específicas DrSimi
                        name_lower_brand = name.lower()
                        if "veggimilk" in name_lower_brand:
                            brand = "Aquasolar"
                        elif "suerox" in name_lower_brand:
                            brand = "Suerox"
                        
                        # Si la marca sigue siendo N/D, intentar detectarla con el diccionario
                        # Primero en el título, luego en la descripción
                        if brand in ["N/D", "ND", ""]:
                            detected_brand = self.brand_matcher.get_best_match(name)
                            if detected_brand != "N/D":
                                brand = detected_brand
                            else:
                                detected_brand = self.brand_matcher.get_best_match(description)
                                if detected_brand != "N/D":
                                    brand = detected_brand

                        # Categorización: delegar completamente a _classify_product.
                        # Toda la lógica de override vive ahí para operar solo sobre el título.
                        name_upper = name.upper()

                        # --- Override: Vaso Shaker -> OTROS ---
                        if "VASO SHAKER" in name_upper:
                            final_category = "OTROS"
                            final_subcategory = "Otros"
                            print(f"  [magenta]Override Shaker aplicado: {name}[/magenta]")
                        else:
                            final_category, final_subcategory = self._classify_product(
                                name, description, main_category, deterministic_subcategory, brand
                            )

                        product_obj = {
                            'date': current_date,
                            'site_name': self.site_name,
                            'category': self.clean_text(final_category),
                            'subcategory': final_subcategory,
                            'product_name': name,
                            # La marca ya fue enriquecida arriba, la pasamos directo
                            'brand': brand,
                            'price': price,
                            'link': link,
                            'rating': "0",
                            'reviews': "0",
                            'active_discount': False, 
                            'thumbnail_image_url': image_url,
                            'image_url': detail_image_url,
                            'sku': sku,
                            'description': description
                        }
                        
                        yield product_obj

                    # Check if there is a next page/show more button to decide if we continue
                    page_num += 1
                    
                    # Safety break to avoid infinite loops if something goes wrong
                    if page_num > 50:
                        has_more = False

                except Exception as e:
                    print(f"    [red]Error en página {page_num}: {e}[/red]")
                    has_more = False
            
            # End of category loop - No buffer needed anymore

if __name__ == "__main__":
    scraper = DrSimiScraper("https://www.drsimi.cl", headless=True)
    scraper.run()
