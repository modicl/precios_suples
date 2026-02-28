# Scraper para la pagina web FarmaciaKnop.com
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from rich import print
from datetime import datetime
import re
import random
import time
import unicodedata

class FarmaciaKnopScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        # Mapeo de URLs
        # Mapeo de URLs
        self.category_urls = {
            "Proteinas": [{"url": "https://www.farmaciasknop.com/types/proteinas", "subcategory": "Proteinas"}],
            "Vitaminas y Minerales" : [{"url": "https://www.farmaciasknop.com/types/multivitaminico", "subcategory": "Multivitamínicos"},
            {"url": "https://www.farmaciasknop.com/types/vitaminas", "subcategory": "CATEGORIZAR_VITAMINAS"},
            {"url": "https://www.farmaciasknop.com/types/magnesio", "subcategory": "Magnesio"},
            {"url": "https://www.farmaciasknop.com/types/zinc", "subcategory": "Otros Vitaminas y Minerales"},
            {"url": "https://www.farmaciasknop.com/types/calcio", "subcategory": "Calcio"},
            {"url": "https://www.farmaciasknop.com/types/probioticos", "subcategory": "Probióticos"}
            ],
            "Packs": [{"url": "https://www.farmaciasknop.com/types/pack", "subcategory": "Packs"}]
        }

        # Selectores identificados
        selectors = {
            "product_card": "div.product-item",
            "name": "h3.product-name a", # Nombre dentro del link
            "brand": "span.product-vendor", # Marca 
            "image": "a.product-link img", # Imagen dentro del link principal
            
            # Link del producto
            "link": "a.product-link",
            
            # Precios
            # Contenedor precio
            "price_container": ".prices",
            # Precio oferta o normal 
            "current_price": "span.bootic-price", 
            # Precio tachado
            "old_price": "strike.bootic-price-comparison, .old-price",
            
            # Stock: Botón "Notificarme" indica sin stock
            "notify_button": "button.notify-in-stock, button:has-text('Notificarme')",
            
            # Selectores de página de detalle
            "detail_image": ".product-image-container img, img[class*='product']",
            "description": "h3.accordion:has(button:has-text('Descripción')) + div.panel",
            "sku": "p:has-text('SKU:')"
        }

        super().__init__(base_url, headless, category_urls=self.category_urls, selectors=selectors, site_name="Farmacia Knopp")
        self.classifier = CategoryClassifier()

    def _classify_product(self, name, description, main_category, deterministic_subcategory):
        """
        Clasificación de productos para Farmacia Knop.
        Usa CategoryClassifier como base y aplica lógica extra específica de Knop.
        """
        from CategoryClassifier import normalize
        name_lower = normalize(name.lower())
        text = name_lower + " " + normalize((description or "").lower())

        # ── Lógica extra Knop: Packs con número inicial ──────────────────────
        # " + " en proteínas NO indica pack salvo que empiece con número
        starts_with_number = bool(re.match(r'^\d', name_lower.strip()))
        plus_is_pack = (" + " in name_lower) and starts_with_number
        if plus_is_pack:
            return "Packs", "Packs"

        # ── Lógica extra Knop: Snacks que se cuelan en Proteínas ─────────────
        if main_category == "Proteinas":
            if re.search(r'\bbar\b', text) or re.search(r'\bbarra\b', text) or "bites" in text or "whey bar" in text or "barrita" in text:
                return "Snacks y Comida", "Barritas Y Snacks Proteicas"
            elif "alfajor" in text:
                return "Snacks y Comida", "Snacks Dulces"

        # ── Lógica extra Knop: Vitaminas determinísticas no necesitan heurística
        if main_category == "Vitaminas y Minerales":
            if deterministic_subcategory not in ("CATEGORIZAR_VITAMINAS", "Vitaminas y Minerales"):
                return main_category, deterministic_subcategory

        # ── Base: CategoryClassifier ─────────────────────────────────────────
        # Para Proteinas: overrides específicos de Knop (yuno, lean active, wpc)
        final_category, final_subcategory = self.classifier.classify(
            name, description, main_category, deterministic_subcategory
        )

        # ── Post-clasificación Knop: overrides de proteínas ──────────────────
        if main_category == "Proteinas" and final_category == "Proteinas":
            if "lean active" in text:
                final_subcategory = "Proteína Vegana"
            elif "yuno" in text:
                final_subcategory = "Proteína de Whey"
            elif "wpc" in text and final_subcategory not in ("Proteína Aislada", "Proteína Hidrolizada", "Proteína Vegana", "Proteína de Carne", "Caseína"):
                final_subcategory = "Proteína de Whey"

        return final_category, final_subcategory

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en Farmacia Knop...[/green]")
        
        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                deterministic_subcategory = item['subcategory']
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {deterministic_subcategory} ({url})")

                try:
                    # Delay aleatorio entre categorías para evitar rate limiting
                    page.wait_for_timeout(random.randint(3000, 6000))
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_timeout(2000)  # Espera adicional para JS dinámico
                except Exception as e:
                    print(f"[red]Error cargando {url}: {e}[/red]")
                    continue

                # Esperar a que los productos estén visibles en el DOM
                try:
                    page.wait_for_selector(self.selectors['product_card'], timeout=15000)
                except Exception:
                    print(f"  [yellow]> No se encontraron productos en {url} (selector timeout)[/yellow]")
                    continue

                # Scroll infinito / Carga completa
                last_height = page.evaluate("document.body.scrollHeight")
                while True:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(2000)
                    new_height = page.evaluate("document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                # Extracción
                cards = page.locator(self.selectors['product_card'])
                count = cards.count()
                print(f"  > Encontrados {count} productos totales.")

                for i in range(count):
                    try:
                        card = cards.nth(i)
                        
                        # 1. Extracción de datos básicos
                        name = "N/D"
                        name_el = card.locator(self.selectors['name']).first
                        if name_el.count() > 0:
                            raw_name = name_el.inner_text()
                            name = self.clean_text(raw_name)
                        else:
                            # Fallback: title attribute or text inside h3 a
                            name_backup = card.locator("h3.product-title a").first
                            if name_backup.count() > 0:
                                raw_name = name_backup.get_attribute("title") or name_backup.inner_text()
                                name = self.clean_text(raw_name)

                        brand = "N/D"
                        brand_el = card.locator(self.selectors['brand']).first
                        if brand_el.count() > 0:
                            raw_brand = brand_el.inner_text()
                            brand = self.clean_text(raw_brand)
                            if "%" in brand or "$" in brand:
                                brand = "N/D"

                        link = "N/D"
                        link_el = card.locator(self.selectors['link']).first
                        if link_el.count() > 0:
                            href = link_el.get_attribute("href")
                            if href:
                                link = self.base_url + href if href.startswith('/') else href

                        # Deduplication Check
                        if link != "N/D" and link in self.seen_urls:
                            print(f"[yellow]  >> Producto duplicado omitido: {name}[/yellow]")
                            continue
                        if link != "N/D":
                            self.seen_urls.add(link)

                        image_url = ""
                        img_el = card.locator(self.selectors['image']).first
                        if img_el.count() > 0:
                            src = img_el.get_attribute("src")
                            if src:
                                image_url = src
                        
                        # 2. Stock Logic
                        price = 0
                        active_discount = False
                        
                        if card.locator(self.selectors['notify_button']).count() > 0 or \
                           card.locator("p.units-in-stock.no-stock").count() > 0:
                            price = 0
                        else:
                            # 3. Price Logic
                            old_price_el = card.locator(self.selectors['old_price']).first
                            has_old_price = old_price_el.count() > 0 and old_price_el.is_visible()
                            active_discount = has_old_price
                            
                            current_price_el = card.locator(self.selectors['current_price']).first
                            if current_price_el.count() > 0:
                                price_text = current_price_el.inner_text()
                                clean_price = re.sub(r'[^\d]', '', price_text)
                                if clean_price:
                                    price = int(clean_price)
                            else:
                                price = 0

                        # DETAIL EXTRACTION (NEW TAB)
                        detail_image_url = image_url  # Fallback a thumbnail
                        sku = "N/D"
                        description = "N/D"
                        
                        if link != "N/D":
                            context = page.context
                            detail_page = None
                            for attempt in range(3):
                                try:
                                    detail_page = context.new_page()
                                    detail_page.wait_for_timeout(random.randint(2000, 4000))
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=60000)
                                    detail_page.wait_for_timeout(2000)
                                    
                                    img_selector = self.selectors['detail_image']
                                    if detail_page.locator(img_selector).count() > 0:
                                        detail_image_url = detail_page.locator(img_selector).first.get_attribute('src')
                                    
                                    desc_selector = self.selectors['description']
                                    desc_el = detail_page.locator(desc_selector).first
                                    if desc_el.count() > 0:
                                        description = desc_el.inner_text().strip()
                                    else:
                                        fallback_sel = "h3.accordion:has(button:has-text('Información adicional')) + div.panel"
                                        fallback_el = detail_page.locator(fallback_sel).first
                                        if fallback_el.count() > 0:
                                            description = fallback_el.inner_text().strip()
                                    
                                    sku_selector = self.selectors['sku']
                                    if detail_page.locator(sku_selector).count() > 0:
                                        sku_text = detail_page.locator(sku_selector).first.inner_text()
                                        sku_match = re.search(r'SKU:\s*([A-Z0-9-]+)', sku_text, re.IGNORECASE)
                                        if sku_match:
                                            sku = sku_match.group(1)
                                    
                                    detail_page.close()
                                    break

                                except Exception as e:
                                    if detail_page:
                                        try:
                                            detail_page.close()
                                        except:
                                            pass
                                        detail_page = None
                                    if attempt < 2:
                                        wait = (attempt + 1) * 3000
                                        print(f"[yellow]  >> Reintento {attempt + 1}/3 para {link} (esperando {wait//1000}s): {e}[/yellow]")
                                        page.wait_for_timeout(wait)
                                    else:
                                        print(f"[red]Error extrayendo detalle de {link} tras 3 intentos: {e}[/red]")

                        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        final_category, final_subcategory = self._classify_product(
                            name, description, main_category, deterministic_subcategory
                        )

                        site_folder = self.site_name.replace(" ", "_").lower()
                        if image_url:
                            local_thumb = self.download_image(image_url, subfolder=site_folder)
                            if local_thumb: image_url = local_thumb
                        if detail_image_url:
                            local_img = self.download_image(detail_image_url, subfolder=site_folder)
                            if local_img: detail_image_url = local_img

                        yield {
                            'date': current_date,
                            'site_name': self.site_name,
                            'category': self.clean_text(final_category),
                            'subcategory': final_subcategory,
                            'product_name': name,
                            'brand': self.enrich_brand(brand, name),
                            'price': price,
                            'link': link,
                            'rating': "0",
                            'reviews': "0",
                            'active_discount': active_discount,
                            'thumbnail_image_url': image_url,
                            'image_url': detail_image_url,
                            'sku': sku,
                            'description': description
                        }

                    except Exception as e:
                        print(f"[red]Error extrayendo producto: {e}[/red]")
                        continue


if __name__ == "__main__":
    scraper = FarmaciaKnopScraper("https://www.farmaciasknop.com", headless=True)
    scraper.run()
