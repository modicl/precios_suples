from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from rich import print
from datetime import datetime
import re
import json
import time

class DecathlonScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        self.category_urls = {
            "Proteinas": [
                {"url": "https://www.decathlon.cl/5270-proteinas", "subcategory": "Otros Proteinas"}
            ],
            "Snacks y Comida": [
                {"url": "https://www.decathlon.cl/5271-barritas-de-proteina", "subcategory": "Barritas Y Snacks Proteicas"}   
            ],
            "Post Entreno" : [
                {"url": "https://www.decathlon.cl/5339-gel-energetico", "subcategory": "Energía (Geles/Café)"}
            ],
            "Creatinas": [
                {"url": "https://www.decathlon.cl/5341-creatina", "subcategory": "Otros Creatinas"}
            ],
            "Bebidas Nutricionales": [
                {"url": "https://www.decathlon.cl/5813-bebidas-isotonicas", "subcategory": "Bebidas Nutricionales"}
            ]
        }

        # Selectors not strictly used for JSON extraction but kept for compatibility or fallback
        selectors = {
            "product_card": "body", # dummy
            "next_button": "body"   # dummy
        }

        super().__init__(base_url, headless, self.category_urls, selectors, site_name="Decathlon")
        self.classifier = CategoryClassifier()

    # ------------------------------------------------------------------
    # Clasificación heurística
    # ------------------------------------------------------------------

    def _classify_product(self, title, description, main_category, deterministic_subcategory, brand):
        """
        Aplica CategoryClassifier para afinar la subcategoría.

        Decathlon presenta subcategorías deterministas fuertes gracias a sus
        URLs especializadas, por lo que solo se delega al clasificador en las
        categorías cuya subcategoría de origen es un fallback genérico:
          - "Proteinas"             → heurística completa (whey/aislada/vegana/…)
          - "Creatinas"             → heurística completa (monohidrato/HCL/…)
          - "Bebidas Nutricionales" → heurística para distinguir isotónico/energética/gel/batido

        Las demás URLs ya exponen subcategorías exactas y se respetan tal cual:
          - "Snacks y Comida" / "Barritas Y Snacks Proteicas"  → fijo
          - "Post Entreno"    / "Energía (Geles/Café)"         → fijo
        """
        NEEDS_CLASSIFICATION = {
            "Proteinas",
            "Creatinas",
            "Bebidas Nutricionales",
        }

        if main_category not in NEEDS_CLASSIFICATION:
            return main_category, deterministic_subcategory

        return self.classifier.classify(title, description, main_category, deterministic_subcategory, brand)

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en Decathlon (Modo JSON)...[/green]")
        context = page.context

        for main_category, items in self.category_urls.items():
            for item in items:
                base_category_url = item['url']
                deterministic_subcategory = item['subcategory']
                print(f"\n[bold blue]Procesando categoría:[/bold blue] {main_category} -> {deterministic_subcategory} ({base_category_url})")
                
                try:
                    page_number = 1
                    while True:
                        target_url = f"{base_category_url}?page={page_number}"
                        print(f"--- Página {page_number} ({target_url}) ---")
                        
                        try:
                            # Go to page
                            page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
                            # Wait a bit for scripts to populate if needed, though usually it's static in source
                            # Parse content
                            content = page.content()
                            
                            # Extract data using JS evaluation of the 'prestashop' global variable
                            try:
                                prestashop_data = page.evaluate("() => window.prestashop")
                            except Exception as e:
                                print(f"[red]Error evaluando window.prestashop: {e}[/red]")
                                break

                            if not prestashop_data:
                                print(f"[red]window.prestashop es None o vacío en página {page_number}[/red]")
                                break
                            
                            # Navigate to resultHits
                            # Structure appears to be: prestashop['modules']['oneshop_algolia']['resultHits']
                            try:
                                modules = prestashop_data.get('modules', {})
                                algolia_data = modules.get('oneshop_algolia', {})
                                hits = algolia_data.get('resultHits', [])
                            except AttributeError:
                                print(f"[red]Estructura de objeto inesperada en página {page_number}[/red]")
                                break
                            
                            if not hits:
                                print(f"[yellow]No hay productos en 'resultHits' para la página {page_number}. Fin de categoría.[/yellow]")
                                break

                            print(f"  > Encontrados {len(hits)} productos en JSON.")

                            for item in hits:
                                current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                
                                raw_title = item.get('product_name', 'N/D')
                                title = self.clean_text(raw_title)
                                
                                raw_brand = item.get('brand', 'Decathlon')
                                brand = self.clean_text(raw_brand)
                                
                                link = item.get('url', 'N/D')

                                # Deduplication Check
                                if link != "N/D" and link in self.seen_urls:
                                    print(f"[yellow]  >> Producto duplicado omitido: {title}[/yellow]")
                                    continue
                                if link != "N/D":
                                    self.seen_urls.add(link)
                                # Price logic
                                price = item.get('prix', 0)
                                regular_price = item.get('regular', 0)
                                active_discount = False
                                
                                if regular_price > price:
                                    active_discount = True
                                
                                # Image
                                thumbnail_url = item.get('image_url', '')
                                if not thumbnail_url:
                                    thumbnail_url = item.get('thumb_url', '')
                                
                                image_url = thumbnail_url # Detailed image is usually same URL with different params
                                
                                sku = item.get('sku', '')

                                # Descripción enriquecida: combina teaser (qué es) +
                                # made_for (para quién sirve) + composition (ingredientes, si existe).
                                # Ambos campos están siempre presentes en Algolia; composition
                                # suele estar vacío salvo en gainers/productos compuestos.
                                parts = []
                                teaser = (item.get('teaser') or '').strip()
                                made_for = (item.get('made_for') or '').strip()
                                composition = (item.get('composition') or '').strip()
                                if teaser:
                                    parts.append(teaser)
                                if made_for and made_for != teaser:
                                    parts.append(made_for)
                                if composition:
                                    parts.append(composition)
                                description = ' | '.join(parts)
                                
                                # Categorización heurística via CategoryClassifier
                                final_category, final_subcategory = self._classify_product(
                                    title, description, main_category, deterministic_subcategory, brand
                                )
                                # cat_info = self.categorizer.classify_product(title, main_category)
                                # if cat_info:
                                #    final_subcategory = cat_info['nombre_subcategoria']

                                site_folder = self.site_name.replace(" ", "_").lower()
                                if thumbnail_url:
                                    local_thumb = self.download_image(thumbnail_url, subfolder=site_folder)
                                    if local_thumb: thumbnail_url = local_thumb
                                if image_url:
                                    local_img = self.download_image(image_url, subfolder=site_folder)
                                    if local_img: image_url = local_img

                                yield {
                                    'date': current_date,
                                    'site_name': self.site_name,
                                    'category': self.clean_text(final_category),
                                    'subcategory': final_subcategory,
                                    'product_name': title,

                                    'brand': self.enrich_brand(brand, title),
                                    'price': int(price),
                                    'link': link,
                                    'rating': str(item.get('rating_average', 0)),
                                    'reviews': str(item.get('rating_count', 0)),
                                    'active_discount': active_discount,
                                    'thumbnail_image_url': thumbnail_url,
                                    'image_url': image_url,
                                    'sku': sku,
                                    'description': description
                                }

                            page_number += 1
                            # Optional: avoid infinite loops if something is wrong
                            if page_number > 50: break 

                        except Exception as e:
                            print(f"[red]Error procesando página {page_number}: {e}[/red]")
                            # Try next page or break? Break usually.
                            break

                except Exception as e:
                    print(f"[red]Error procesando categoría {main_category}: {e}[/red]")

if __name__ == "__main__":
    # Keep headless=False because of Cloudflare
    scraper = DecathlonScraper("https://www.decathlon.cl", headless=True)
    scraper.run()
