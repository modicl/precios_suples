from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import json
import time

class DecathlonScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        self.category_urls = {
            "Proteinas": [
                {"url": "https://www.decathlon.cl/5270-proteinas", "subcategory": "Proteinas"}
            ],
            "Snacks y Comida": [
                {"url": "https://www.decathlon.cl/5271-barritas-de-proteina", "subcategory": "Barritas De Proteina"},
                {"url": "https://www.decathlon.cl/5339-gel-energetico", "subcategory": "Gel Energetico"}
            ],
            "Creatinas": [
                {"url": "https://www.decathlon.cl/5341-creatina", "subcategory": "Creatina"}
            ],
            "Isotonicas": [
                {"url": "https://www.decathlon.cl/5813-bebidas-isotonicas", "subcategory": "Bebidas Isotonicas"}
            ]
        }

        # Selectors not strictly used for JSON extraction but kept for compatibility or fallback
        selectors = {
            "product_card": "body", # dummy
            "next_button": "body"   # dummy
        }

        super().__init__(base_url, headless, category_urls, selectors, site_name="Decathlon")

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
                                description = item.get('made_for', '')
                                if not description:
                                    description = item.get('teaser', '')
                                
                                # New Categorization Logic
                                final_subcategory = deterministic_subcategory
                                # cat_info = self.categorizer.classify_product(title, main_category)
                                # if cat_info:
                                #    final_subcategory = cat_info['nombre_subcategoria']

                                yield {
                                    'date': current_date,
                                    'site_name': self.site_name,
                                    'category': self.clean_text(main_category),
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
