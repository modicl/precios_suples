from BaseScraper import BaseScraper
from playwright.sync_api import Page
import time
from datetime import datetime

class WildFoodsScraper(BaseScraper):
    def __init__(self, headless=False):
        base_url = "https://thewildfoods.com"
        
        # 1. Definición de Categorías (Categoría -> Lista de URLs)
        category_urls = {
            "Snacks y Comida": [
                "https://thewildfoods.com/collections/barritas-wild-protein",
                "https://thewildfoods.com/collections/barritas-veganas",
                "https://thewildfoods.com/collections/wild-fit"
            ],
            "Creatinas": [
                "https://thewildfoods.com/collections/creatina"
            ],
            "Bebidas Nutricionales": [
                "https://thewildfoods.com/collections/batidos-de-proteina"
            ],
            "Proteinas": [
                "https://thewildfoods.com/collections/whey-protein"
            ]
        }
        
        # 2. Mapa Específico de Marcas (URL Parcial o Completa -> Marca)
        self.brand_map = {
            "barritas-wild-protein": "WILD PROTEIN",
            "barritas-veganas": "WILD PROTEIN",
            "wild-fit": "WILD FIT",
            "creatina": "WILD",
            "batidos-de-proteina": "WILD",
            "whey-protein": "WILD"
        }
        
        # 3. Selectores Verificados
        selectors = {
            'product_card': '.product-item', 
            'product_name': '.product-item__title',
            'price': '.product-price--original, .price-item--regular, .price',
            'link': 'a.product-item__link, .product-item__title',
            'image': '.product-item__image img, .product-card__image img',
            'next_button': '.pagination__next a'
        }
        
        super().__init__(base_url, headless, category_urls, selectors, site_name="Wild Foods")

    def extract_process(self, page: Page):
        for main_category, urls in self.category_urls.items():
            for url in urls:
                # Determinar Marca
                slug = url.rstrip('/').split('/')[-1]
                current_brand = self.brand_map.get(slug, "WILD") # Default a WILD
                current_brand = self.clean_text(current_brand)
                
                print(f"Procesando {main_category} - Marca: {current_brand} - URL: {url}")
                
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(3) # Espera inicial
                    
                    while True:
                        # Scroll to bottom to ensure images load
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        time.sleep(2)
                        
                        products = page.query_selector_all(self.selectors['product_card'])
                        print(f"Productos encontrados en página: {len(products)}")
                        
                        for product in products:
                            try:
                                # Nombre
                                name_el = product.query_selector(self.selectors['product_name'])
                                raw_name = name_el.inner_text() if name_el else "N/D"
                                name = self.clean_text(raw_name)
                                
                                # Precio
                                price_el = product.query_selector(self.selectors['price'])
                                price_text = price_el.inner_text().strip() if price_el else "0"
                                price = int(price_text.replace('$', '').replace('.', '').replace(' ', '')) if price_text != "0" else 0
                                
                                # Link
                                link_el = product.query_selector(self.selectors['link'])
                                link = link_el.get_attribute('href') if link_el else "N/D"
                                if link and not link.startswith('http'):
                                    link = self.base_url + link
                                
                                # Imagen
                                image_el = product.query_selector(self.selectors['image'])
                                image_url = "N/D"
                                if image_el:
                                    # Try data-src, src, srcset
                                    image_url = image_el.get_attribute('src')
                                    if not image_url:
                                         image_url = image_el.get_attribute('data-src')
                                    
                                    if image_url and image_url.startswith('//'):
                                        image_url = "https:" + image_url
                                
                                # Descargar imagen localmente
                                local_image_path = ""
                                if image_url and image_url != "N/D":
                                    local_image_path = self.download_image(image_url, subfolder="wild_foods")
                                
                                # Yield result
                                yield {
                                    'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    'site_name': self.site_name,
                                    'category': self.clean_text(main_category),
                                    'subcategory': "N/D",
                                    'product_name': name,
                                    'brand': self.enrich_brand(current_brand, name),
                                    'price': price,
                                    'link': link,
                                    'rating': "N/D",
                                    'reviews': "N/D",
                                    'active_discount': "N/D",
                                    'thumbnail_image_url': local_image_path if local_image_path else image_url, # Use local path if available
                                    'image_url': local_image_path if local_image_path else image_url,
                                    'sku': "N/D",
                                    'description': "N/D"
                                }
                                
                            except Exception as e:
                                print(f"Error extrayendo producto: {e}")
                                continue
                        
                        # Paginación
                        next_btn = page.query_selector(self.selectors['next_button'])
                        if next_btn:
                            print("Navegando a la siguiente página...")
                            # Sometimes next button is hidden or link logic varies
                            next_url = next_btn.get_attribute('href')
                            if next_url:
                                page.goto(next_url if next_url.startswith('http') else self.base_url + next_url)
                                time.sleep(3)
                            else:
                                break
                        else:
                            break
                            
                except Exception as e:
                    print(f"Error navegando a {url}: {e}")

if __name__ == "__main__":
    scraper = WildFoodsScraper(headless=True)
    scraper.run()
