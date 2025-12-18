import csv
import os
from datetime import datetime
from playwright.sync_api import sync_playwright, Page, Playwright

class BaseScraper:
    # Constructor que recibe la URL base, el modo headless, urls de categorías y selectores
    def __init__(self, base_url: str, headless: bool = False, category_urls: list = None, selectors: dict = None, site_name: str = "N/D"):
        self.base_url = base_url # URL base del sitio web
        self.headless = headless # Modo headless
        self.category_urls = category_urls if category_urls else [] # Categorias de productos y su URL
        self.selectors = selectors if selectors else {} # Selectores para extraer datos en forma de diccionario
        self.site_name = site_name # Nombre del sitio web
        
    # Metodo que inicia el navegador y el proceso de scraping
    def run(self):
        """
        Inicia el navegador y el proceso de scraping.
        """
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()
            
            # Crear directorio raw_data si no existe
            output_dir = "raw_data"
            os.makedirs(output_dir, exist_ok=True)
            
            # Nombre del archivo CSV estandarizado dentro de raw_data
            csv_filename = f"productos_{self.site_name.replace(' ', '_').lower()}_{datetime.now().strftime('%Y-%m-%d')}.csv"
            csv_file = os.path.join(output_dir, csv_filename)
            
            csv_headers = ['date', 'site_name', 'category', 'subcategory', 'product_name', 'brand', 'price', 'link', 'rating', 'reviews', 'active_discount']
            
            print(f"Guardando datos en {csv_file}...")
            
            try:
                with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=csv_headers)
                    writer.writeheader()
                
                    print(f"Navegando a: {self.base_url}")
                    # page.goto(self.base_url, wait_until="domcontentloaded") 
                    # El extract_process de AllNutrition navega a URLs propias, pero dejamos esto por si acaso
                    
                    # Consumimos el generador
                    for product_data in self.extract_process(page):
                        writer.writerow(product_data)
                        f.flush() # Guardado progresivo
                        
            except Exception as e:
                print(f"Ocurrió un error: {e}")
            finally:
                browser.close()

    # Metodo abstracto que debe ser implementado por las clases hijas
    def extract_process(self, page: Page):
        """
        Método que implementa la lógica de extracción específica.
        Debe ser sobreescrito por las clases hijas y debe funcionar como un Generador (yield).
        """
        raise NotImplementedError("El método extract_process debe ser implementado por la clase hija.")