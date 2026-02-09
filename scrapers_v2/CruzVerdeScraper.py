from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import time

class CruzVerdeScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        # Configuración de URLs por categoría
        # La clave es el nombre interno/display, el valor es la URL
        self.categories_config = {
            "Proteinas": "https://www.cruzverde.cl/vitaminas-y-suplementos/nutricion-deportiva/proteinas/",
            "Energia": "https://www.cruzverde.cl/vitaminas-y-suplementos/nutricion-deportiva/energia-y-resistencia/",
            "Snacks y Comida": "https://www.cruzverde.cl/vitaminas-y-suplementos/nutricion-deportiva/barras-proteicas/",
            "Hidratación": "https://www.cruzverde.cl/vitaminas-y-suplementos/nutricion-deportiva/hidratacion/"
        }


        # Selectores identificados
        selectors = {
            "product_card": "ml-new-card-product", # Custom element wrapper
            # Dentro del card:
            "brand": "p.text-gray-dark.uppercase",
            "name": "h2 a.new-ellipsis",
            "image": "at-image img",
            "link": "at-image a",
            
            # Precios
            # Oferta: p.text-green-turquoise (ej: $19.990)
            # Normal: p.line-through (ej: $24.990)
            # Si no hay oferta, a veces usa clases estándar dentro de ml-price-tag-v2
            
            # Stock
            "no_stock_badge": "div:has-text('Sin stock online')", # Pseudo-selector de Playwright muy útil
            
            # Selectores de página de detalle
            "detail_image": "img.ngxImageZoomThumbnail",
            "description": "p >> text=Ayuda a la recuperación"
        }

        super().__init__(base_url, headless, category_urls=list(self.categories_config.values()), selectors=selectors, site_name="Cruz Verde")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.categories_config)} categorías en Cruz Verde...[/green]")
        
        for category_internal_name, category_url in self.categories_config.items():
            print(f"\n[bold blue]Procesando categoría:[/bold blue] {category_internal_name} ({category_url})")
            
            # Navegar a la primera página
            current_url = category_url
            page_num = 1
            has_more_pages = True
            
            try:
                page.goto(current_url, wait_until="networkidle", timeout=60000)
            except Exception as e:
                print(f"[red]Error cargando {current_url}: {e}[/red]")
                continue

            while has_more_pages:
                print(f"--- Página {page_num} ---")
                
                # Esperar a que carguen las cards
                try:
                    page.wait_for_selector("ml-new-card-product", timeout=10000)
                except:
                    print(f"[yellow]No se encontraron productos en la página {page_num}.[/yellow]")
                    break

                # Scroll para asegurar carga de imágenes (lazy load)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1) # Pequeña pausa

                cards = page.locator("ml-new-card-product")
                count = cards.count()
                print(f"  > Encontrados {count} productos.")
                
                if count == 0:
                    break

                for i in range(count):
                    try:
                        card = cards.nth(i)
                        
                        # 1. Extracción de datos básicos
                        # Nombre
                        name = "N/D"
                        name_el = card.locator(self.selectors['name']).first
                        if name_el.count() > 0:
                            raw_name = name_el.inner_text()
                            name = self.clean_text(raw_name)
                        
                        # Marca
                        brand = "N/D"
                        brand_el = card.locator(self.selectors['brand']).first
                        if brand_el.count() > 0:
                            raw_brand = brand_el.inner_text()
                            brand = self.clean_text(raw_brand)

                        
                        # Link
                        link = "N/D"
                        link_el = card.locator(self.selectors['link']).first
                        if link_el.count() > 0:
                            href = link_el.get_attribute("href")
                            if href:
                                link = self.base_url + href if href.startswith('/') else href
                        
                        # Imagen
                        image_url = ""
                        img_el = card.locator(self.selectors['image']).first
                        if img_el.count() > 0:
                            src = img_el.get_attribute("src")
                            if src:
                                image_url = src

                        # 2. Lógica de Stock
                        # Si aparece "Sin stock online", precio es 0
                        is_out_of_stock = False
                        # Buscamos texto específico dentro del card
                        if card.get_by_text("Sin stock online").count() > 0:
                            is_out_of_stock = True

                        # 3. Lógica de Precios
                        price = 0
                        active_discount = False
                        
                        if is_out_of_stock:
                            price = 0
                            price_text = "0"
                        else:
                            # Intentar buscar precio oferta
                            offer_el = card.locator("p.text-green-turquoise").first
                            normal_el = card.locator("p.line-through").first
                            
                            if offer_el.count() > 0:
                                # Hay oferta
                                price_text = offer_el.inner_text()
                                active_discount = True
                            elif normal_el.count() > 0:
                                # A veces solo sale el precio normal tachado si es oferta pero no cargó la otra clase?
                                # No, usualmente si hay tachado hay oferta.
                                # Busquemos cualquier precio semibold/bold si no hay oferta explicita
                                any_price = card.locator("p.font-bold").first
                                if any_price.count() > 0:
                                    price_text = any_price.inner_text()
                                else:
                                    price_text = "0"
                            else:
                                # Precio normal sin oferta
                                # Clases observadas para precio normal simple: font-bold text-gray-dark
                                normal_price_simple = card.locator("p.text-gray-dark.font-bold, p.text-green-turquoise").first 
                                if normal_price_simple.count() > 0:
                                    price_text = normal_price_simple.inner_text()
                                else:
                                    # Fallback genérico: busca cualquier texto con $
                                    all_texts = card.inner_text()
                                    # Regex simple para encontrar precio
                                    match = re.search(r'\$[\d.]+', all_texts)
                                    price_text = match.group(0) if match else "0"

                            # Limpieza precio
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)

                        # 4. Lógica de Categoría Especial (Energia -> Creatinas)
                        final_category = category_internal_name
                        if category_internal_name == "Energia":
                            if "creatina" in name.lower():
                                final_category = "Creatinas"
                            else:
                                final_category = "Vitaminas y Minerales"
                        
                        # 5. DETAIL EXTRACTION (NEW TAB)
                        detail_image_url = image_url  # Fallback a thumbnail
                        sku = "N/D"
                        description = "N/D"
                        
                        if link != "N/D":
                            context = page.context
                            detail_page = None
                            try:
                                detail_page = context.new_page()
                                detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                                
                                # Esperar a que la página cargue completamente
                                detail_page.wait_for_load_state("networkidle", timeout=10000)
                                
                                # Extraer imagen full
                                if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                    img_elem = detail_page.locator(self.selectors['detail_image']).first
                                    img_src = img_elem.get_attribute('src')
                                    if img_src and not ('disclaimer' in img_src or 'logo' in img_src):
                                        detail_image_url = img_src
                                
                                # Extraer descripción - usar evaluate para buscar el párrafo
                                try:
                                    description_js = detail_page.evaluate('''() => {
                                        const allP = Array.from(document.querySelectorAll('p'));
                                        const p = allP.find(p => p.textContent.includes('Ayuda a la recuperación'));
                                        return p ? p.textContent.trim() : null;
                                    }''')
                                    if description_js:
                                        description = description_js
                                except:
                                    pass
                                
                                # Extraer SKU de la URL
                                url_match = re.search(r'/(\d+)\.html', link)
                                if url_match:
                                    sku = url_match.group(1)
                                
                                detail_page.close()
                            except Exception as e:
                                print(f"[red]Error extrayendo detalle de {link}: {e}[/red]")
                                if detail_page:
                                    detail_page.close()
                        
                        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # New Categorization Logic
                        final_subcategory_val = self.clean_text(final_category)
                        cat_info = self.categorizer.classify_product(name, final_category)
                        if cat_info:
                            final_subcategory_val = cat_info['nombre_subcategoria']

                        yield {
                            'date': current_date,
                            'site_name': self.site_name,
                            'category': self.clean_text(final_category),
                            'subcategory': final_subcategory_val,
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

                # Paginación
                # Buscamos el botón de siguiente página
                # El sitio usa parámetros ?start=12&sz=12 usualmente, o paginación por JS
                # En la investigación vimos botones numéricos.
                
                # Intentamos buscar el botón "Siguiente" o un número mayor al actual
                # Un selector robusto para "Siguiente" suele ser un icono de flecha o un aria-label
                
                # Check if there is a 'next' button enabled
                # Cruz Verde usa a veces una flecha >
                next_btn = page.locator("a[aria-label='Ir a la siguiente página'], a.page-next, li.page-item.next a")
                
                # Si no existe aria-label estándar, intentamos buscar por URL parameters
                # Si la URL no cambia, es JS.
                
                # Estrategia alternativa: Buscar el botón de la página actual (clase activa) y el siguiente hermano
                # .pagination .bg-main es la pagina activa
                
                current_page_btn = page.locator(".pagination .bg-main, .pagination .active").first
                if current_page_btn.count() > 0:
                    # Buscar el elemento padre (li o div) y su siguiente hermano
                    # Esto es difícil en playwright directo sin selectores claros.
                    
                    # Vamos a intentar buscar por texto de número
                    next_page_num = page_num + 1
                    next_page_link = page.locator(f".pagination a:has-text('{next_page_num}')")
                    
                    if next_page_link.count() > 0:
                        print(f"Navegando a página {next_page_num}...")
                        next_page_link.first.click()
                        # Esperar carga
                        time.sleep(3)
                        page_num += 1
                        
                        try:
                             page.wait_for_load_state("networkidle")
                        except:
                            pass
                    else:
                         print("No se encontró enlace a la siguiente página. Terminando categoría.")
                         has_more_pages = False
                else:
                    # Fallback: intentar ver si hay un botón "Cargar más"
                    load_more = page.locator("button.load-more, .btn-load-more")
                    if load_more.count() > 0 and load_more.is_visible():
                        print("Clic en Cargar Más...")
                        load_more.click()
                        time.sleep(3)
                        page_num += 1 # Es un scroll infinito simulado
                    else:
                        print("No se detectó paginación estándar. Terminando.")
                        has_more_pages = False
                        
if __name__ == "__main__":
    scraper = CruzVerdeScraper("https://www.cruzverde.cl", headless=True)
    scraper.run()
