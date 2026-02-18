from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re
import time
import unicodedata
from playwright.sync_api import TimeoutError

class SuplementosMayoristasScraper(BaseScraper):
    def __init__(self, base_url, headless=False):
        # Mapeo directo de las URLs proporcionadas
        self.categories_config = {
            "Proteinas": [
                { "url": "https://www.suplementosmayoristas.cl/proteinas/whey-protein", "subcategory": "Proteína de Whey" },
                { "url": "https://www.suplementosmayoristas.cl/proteinas/whey-Isolate", "subcategory": "Proteína Aislada" },
                { "url": "https://www.suplementosmayoristas.cl/proteinas/clear-whey-isolate", "subcategory": "Proteína de Whey" },
                { "url": "https://www.suplementosmayoristas.cl/proteinas/proteinas-veganas", "subcategory": "Proteína Vegana" }
            ],
            "Creatinas": [
                { "url": "https://www.suplementosmayoristas.cl/creatinas/micronizada", "subcategory": "Micronizada" },
                { "url": "https://www.suplementosmayoristas.cl/creatinas/monohidratada", "subcategory": "Creatina Monohidrato" }
            ],
            "Perdida de Grasa": [
                { "url": "https://www.suplementosmayoristas.cl/quemadores", "subcategory": "Quemadores" }
            ],
            "Pre Entrenos": [
                 { "url": "https://www.suplementosmayoristas.cl/pre-entreno", "subcategory": "Pre Entreno" }
            ],
            "Ganadores de Peso": [
                { "url": "https://www.suplementosmayoristas.cl/ganadores-de-masa-muscular", "subcategory": "Ganadores De Peso" }
            ],
            "Aminoacidos y BCAA": [
                { "url": "https://www.suplementosmayoristas.cl/aminoacidos/bcaa", "subcategory": "BCAAs" },
                { "url": "https://www.suplementosmayoristas.cl/aminoacidos/eaa", "subcategory": "EAAs (Esenciales)" },
                { "url": "https://www.suplementosmayoristas.cl/aminoacidos/hmb-y-zma", "subcategory": "DETECTAR_HMB_ZMA" },
                { "url": "https://www.suplementosmayoristas.cl/aminoacidos/especificos", "subcategory": "Otros Aminoacidos y BCAA" }
            ]
        }
        
        # VTEX Selectors
        selectors = {
            "product_card": "section.vtex-product-summary-2-x-container", # Container más robusto
            "link_container": "a.vtex-product-summary-2-x-clearLink", # Link principal
            
            "brand": ".vtex-store-components-3-x-productBrandName",
            "name": ".vtex-product-summary-2-x-productBrand",
            "image": "img.vtex-product-summary-2-x-image",
            
            # Precio: a veces está dividido en partes, buscamos el contenedor
            "price_container": ".vtex-product-price-1-x-sellingPriceValue, .vtex-product-price-1-x-currencyContainer",
            
            # Stock: Botón "Agregar al carro" vs "Agotado"
            "add_to_cart_btn": ".vtex-add-to-cart-button-0-x-buttonDataContainer",
            "stock_badge": ".vtex-product-summary-2-x-element", # Textos genéricos en el card
            
            # Paginación "Mostrar más"
            "show_more_btn": ".vtex-search-result-3-x-buttonShowMore button",
            
            # Selectores de página de detalle
            "detail_image": ".vtex-store-components-3-x-productImageTag",
            "description": ".vtex-store-components-3-x-productDescriptionText"
        }

        super().__init__(base_url, headless, category_urls=self.categories_config, selectors=selectors, site_name="SuplementosMayoristas")

    def extract_process(self, page):
        print(f"[green]Iniciando scraping Determinista (V2) de Suplementos Mayoristas...[/green]")
        
        for main_category, items in self.categories_config.items():
            for item in items:
                url = item['url']
                deterministic_sub = item['subcategory']
                
                print(f"\n[bold blue]Procesando:[/bold blue] {main_category} -> {deterministic_sub} ({url})")
                
                try:
                    page.goto(url, wait_until="networkidle", timeout=60000)
                except Exception as e:
                    print(f"[red]Error cargando {url}: {e}[/red]")
                    continue

                # Lógica de "Mostrar más" (Scroll Infinito manual)
                while True:
                    # Esperar carga inicial de productos
                    try:
                        page.wait_for_selector(self.selectors['link_container'], timeout=10000)
                    except:
                        print("[yellow]No se encontraron productos iniciales.[/yellow]")
                        break

                    # Intentar clic en "Mostrar más" repetidamente hasta que no haya más
                    show_more = page.locator(self.selectors['show_more_btn'])
                    if show_more.count() > 0 and show_more.is_visible():
                        print("Clic en 'Mostrar más'...")
                        try:
                            show_more.click()
                            time.sleep(3) # Esperar carga de nuevos productos
                            page.wait_for_load_state("networkidle")
                            continue # Volver a chequear si hay otro botón
                        except Exception as e:
                            print(f"[yellow]Error clicks mostrar más: {e}, asumiendo fin de lista.[/yellow]")
                            break
                    else:
                        break # Ya no hay botón mostrar más, estamos listos para extraer todo

                # Una vez cargado todo, extraemos
                print("Extrayendo productos cargados...")
                
                # Scroll al fondo para asegurar lazy load de imágenes
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)

                # Selectores a usar
                cards = page.locator(self.selectors['link_container'])
                count = cards.count()
                print(f"  > Encontrados {count} productos totales en la vista.")

                for i in range(count):
                    try:
                        card = cards.nth(i)
                        
                        # Data extraction
                        # Link
                        link = "N/D"
                        href = card.get_attribute("href")
                        if href:
                            link = self.base_url + href if href.startswith('/') else href

                        # Deduplication Check
                        if link != "N/D" and link in self.seen_urls:
                            print(f"[yellow]  >> Producto duplicado omitido.[/yellow]")
                            continue
                        if link != "N/D":
                            self.seen_urls.add(link)

                        # Name
                        name = "N/D"
                        name_el = card.locator(self.selectors['name']).first
                        if name_el.count() > 0:
                            raw_name = name_el.inner_text()
                            name = self.clean_text(raw_name)

                        # Brand
                        brand = "N/D"
                        brand_el = card.locator(self.selectors['brand']).first
                        if brand_el.count() > 0:
                            raw_brand = brand_el.inner_text()
                            brand = self.clean_text(raw_brand)


                        # Image
                        image_url = ""
                        img_el = card.locator(self.selectors['image']).first
                        if img_el.count() > 0:
                            src = img_el.get_attribute("src")
                            if src:
                                image_url = src

                        # Price & Stock
                        # VTEX logic: Check availability container first
                        price = 0
                        
                        # Check for "Agotado" text anywhere in the card
                        card_text = card.inner_text().lower()
                        is_out_of_stock = "agotado" in card_text or "sin stock" in card_text or "avísame" in card_text
                        
                        # Also check if price container exists. Sometimes out of stock hides price.
                        price_el = card.locator(self.selectors['price_container']).first
                        
                        if is_out_of_stock:
                            price = 0
                        elif price_el.count() > 0:
                            price_text = price_el.inner_text()
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)
                        else:
                            # No stock msg but no price? treat as 0 or check if it's a layout issue
                            price = 0

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
                                detail_page.wait_for_load_state("networkidle")  # Wait for dynamic content
                                
                                # Extraer imagen full
                                if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                    src = detail_page.locator(self.selectors['detail_image']).first.get_attribute('src')
                                    if src:
                                        detail_image_url = src
                                
                                # Extraer descripción con espera explícita
                                try:
                                    detail_page.wait_for_selector(self.selectors['description'], timeout=5000)
                                    if detail_page.locator(self.selectors['description']).count() > 0:
                                        description = detail_page.locator(self.selectors['description']).first.inner_text().strip()
                                except:
                                    # Fallback a contenedor si falla el texto directo
                                    try:
                                        container = ".vtex-store-components-3-x-productDescriptionContainer"
                                        if detail_page.locator(container).count() > 0:
                                            description = detail_page.locator(container).first.inner_text().strip()
                                    except:
                                        pass
                                
                                # Extraer datos estructurados (JSON-LD) para Brand y SKU
                                try:
                                    json_data = detail_page.evaluate('''() => {
                                        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                        for (const script of scripts) {
                                            try {
                                                const text = script.innerText;
                                                const data = JSON.parse(text);
                                                // Handle array or object
                                                const items = Array.isArray(data) ? data : [data];
                                                for (const item of items) {
                                                    if (item['@type'] === 'Product') {
                                                        return {
                                                            brand: item.brand ? (item.brand.name || item.brand) : null,
                                                            sku: item.sku || item.mpn || null
                                                        };
                                                    }
                                                }
                                            } catch (e) {}
                                        }
                                        return null;
                                    }''')
                                    
                                    if json_data:
                                        if json_data.get('brand'):
                                            brand = self.clean_text(json_data['brand'])
                                        if json_data.get('sku') and json_data.get('sku') != "N/D":
                                            sku = json_data['sku']
                                except Exception as e:
                                    pass

                                # Fallback: Extraer Brand visualmente si aún es N/D
                                if brand == "N/D":
                                    try:
                                        brand_el = detail_page.locator(".vtex-store-components-3-x-productBrandName").first
                                        if brand_el.count() > 0:
                                            raw_brand = brand_el.inner_text()
                                            brand = self.clean_text(raw_brand)
                                    except:
                                        pass

                                # Fallback: Extraer SKU del script JSON si no se encontró en JSON-LD
                                if sku == "N/D":
                                    try:
                                        sku_script = detail_page.evaluate('''() => {
                                            const scripts = Array.from(document.querySelectorAll('script'));
                                            for (const script of scripts) {
                                                const text = script.textContent;
                                                if (text.includes('"sku"') || text.includes('"skuId"')) {
                                                    const skuMatch = text.match(/"(?:sku|skuId)"\\s*:\\s*"([^"]+)"/);
                                                    if (skuMatch) return skuMatch[1];
                                                }
                                            }
                                            return null;
                                        }''')
                                        if sku_script:
                                            sku = sku_script
                                    except:
                                        pass
                                
                                detail_page.close()
                            except Exception as e:
                                print(f"[red]Error extrayendo detalle de {link}: {e}[/red]")
                                if detail_page:
                                    detail_page.close()
                        
                        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # --- IMPLEMENTACIÓN DE DESCARGA ---
                        site_folder = self.site_name.replace(" ", "_").lower()
                        if image_url:
                            local_thumb = self.download_image(image_url, subfolder=site_folder)
                            if local_thumb: image_url = local_thumb
                        if detail_image_url:
                            local_img = self.download_image(detail_image_url, subfolder=site_folder)
                            if local_img: detail_image_url = local_img

                        # --- LOGICA HEURÍSTICA Y FALLBACKS ---
                        # Usar variables temporales para no contaminar el loop
                        final_category = main_category
                        final_sub = deterministic_sub
                        
                        def _normalize(text):
                            nfd = unicodedata.normalize('NFD', text)
                            return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')

                        # 1. Packs (Global)
                        title_lower = _normalize(name.lower())
                        if "pack" in title_lower or "paquete" in title_lower or "combo" in title_lower:
                            final_category = "Packs"
                            final_sub = "Packs"

                        # 2. Heurística para HMB/ZMA (Categoría Aminoacidos)
                        elif final_sub == "DETECTAR_HMB_ZMA":
                            name_lower = _normalize(str(name).lower())
                            desc_lower = _normalize(str(description).lower())
                            text_to_search = name_lower + " " + desc_lower
                            
                            # PRIORIDAD A HMB
                            if "hmb" in text_to_search:
                                final_category = "Aminoacidos y BCAA"
                                final_sub = "Otros Aminoacidos y BCAA"
                            # ZMA explicito (en nombre o descripcion)
                            elif "zma" in text_to_search or "zmar" in text_to_search:
                                final_category = "Vitaminas y Minerales"
                                final_sub = "Multivitamínicos"
                            # Magnesio o Zinc SOLO si estan en el nombre (para evitar falsos positivos por descripcion)
                            elif "magnesio" in name_lower or "zinc" in name_lower:
                                final_category = "Vitaminas y Minerales"
                                final_sub = "Multivitamínicos"
                            else:
                                # Fallback seguro: Si no es HMB ni ZMA, sigue siendo Aminoacido (probablemente del grupo Especificos)
                                final_category = "Aminoacidos y BCAA"
                                final_sub = "Otros Aminoacidos y BCAA"

                        yield {
                            'date': current_date,
                            'site_name': self.site_name,
                            'category': self.clean_text(final_category),
                            'subcategory': final_sub,
                            'product_name': name,
                            'brand': self.enrich_brand(brand, name),
                            'price': price,
                            'link': link,
                            'rating': "0",
                            'reviews': "0",
                            'active_discount': False, # User said no discounts for now
                            'thumbnail_image_url': image_url,
                            'image_url': detail_image_url,
                            'sku': sku,
                            'description': description
                        }

                    except Exception as e:
                        print(f"[red]Error extrayendo producto individual: {e}[/red]")
                        continue

if __name__ == "__main__":
    scraper = SuplementosMayoristasScraper("https://www.suplementosmayoristas.cl", headless=True)
    scraper.run()
