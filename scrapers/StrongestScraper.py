# Scraper para la pagina web Strongest.cl
from BaseScraper import BaseScraper
from rich import print
from datetime import datetime
import re

class StrongestScraper(BaseScraper):
    def __init__(self):
        super().__init__(
            base_url="https://www.strongest.cl",
            site_name="Strongest",
            headless=True
        )
        
        # URLs Cateogorias
        self.category_urls = {
            "Proteinas": "https://www.strongest.cl/collection/proteinas",
            # "Creatinas": "https://www.strongest.cl/collection/creatinas",
            # "Vitaminas": "https://www.strongest.cl/collection/salud-y-bienestar",
            # "Pre Entrenos": "https://www.strongest.cl/collection/pre-entrenos",
            # "Ganadores de Peso": "https://www.strongest.cl/collection/ganadores-de-masa",
            # "Aminoacidos y BCAA": "https://www.strongest.cl/collection/aminoacidos-bcaa",
            # "Perdida de Grasa": "https://www.strongest.cl/collection/termogenicos",
            # "Snacks y Comida": "https://www.strongest.cl/collection/snacks",
            # "Ofertas": "https://www.strongest.cl/collection/ofertas-y-precios-bajos"

        }
        
        self.selectors = {
            "product_card": ".bs-collection__product",
            "product_name": ".bs-collection__product-title",
            "price": ".bs-collection__product-final-price", 
            "old_price": ".bs-collection__old-price", 
            "link": "a.bs-collection__product-info",
            "thumbnail": ".bs-collection__product__img img, .bs-collection__product-image img",
            "brand": ".bs-collection__product-brand"
        }

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de {len(self.category_urls)} categorías en Strongest...[/green]")
        context = page.context

        for category, url in self.category_urls.items():
            
            # Subcategory is not really used here, map to empty or same as category
            subcategory_name = category 
            
            print(f"\n[bold blue]Procesando categoría:[/bold blue] {category} ({url})")
            
            try:
                page.goto(url, timeout=60000, wait_until='domcontentloaded')
                
                page_num = 1
                while True:
                    print(f"--- Página {page_num} ---")
                    
                    try:
                        page.wait_for_selector(self.selectors['product_card'], timeout=5000)
                    except:
                        print(f"[red]No se encontraron productos en la página {page_num} o fin de lista.[/red]")
                        break
                    
                    cards = page.locator(self.selectors['product_card'])
                    count = cards.count()
                    
                    if count == 0:
                        print("  Grilla vacía.")
                        break
                        
                    print(f"  > Encontrados {count} productos.")
                    
                    for i in range(count):
                        card = cards.nth(i)
                        current_date = datetime.now().strftime("%Y-%m-%d")
                        
                        # --- Grid Extraction ---
                        
                        # Title
                        title = "N/D"
                        if card.locator(self.selectors['product_name']).count() > 0:
                            raw_title = card.locator(self.selectors['product_name']).first.inner_text()
                            title = self.clean_text(raw_title)
                        
                        # Link

                        link = "N/D"
                        if card.locator(self.selectors['link']).count() > 0:
                            href = card.locator(self.selectors['link']).first.get_attribute("href")
                            if href:
                                link = href if href.startswith('http') else f"https://www.strongest.cl{href}"
                        
                        # Thumbnail
                        thumbnail_url = ""
                        if card.locator(self.selectors['thumbnail']).count() > 0:
                            t_src = card.locator(self.selectors['thumbnail']).first.get_attribute("src")
                            if t_src:
                                thumbnail_url = t_src if t_src.startswith('http') else f"https:{t_src}"

                        # Price
                        price = 0
                        price_elem = card.locator(self.selectors['price'])
                        if price_elem.count() > 0:
                            price_text = price_elem.first.inner_text()
                            clean_price = re.sub(r'[^\d]', '', price_text)
                            if clean_price:
                                price = int(clean_price)
                        
                        # Active Discount
                        active_discount = False
                        if card.locator(self.selectors['old_price']).count() > 0:
                            active_discount = True

                        # Brand Extraction
                        brand = "N/D"
                        if card.locator(self.selectors['brand']).count() > 0:
                            raw_brand = card.locator(self.selectors['brand']).first.inner_text()
                            brand = self.clean_text(raw_brand)
                        
                        # --- Detail Extraction (Multi-tab) ---
                        image_url = ""
                        sku = ""
                        description = ""

                        if link != "N/D":
                            try:
                                detail_page = context.new_page()
                                detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)
                                
                                # 1. Main Image - Priority: Open Graph > JSON-LD > DOM
                                
                                # Open Graph
                                try:
                                    og_img = detail_page.locator('meta[property="og:image"]').first.get_attribute('content')
                                    if og_img:
                                        image_url = og_img
                                except: pass

                                # JSON-LD
                                if not image_url:
                                    try:
                                        json_img = detail_page.evaluate('''() => {
                                            const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                            for (const s of scripts) {
                                                try {
                                                    const d = JSON.parse(s.innerText);
                                                    if ((d['@type'] === 'Product' || d['@type'] === 'ProductGroup') && d.image) 
                                                        return Array.isArray(d.image) ? d.image[0] : d.image;
                                                } catch(e){}
                                            }
                                            return null;
                                        }''')
                                        if json_img:
                                            image_url = json_img
                                    except: pass

                                # DOM Fallback
                                if not image_url:
                                    img_el = detail_page.locator('.bs-product-gallery__img img, .bs-product__image img, .bs-img-square img').first
                                    if img_el.count() > 0:
                                        src = img_el.get_attribute("src")
                                        if src:
                                            image_url = src if src.startswith('http') else f"https:{src}"
                                
                                # 2. SKU
                                sku_el = detail_page.locator('.bs-product__sku').first
                                if sku_el.count() > 0:
                                    sku_raw = sku_el.inner_text().strip()
                                    sku = sku_raw.replace("SKU:", "").strip()
                                
                                # 3. Description
                                desc_el = detail_page.locator('.bs-product__description').first
                                if desc_el.count() > 0:
                                    description = desc_el.inner_text().strip()
                                    
                                detail_page.close()
                                
                            except Exception as e:
                                print(f"[yellow]Error loading details for {link}: {e}[/yellow]")
                                try: detail_page.close()
                                except: pass

                        # --- IMPLEMENTACIÓN DE DESCARGA ---
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
                            'category': self.clean_text(category),
                            'subcategory': self.clean_text(subcategory_name),
                            'product_name': title,
                            'brand': brand,

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

                    # Pagination
                    # Strongest uses ?page=N
                    # We can click next if exists or just navigate URL
                    
                    next_btn = page.locator('a[aria-label="Next page"], .pagination .next, a:has-text("Siguiente")')
                    # Or simpler: check if we just processed less than expected or check button
                    
                    # Safer: Construct next URL or look for next button href
                    # The original scraper used URL logic, let's try to stick to button if possible or URL
                    
                    # Try to find next button href
                    next_href = None
                    if next_btn.count() > 0:
                         href = next_btn.first.get_attribute("href")
                         if href: next_href = href
                    
                    if next_href:
                        print(f"  > Avanzando a página {page_num + 1}...")
                        page.goto(self.base_url + next_href if next_href.startswith('/') else next_href)
                        page_num += 1
                        page.wait_for_timeout(2000)
                    else:
                        # Fallback to URL increment if no button found but maybe it's infinite scroll or js?
                        # Strongest is usually paginated. If no next button, we assume end.
                        print("  > No hay más páginas (o botón no encontrado).")
                        break

            except Exception as e:
                print(f"[red]Error procesando categoría {category}: {e}[/red]")

if __name__ == "__main__":
    scraper = StrongestScraper()
    scraper.run()