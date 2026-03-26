from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from playwright.sync_api import Page
import time
import re
import json
from datetime import datetime


class OutletFitScraper(BaseScraper):
    def __init__(self, headless=False):

        # Taxonomía determinista basada en las categorías del menú de outletfit.cl
        # Excluimos "Ropa y Accesorios" y "Packs Ofertas" (packs se clasifican vía CategoryClassifier)
        category_urls = {
            "Proteinas": [
                {
                    "url": "https://www.outletfit.cl/catalogo/proteinas",
                    "subcategory": "Proteína de Whey"
                }
            ],
            "Creatinas": [
                {
                    "url": "https://www.outletfit.cl/catalogo/creatinas",
                    "subcategory": "Creatina Monohidrato"
                }
            ],
            "Pre Entrenos": [
                {
                    "url": "https://www.outletfit.cl/catalogo/pre-entrenos",
                    "subcategory": "Otros Pre Entrenos"
                }
            ],
            "Vitaminas y Minerales": [
                {
                    "url": "https://www.outletfit.cl/catalogo/vitaminas",
                    "subcategory": "Multivitamínicos"
                }
            ],
            "Snacks y Comida": [
                {
                    "url": "https://www.outletfit.cl/catalogo/snack-proteico",
                    "subcategory": "Barritas Y Snacks Proteicas"
                }
            ],
            "Aminoácidos": [
                {
                    "url": "https://www.outletfit.cl/aminoacidos/bcaa",
                    "subcategory": "BCAAs"
                }
            ],
            "Ganadores de Peso": [
                {
                    "url": "https://www.outletfit.cl/catalogo/ganadores-de-peso",
                    "subcategory": "Ganadores de Peso"
                }
            ],
            "Pro-Hormonales": [
                {
                    "url": "https://www.outletfit.cl/catalogo/pro-hormonales",
                    "subcategory": "Pro-Hormonales"
                }
            ],
            "Bebidas Nutricionales": [
                {
                    "url": "https://www.outletfit.cl/catalogo/bebida-energetica",
                    "subcategory": "Bebidas Energéticas"
                }
            ],
            "Pérdida de Grasa": [
                {
                    "url": "https://www.outletfit.cl/catalogo/termogenicos",
                    "subcategory": "Termogénicos / Quemadores"
                }
            ]
        }

        selectors = {
            'product_card': 'article',
            'product_name': 'h2 a',
            'price': '.price--sale, .price',
            'link': 'h2 a',
            'image': 'img'
        }

        super().__init__(
            "https://www.outletfit.cl",
            headless,
            category_urls,
            selectors,
            site_name="OutletFit"
        )

        self.classifier = CategoryClassifier()

    def _extract_product_links(self, page: Page) -> list:
        """Extrae todos los links de productos de una página de categoría."""
        links = page.evaluate("""
            () => {
                const articles = document.querySelectorAll('article');
                const links = [];
                articles.forEach(a => {
                    const link = a.querySelector('h2 a');
                    if (link && link.href) {
                        links.push(link.href);
                    }
                });
                return links;
            }
        """)
        return links

    def _extract_json_ld(self, page: Page) -> dict:
        """Extrae datos del producto desde JSON-LD embedded en la página de detalle."""
        try:
            json_ld_data = page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (const s of scripts) {
                        try {
                            const data = JSON.parse(s.textContent);
                            // Recorrer recursivamente: puede ser array de arrays o objeto directo
                            function findProduct(obj) {
                                if (!obj) return null;
                                if (Array.isArray(obj)) {
                                    for (const item of obj) {
                                        const found = findProduct(item);
                                        if (found) return found;
                                    }
                                    return null;
                                }
                                if (obj['@type'] === 'Product') return obj;
                                return null;
                            }
                            const found = findProduct(data);
                            if (found) return found;
                        } catch(e) {}
                    }
                    return null;
                }
            """)
            return json_ld_data
        except Exception:
            return None

    def _extract_sku_from_dom(self, page: Page) -> str:
        """Extrae el SKU desde el texto visible en la página de detalle (antes de la marca)."""
        try:
            sku_text = page.evaluate("""
                () => {
                    // El SKU aparece en un span/div con texto "SKU: XXX"
                    const els = document.querySelectorAll('[class*="sku"], [class*="product__sku"]');
                    for (const el of els) {
                        const text = el.textContent.trim();
                        if (text.startsWith('SKU:')) return text.replace('SKU:', '').trim();
                    }
                    // Fallback: buscar en el body
                    const body = document.body.innerText;
                    const match = body.match(/SKU:\\s*([A-Za-z0-9\\-]+)/);
                    return match ? match[1] : '';
                }
            """)
            return sku_text or ""
        except Exception:
            return ""

    def _parse_price(self, offers: dict) -> int:
        """Extrae el precio actual del producto desde la sección offers del JSON-LD."""
        if not offers:
            return 0
        try:
            # En outletfit, "price" tiene el precio de venta (actual)
            price = offers.get("price", 0)
            return int(float(price))
        except (ValueError, TypeError):
            return 0

    def _has_discount(self, offers: dict) -> bool:
        """Determina si el producto tiene descuento comparando price vs lowPrice/highPrice."""
        if not offers:
            return False
        try:
            current = float(offers.get("price", 0))
            original = float(offers.get("highPrice", 0) or offers.get("lowPrice", 0))
            return original > current > 0
        except (ValueError, TypeError):
            return False

    def extract_process(self, page: Page):
        print(f"[green]Iniciando scraping de OutletFit.cl...[/green]")

        for main_category, items in self.category_urls.items():
            for item in items:
                url = item['url']
                deterministic_sub = item['subcategory']

                print(f"\n[bold blue]Categoría:[/bold blue] {main_category} -> {deterministic_sub}")
                print(f"  URL: {url}")

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(1.5)

                    # Extraer links de productos desde el listado
                    product_links = self._extract_product_links(page)
                    print(f"  > Encontrados {len(product_links)} productos en listado")

                    if not product_links:
                        print(f"[yellow]  > Sin productos en {url}[/yellow]")
                        continue

                    for link in product_links:
                        # Deduplicación
                        if link in self.seen_urls:
                            continue
                        self.seen_urls.add(link)

                        try:
                            page.goto(link, wait_until="domcontentloaded", timeout=60000)
                            time.sleep(0.8)

                            # Extraer datos del JSON-LD
                            json_ld = self._extract_json_ld(page)
                            if not json_ld:
                                print(f"[yellow]  >> Sin JSON-LD en {link}, omitiendo[/yellow]")
                                continue


                            # Nombre
                            name = self.clean_text(json_ld.get("name", "N/D").strip())

                            # Marca
                            brand_data = json_ld.get("brand", {})
                            raw_brand = brand_data.get("name", "N/D") if isinstance(brand_data, dict) else "N/D"
                            brand = self.enrich_brand(raw_brand, name, scan_title=True)

                            if brand == "N/D":
                                print(f"[yellow]  >> Marca N/D, omitiendo: {name}[/yellow]")
                                continue

                            # Precio
                            offers = json_ld.get("offers", {})
                            price = self._parse_price(offers)
                            if price <= 0:
                                print(f"[yellow]  >> Precio inválido, omitiendo: {name}[/yellow]")
                                continue

                            # SKU
                            sku = self._extract_sku_from_dom(page)
                            if not sku:
                                sku = json_ld.get("productID", "")

                            # Descripción
                            description = self.clean_description(json_ld.get("description", ""))

                            # Rating y reviews
                            aggregate = json_ld.get("aggregateRating", {})
                            rating = aggregate.get("ratingValue", "0") if aggregate else "0"
                            reviews = aggregate.get("reviewCount", "0") if aggregate else "0"

                            # Descuento
                            active_discount = self._has_discount(offers)

                            # Imagen
                            image_url = json_ld.get("image", "")
                            thumbnail_url = image_url

                            # Descargar imagen a S3
                            site_folder = self.site_name.replace(" ", "_").lower()
                            if image_url:
                                s3_url = self.download_image(image_url, subfolder=site_folder)
                                if s3_url:
                                    image_url = s3_url
                                    thumbnail_url = s3_url

                            # Clasificación heurística para subcategorías refinadas
                            final_category, final_subcategory = self.classifier.classify(
                                title=name,
                                description=description,
                                main_category=main_category,
                                deterministic_subcategory=deterministic_sub,
                                brand=brand
                            )

                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            yield {
                                'date': current_date,
                                'site_name': self.site_name,
                                'category': final_category,
                                'subcategory': final_subcategory,
                                'product_name': name,
                                'brand': brand,
                                'price': price,
                                'link': link,
                                'rating': rating,
                                'reviews': reviews,
                                'active_discount': active_discount,
                                'thumbnail_image_url': thumbnail_url,
                                'image_url': image_url,
                                'sku': sku,
                                'description': description
                            }

                            print(f"  [green]✓[/green] {name} | {brand} | ${price:,} | {final_category}/{final_subcategory}")

                        except Exception as e:
                            print(f"[red]  >> Error en detalle {link}: {e}[/red]")
                            continue

                except Exception as e:
                    print(f"[red]Error crítico en categoría {url}: {e}[/red]")
                    continue


if __name__ == "__main__":
    scraper = OutletFitScraper(headless=True)
    scraper.run()
