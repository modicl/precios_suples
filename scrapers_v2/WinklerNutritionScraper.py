# Scraper para WinklerNutrition.cl (Shopify — migrado desde WooCommerce en 2026)
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from playwright.sync_api import Page
from datetime import datetime
from rich import print
import re


class WinklerNutritionScraper(BaseScraper):
    def __init__(self, headless=False):

        # Winkler Nutrition migró a Shopify: todos los productos en una sola colección.
        # CategoryClassifier se encarga de clasificar por nombre/descripción.
        category_urls = {
            "Todos los productos": [
                {
                    "url": "https://winklernutrition.cl/collections/nuestros-productos",
                    "subcategory": ""
                }
            ]
        }

        selectors = {
            # Grid de productos (Shopify tema custom)
            "product_card": ".card-product",
            "product_name": "a.x-card-title",
            # Precio
            "price_sale": "span.price-sale",
            "price_regular": "p.price",
            "price_target": "div.target-price",        # entero en centavos (ej: 8499000 → $84.990)
            "price_compare": "s.price-compare",
            # Thumbnail
            "thumbnail": "img",
            # Paginación Shopify: ?page=N
            "next_button": "a[href*='?page=']",
            # Detalle
            "detail_sku": ".sku, [class*='sku']",
            "detail_desc": ".product__description, [class*='product-description'], #tab-description",
            "detail_image": ".woocommerce-product-gallery__image img",  # fallback; OG es primario
        }

        super().__init__(
            base_url="https://winklernutrition.cl",
            headless=headless,
            category_urls=category_urls,
            selectors=selectors,
            site_name="Winkler Nutrition"
        )

        self.classifier = CategoryClassifier()

    # ------------------------------------------------------------------
    # Inferencia de categoría principal desde el nombre del producto
    # ------------------------------------------------------------------

    _CATEGORY_KEYWORDS = [
        ("Proteinas",           ["proteín", "whey", "isolate", "hidroliza", "caseín", "vegan shake", "vegan protein", "iso win", "pro win", "cooking"]),
        ("Creatinas",           ["creatina"]),
        ("Pre Entrenos",        ["pre-entr", "pre entr", "kreator", "cafeína", "coffit", "beta alan", "citrulina", "óxido nítrico"]),
        ("Aminoacidos y BCAA",  ["aminoácid", "bcaa", "glutamin", "l-citrulina", "arginina", "taurina"]),
        ("Vitaminas y Minerales", ["vitamina", "mineral", "omega", "zinc", "magnesio", "hierro", "calcio", "multivitam", "ashwagandha", "hongo", "zma", "nad", "nmn", "resveratrol", "spirulina", "artichoke", "hemp", "cápsulas"]),
        ("Quemadores",          ["quemador", "fat burn", "termogénic", "l-carnitin"]),
        ("Snacks y Comida",     ["barrita", "snack", "manteca", "maní", "granola", "protein sure"]),
        ("Bebidas Nutricionales", ["bebida", "energética", "electrolito", "batido", "shake líquid", "líquida"]),
        ("Pro Hormonales",      ["pro-hormonal", "pro hormonal", "hormonal", "testosteron"]),
        ("Ganadores",           ["gainer", "ganador", "mass"]),
        ("Packs",               ["pack "]),
    ]

    def _infer_main_category(self, title: str) -> str:
        """Mapea el nombre del producto a una categoría del sistema reconocida por CategoryClassifier."""
        t = title.lower()
        for category, keywords in self._CATEGORY_KEYWORDS:
            if any(kw in t for kw in keywords):
                return category
        return "Vitaminas y Minerales"  # fallback conservador para productos de salud

    # ------------------------------------------------------------------
    # Helpers de precio (Shopify)
    # ------------------------------------------------------------------

    def _extract_price(self, card):
        """
        Extrae precio final y si tiene descuento activo desde la card de producto.
        Shopify: precio de venta en span.price-sale, precio normal en p.price.
        div.target-price contiene el precio como entero en centavos (más confiable).
        """
        price = 0
        active_discount = False

        # Detectar si tiene descuento activo
        sale_el = card.locator(self.selectors["price_sale"])
        if sale_el.count() > 0:
            active_discount = True

        # Fuente primaria: target-price (entero en centavos, sin formato)
        target_el = card.locator(self.selectors["price_target"])
        if target_el.count() > 0:
            raw = target_el.first.inner_text().strip()
            if raw and raw.isdigit():
                price = int(raw) // 100   # centavos → pesos chilenos
                return price, active_discount

        # Fallback: parsear texto visible
        if active_discount:
            raw = sale_el.first.inner_text()
        else:
            reg_el = card.locator(self.selectors["price_regular"])
            raw = reg_el.first.inner_text() if reg_el.count() > 0 else ""

        clean = re.sub(r"[^\d]", "", raw)
        if clean:
            price = int(clean)

        return price, active_discount

    # ------------------------------------------------------------------
    # Lógica principal
    # ------------------------------------------------------------------

    def extract_process(self, page: Page):
        print(f"[green]Iniciando scraping de Winkler Nutrition (Shopify)...[/green]")
        context = page.context

        base_collection_url = "https://winklernutrition.cl/collections/nuestros-productos"
        page_number = 1

        while True:
            collection_url = (
                base_collection_url if page_number == 1
                else f"{base_collection_url}?page={page_number}"
            )
            print(f"\n[bold blue]Página {page_number}:[/bold blue] {collection_url}")

            try:
                page.goto(collection_url, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                print(f"[red]Error navegando a {collection_url}: {e}[/red]")
                break

            # Scroll para activar lazy-load
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1000)

            try:
                page.wait_for_selector(self.selectors["product_card"], timeout=7000)
            except Exception:
                print(f"[red]No se encontraron productos en página {page_number}.[/red]")
                break

            cards = page.locator(self.selectors["product_card"])
            count = cards.count()
            print(f"  > Encontrados {count} productos.")

            if count == 0:
                break

            for i in range(count):
                card = cards.nth(i)
                current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # ── Link y nombre (a.x-card-title sirve para ambos) ────────
                link = "N/D"
                title = ""
                name_el = card.locator(self.selectors["product_name"])
                if name_el.count() > 0:
                    href = name_el.first.get_attribute("href")
                    if href:
                        link = href if href.startswith("http") else self.base_url + href
                    title = self.clean_text(name_el.first.inner_text())

                if not title or title.upper() == "N/D":
                    print(f"[yellow]  >> Saltando producto sin nombre en {link}[/yellow]")
                    continue

                # ── Deduplicación ───────────────────────────────────────────
                if link != "N/D" and link in self.seen_urls:
                    continue
                if link != "N/D":
                    self.seen_urls.add(link)

                # ── Precio ──────────────────────────────────────────────────
                price, active_discount = self._extract_price(card)

                # ── Thumbnail (Shopify CDN — quitar parámetro width) ────────
                thumbnail_url = ""
                img_el = card.locator("img")
                if img_el.count() > 0:
                    src = img_el.first.get_attribute("src") or ""
                    if src:
                        # Shopify CDN puede devolver URLs protocol-relative (//...)
                        if src.startswith("//"):
                            src = "https:" + src
                        # Quitar &width=N o ?width=N manteniendo ?v=... si existe
                        thumbnail_url = re.sub(r"[&?]width=\d+", "", src).rstrip("?&")

                # ── Detalle ──────────────────────────────────────────────────
                image_url = ""
                sku = ""
                description = ""

                if link != "N/D":
                    detail_page = None
                    try:
                        detail_page = context.new_page()
                        detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)

                        # Imagen: Open Graph (funciona igual en Shopify)
                        try:
                            og = detail_page.locator('meta[property="og:image"]').first.get_attribute("content")
                            if og:
                                image_url = og
                        except Exception:
                            pass

                        # SKU
                        try:
                            sku_el = detail_page.locator(self.selectors["detail_sku"])
                            if sku_el.count() > 0:
                                sku = sku_el.first.inner_text().replace("SKU:", "").strip()
                        except Exception:
                            pass

                        # Descripción
                        try:
                            desc_el = detail_page.locator(self.selectors["detail_desc"])
                            if desc_el.count() > 0:
                                description = self.clean_description(desc_el.first.inner_text())
                        except Exception:
                            pass

                        detail_page.close()

                    except Exception as e:
                        print(f"[yellow]Error en detalle {link}: {e}[/yellow]")
                        if detail_page:
                            try:
                                detail_page.close()
                            except Exception:
                                pass

                # ── Descarga de imágenes (S3 / local) ──────────────────────
                site_folder = self.site_name.replace(" ", "_").lower()
                if thumbnail_url:
                    local_thumb = self.download_image(thumbnail_url, subfolder=site_folder)
                    if local_thumb:
                        thumbnail_url = local_thumb
                if image_url:
                    local_img = self.download_image(image_url, subfolder=site_folder)
                    if local_img:
                        image_url = local_img

                # ── Clasificación (CategoryClassifier por nombre+desc) ──────
                main_cat = self._infer_main_category(title)
                final_category, final_sub = self.classifier.classify(
                    title, description, main_cat, "", "Winkler Nutrition"
                )

                yield {
                    "date": current_date,
                    "site_name": self.site_name,
                    "category": self.clean_text(final_category),
                    "subcategory": final_sub,
                    "product_name": title,
                    "brand": "Winkler Nutrition",
                    "price": price,
                    "link": link,
                    "rating": "0",
                    "reviews": "0",
                    "active_discount": active_discount,
                    "thumbnail_image_url": thumbnail_url,
                    "image_url": image_url,
                    "sku": sku,
                    "description": description,
                }

            # ── Paginación: verificar si existe página siguiente ────────────
            next_page_link = page.locator(f'a[href*="?page={page_number + 1}"]')
            if next_page_link.count() > 0:
                print(f"  > Avanzando a página {page_number + 1}...")
                page_number += 1
                page.wait_for_timeout(1000)
            else:
                print("  > No hay más páginas.")
                break


if __name__ == "__main__":
    scraper = WinklerNutritionScraper(headless=True)
    scraper.run()
