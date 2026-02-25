# Scraper para WinklerNutrition.cl (WooCommerce)
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from playwright.sync_api import Page
from datetime import datetime
from rich import print
import re


class WinklerNutritionScraper(BaseScraper):
    def __init__(self, headless=False):

        # Mapeo determinista: Categoría del sitio → categoría/subcategoría del sistema
        # Basado en las categorías encontradas en el menú de navegación de winklernutrition.cl
        category_urls = {
            "Proteinas": [
                {
                    "url": "https://winklernutrition.cl/categoria-producto/proteinas-wk/",
                    "subcategory": "Proteínas"   # CategoryClassifier refinará
                }
            ],
            "Snacks y Comida": [
                {
                    "url": "https://winklernutrition.cl/categoria-producto/barritas-wk/",
                    "subcategory": "Barritas Y Snacks Proteicas"
                }
            ],
            "Pre Entrenos": [
                {
                    "url": "https://winklernutrition.cl/categoria-producto/pre-entrenamiento-wk/",
                    "subcategory": "Otros Pre Entrenos"  # CategoryClassifier refinará
                }
            ],
            "Aminoacidos y BCAA": [
                {
                    "url": "https://winklernutrition.cl/categoria-producto/aminoacidos-wk/",
                    "subcategory": "Complejos de Aminoácidos"  # CategoryClassifier refinará
                }
            ],
            "Vitaminas y Minerales": [
                {
                    "url": "https://winklernutrition.cl/categoria-producto/salud-y-bienestar-wk/",
                    "subcategory": "Bienestar General"  # CategoryClassifier refinará
                }
            ],
            "Pro Hormonales": [
                {
                    "url": "https://winklernutrition.cl/categoria-producto/pro-hormonales-wk/",
                    "subcategory": "Pro Hormonales"
                }
            ],
            "Bebidas Nutricionales": [
                {
                    "url": "https://winklernutrition.cl/categoria-producto/energia-wk/",
                    "subcategory": "Bebidas Energéticas"  # CategoryClassifier refinará
                }
            ],
        }

        selectors = {
            # Grid de productos (WooCommerce con tema Wolf/Deadlift)
            "product_card": "article.product",
            "product_name": ".woocommerce-loop-product__title",
            "link": "a.entry-link-mask",
            "thumbnail": "img.attachment-woocommerce_thumbnail",
            # Paginación WooCommerce estándar
            "next_button": "a.next.page-numbers",
            # Detalle
            "detail_sku": ".sku",
            "detail_desc": "#tab-description",
            "detail_short_desc": ".woocommerce-product-details__short-description",
            "detail_image": ".woocommerce-product-gallery__image img",
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
    # Helpers de precio WooCommerce
    # ------------------------------------------------------------------

    def _extract_price(self, card):
        """
        Extrae precio final y si tiene descuento activo desde la card de producto.
        WooCommerce: precio en oferta está dentro de <ins>, precio tachado en <del>.
        """
        price = 0
        active_discount = False

        # Precio en oferta (ins)
        ins_el = card.locator("ins .woocommerce-Price-amount bdi")
        if ins_el.count() > 0:
            active_discount = True
            raw = ins_el.first.inner_text()
            clean = re.sub(r"[^\d]", "", raw)
            if clean:
                price = int(clean)
            return price, active_discount

        # Precio normal (sin descuento)
        reg_el = card.locator(".price .woocommerce-Price-amount bdi")
        if reg_el.count() > 0:
            raw = reg_el.first.inner_text()
            clean = re.sub(r"[^\d]", "", raw)
            if clean:
                price = int(clean)

        return price, active_discount

    # ------------------------------------------------------------------
    # Lógica principal
    # ------------------------------------------------------------------

    def extract_process(self, page: Page):
        print(f"[green]Iniciando scraping de Winkler Nutrition...[/green]")
        context = page.context

        for main_category, items in self.category_urls.items():
            for item in items:
                url = item["url"]
                deterministic_sub = item["subcategory"]

                print(f"\n[bold blue]Procesando:[/bold blue] {main_category} -> {deterministic_sub} ({url})")

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)

                    page_number = 1
                    while True:
                        print(f"--- Página {page_number} ---")

                        # Scroll para activar lazy-load
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(1000)

                        try:
                            page.wait_for_selector(self.selectors["product_card"], timeout=7000)
                        except Exception:
                            print(f"[red]No se encontraron productos en {url} (página {page_number}).[/red]")
                            break

                        cards = page.locator(self.selectors["product_card"])
                        count = cards.count()
                        print(f"  > Encontrados {count} productos.")

                        for i in range(count):
                            card = cards.nth(i)
                            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                            # ── Link ───────────────────────────────────────────
                            link = "N/D"
                            link_el = card.locator(self.selectors["link"])
                            if link_el.count() > 0:
                                href = link_el.first.get_attribute("href")
                                if href:
                                    link = href if href.startswith("http") else self.base_url + href

                            # ── Deduplicación ──────────────────────────────────
                            if link != "N/D" and link in self.seen_urls:
                                continue
                            if link != "N/D":
                                self.seen_urls.add(link)

                            # ── Título ─────────────────────────────────────────
                            title = ""
                            name_el = card.locator(self.selectors["product_name"])
                            if name_el.count() > 0:
                                title = self.clean_text(name_el.first.inner_text())

                            # Saltamos productos sin nombre válido
                            if not title or title.upper() == "N/D":
                                print(f"[yellow]  >> Saltando producto sin nombre en {link}[/yellow]")
                                continue

                            # ── Precio ─────────────────────────────────────────
                            price, active_discount = self._extract_price(card)

                            # ── Thumbnail ──────────────────────────────────────
                            thumbnail_url = ""
                            img_el = card.locator(self.selectors["thumbnail"])
                            if img_el.count() > 0:
                                src = img_el.first.get_attribute("src")
                                if src:
                                    # Limpiar sufijo de tamaño WooCommerce (-412x491)
                                    clean = re.sub(r"-\d+x\d+(\.\w+)$", r"\1", src.split("?")[0])
                                    thumbnail_url = clean

                            # ── Detalle ────────────────────────────────────────
                            image_url = ""
                            sku = ""
                            description = ""

                            if link != "N/D":
                                detail_page = None
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=40000)

                                    # Imagen: Open Graph (más confiable en WooCommerce)
                                    try:
                                        og = detail_page.locator('meta[property="og:image"]').first.get_attribute("content")
                                        if og:
                                            image_url = og
                                    except Exception:
                                        pass

                                    # Fallback: data-large_image o src del gallery
                                    if not image_url:
                                        try:
                                            gal = detail_page.locator(self.selectors["detail_image"]).first
                                            if gal.count() > 0:
                                                large = gal.get_attribute("data-large_image")
                                                src_gal = gal.get_attribute("src")
                                                image_url = large if large else (src_gal or "")
                                        except Exception:
                                            pass

                                    # SKU
                                    try:
                                        sku_el = detail_page.locator(self.selectors["detail_sku"])
                                        if sku_el.count() > 0:
                                            sku = sku_el.first.inner_text().replace("SKU:", "").strip()
                                    except Exception:
                                        pass

                                    # Descripción: pestaña #tab-description → short-desc
                                    try:
                                        desc_el = detail_page.locator(self.selectors["detail_desc"])
                                        if desc_el.count() > 0:
                                            description = self.clean_description(desc_el.first.inner_text())
                                    except Exception:
                                        pass

                                    if not description:
                                        try:
                                            sd_el = detail_page.locator(self.selectors["detail_short_desc"])
                                            if sd_el.count() > 0:
                                                description = self.clean_description(sd_el.first.inner_text())
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

                            # ── Descarga de imágenes (S3 / local) ─────────────
                            site_folder = self.site_name.replace(" ", "_").lower()
                            if thumbnail_url:
                                local_thumb = self.download_image(thumbnail_url, subfolder=site_folder)
                                if local_thumb:
                                    thumbnail_url = local_thumb
                            if image_url:
                                local_img = self.download_image(image_url, subfolder=site_folder)
                                if local_img:
                                    image_url = local_img

                            # ── Marca: siempre Winkler Nutrition ──────────────
                            # La página vende exclusivamente productos propios.
                            brand = "Winkler Nutrition"

                            # ── Clasificación ──────────────────────────────────
                            final_category, final_sub = self.classifier.classify(
                                title, description, main_category, deterministic_sub, brand
                            )

                            yield {
                                "date": current_date,
                                "site_name": self.site_name,
                                "category": self.clean_text(final_category),
                                "subcategory": final_sub,
                                "product_name": title,
                                "brand": brand,
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

                        # ── Paginación ─────────────────────────────────────────
                        next_btn = page.locator(self.selectors["next_button"])
                        if next_btn.count() > 0 and next_btn.first.is_visible():
                            href = next_btn.first.get_attribute("href")
                            if href:
                                next_url = href if href.startswith("http") else self.base_url + href
                                print(f"  > Avanzando a página {page_number + 1}...")
                                page.goto(next_url, wait_until="domcontentloaded")
                                page_number += 1
                                page.wait_for_timeout(1500)
                            else:
                                break
                        else:
                            print("  > No hay más páginas.")
                            break

                except Exception as e:
                    print(f"[red]Error procesando {url}: {e}[/red]")


if __name__ == "__main__":
    scraper = WinklerNutritionScraper(headless=True)
    scraper.run()
