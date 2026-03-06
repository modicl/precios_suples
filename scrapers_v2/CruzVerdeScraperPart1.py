# Scraper para Cruz Verde (Parte 1 de 2)
# Categorias: Proteinas, Snacks y Comida
from BaseScraper import BaseScraper
from CategoryClassifier import CategoryClassifier
from rich import print
from datetime import datetime
import re
import time
import argparse


class CruzVerdeScraperPart1(BaseScraper):
    def __init__(self, headless=False):
        self.category_urls = {
            "Proteinas": [
                {"url": "https://www.cruzverde.cl/vitaminas-y-suplementos/nutricion-deportiva/proteinas/", "subcategory": "Proteinas"},
            ],
            "Snacks y Comida": [
                {"url": "https://www.cruzverde.cl/vitaminas-y-suplementos/nutricion-deportiva/barras-proteicas/", "subcategory": "Barras Proteicas"},
            ],
        }

        selectors = {
            "product_card": "ml-new-card-product",
            "brand": "p.text-gray-dark.uppercase",
            "name": "h2 a.new-ellipsis",
            "image": "at-image img",
            "link": "at-image a",
            "no_stock_badge": "div:has-text('Sin stock online')",
            "detail_image": "img.ngxImageZoomThumbnail",
            "description": "p >> text=Ayuda a la recuperación",
        }

        super().__init__(
            "https://www.cruzverde.cl",
            headless,
            category_urls=self.category_urls,
            selectors=selectors,
            site_name="Cruz Verde",
            output_suffix="_part1",
        )
        self.classifier = CategoryClassifier()

    def _classify_product(self, title, description, main_category, deterministic_subcategory, brand):
        if main_category == "Energia":
            title_lower = title.lower()
            if any(k in title_lower for k in ["gel energizante", "gel energético", "gel energetico", "energy gel", "power honey"]):
                inferred_cat = "Pre Entrenos"
            elif any(k in title_lower for k in ["isoton", "electrolit", "hidratacion", "hidratante", "ampollas bebibles"]):
                inferred_cat = "Bebidas Nutricionales"
            elif any(k in title_lower for k in ["creatina", "creatine", "monohidrato", "creapure", "kre-alkalyn"]):
                inferred_cat = "Creatinas"
            elif any(k in title_lower for k in ["pre workout", "pre-workout", "preworkout", "pre entreno", "pre-entreno", "beta alanin", "beta-alanin", "cafeina", "caffeine", "energy booster"]):
                inferred_cat = "Pre Entrenos"
            elif any(k in title_lower for k in ["carnitina", "carnitine", "l-carnitin", "quemador", "termogenico", "fat burner", "ultra ripped", "cla ", "garcinia"]):
                inferred_cat = "Perdida de Grasa"
            elif any(k in title_lower for k in ["bcaa", "aminoacido", "glutamina", "leucina", "eaa", "hmb", "citrulina", "arginina", "arginine", "taurina", "lisina"]):
                inferred_cat = "Aminoacidos y BCAA"
            elif any(k in title_lower for k in ["proteina", "whey", "protein", "caseina", "casein", "albumina", "isolate"]):
                inferred_cat = "Proteinas"
            elif any(k in title_lower for k in ["vitamina", "mineral", "magnesio", "zinc", "calcio", "hierro", "potasio", "curcuma", "curcumin", "melatonin", "multivitamin", "complejo b", "acido folico", "probiotico"]):
                inferred_cat = "Vitaminas y Minerales"
            elif any(k in title_lower for k in ["gainer", "mass gainer", "hipercalorico", "voluminizador"]):
                inferred_cat = "Ganadores de Peso"
            else:
                return "OTROS", "Otros"
            return self.classifier.classify(title, description, inferred_cat, inferred_cat, brand)
        return self.classifier.classify(title, description, main_category, deterministic_subcategory, brand)

    def extract_process(self, page):
        print(f"[green]Iniciando scraping de Cruz Verde (Part1: Proteinas + Snacks)...[/green]")

        for main_category, items in self.category_urls.items():
            for item in items:
                category_url = item['url']
                deterministic_subcategory = item['subcategory']
                print(f"\n[bold blue]Procesando:[/bold blue] {main_category} -> {deterministic_subcategory}")

                current_url = category_url
                page_num = 1
                has_more_pages = True

                try:
                    page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
                except Exception as e:
                    print(f"[red]Error cargando {current_url}: {e}[/red]")
                    continue

                while has_more_pages:
                    print(f"--- Página {page_num} ---")

                    try:
                        page.wait_for_selector("ml-new-card-product", timeout=10000)
                    except:
                        print(f"[yellow]No se encontraron productos en la página {page_num}.[/yellow]")
                        break

                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(1)

                    cards = page.locator("ml-new-card-product")
                    count = cards.count()
                    print(f"  > Encontrados {count} productos.")

                    if count == 0:
                        break

                    for i in range(count):
                        try:
                            card = cards.nth(i)

                            name = "N/D"
                            name_el = card.locator(self.selectors['name']).first
                            if name_el.count() > 0:
                                name = self.clean_text(name_el.inner_text())

                            brand = "N/D"
                            brand_el = card.locator(self.selectors['brand']).first
                            if brand_el.count() > 0:
                                brand = self.clean_text(brand_el.inner_text())

                            link = "N/D"
                            link_el = card.locator(self.selectors['link']).first
                            if link_el.count() > 0:
                                href = link_el.get_attribute("href")
                                if href:
                                    link = self.base_url + href if href.startswith('/') else href

                            if link != "N/D" and link in self.seen_urls:
                                print(f"[yellow]  >> Duplicado omitido: {name}[/yellow]")
                                continue
                            if link != "N/D":
                                self.seen_urls.add(link)

                            image_url = ""
                            img_el = card.locator(self.selectors['image']).first
                            if img_el.count() > 0:
                                src = img_el.get_attribute("src")
                                if src:
                                    image_url = src

                            is_out_of_stock = card.get_by_text("Sin stock online").count() > 0

                            price = 0
                            active_discount = False

                            if not is_out_of_stock:
                                offer_el = card.locator("p.text-green-turquoise").first
                                normal_el = card.locator("p.line-through").first

                                if offer_el.count() > 0:
                                    price_text = offer_el.inner_text()
                                    active_discount = True
                                elif normal_el.count() > 0:
                                    any_price = card.locator("p.font-bold").first
                                    price_text = any_price.inner_text() if any_price.count() > 0 else "0"
                                else:
                                    normal_price_simple = card.locator("p.text-gray-dark.font-bold, p.text-green-turquoise").first
                                    if normal_price_simple.count() > 0:
                                        price_text = normal_price_simple.inner_text()
                                    else:
                                        all_texts = card.inner_text()
                                        match = re.search(r'\$[\d.]+', all_texts)
                                        price_text = match.group(0) if match else "0"

                                clean_price = re.sub(r'[^\d]', '', price_text)
                                if clean_price:
                                    price = int(clean_price)

                            detail_image_url = image_url
                            sku = "N/D"
                            description = "N/D"

                            if link != "N/D":
                                context = page.context
                                detail_page = None
                                try:
                                    detail_page = context.new_page()
                                    detail_page.goto(link, wait_until="domcontentloaded", timeout=30000)
                                    try:
                                        detail_page.wait_for_selector("ml-accordion", timeout=8000)
                                    except:
                                        pass

                                    if detail_page.locator(self.selectors['detail_image']).count() > 0:
                                        img_src = detail_page.locator(self.selectors['detail_image']).first.get_attribute('src')
                                        if img_src and not ('disclaimer' in img_src or 'logo' in img_src):
                                            detail_image_url = img_src

                                    try:
                                        description_js = detail_page.evaluate('''(productName) => {
                                            const TABS_TO_SCRAPE = ['Beneficios y Usos', 'Ficha técnica'];
                                            const accordions = document.querySelectorAll(
                                                'section.bg-gray-lightest or-menu section.flex.flex-col ml-accordion'
                                            );
                                            const parts = [];
                                            const seen = new Set();
                                            accordions.forEach(acc => {
                                                const tabLabel = acc.querySelector('span.pointer-events-none')?.innerText?.trim();
                                                if (!tabLabel || !TABS_TO_SCRAPE.includes(tabLabel)) return;
                                                acc.querySelectorAll('p').forEach(p => {
                                                    const t = p.innerText?.trim();
                                                    if (!t || t.length < 4) return;
                                                    if (t === productName || t === tabLabel) return;
                                                    if (seen.has(t)) return;
                                                    seen.add(t);
                                                    parts.push(t);
                                                });
                                            });
                                            return parts.length ? parts.join(' | ') : null;
                                        }''', name)
                                        if description_js:
                                            description = description_js
                                    except:
                                        pass

                                    url_match = re.search(r'/(\d+)\.html', link)
                                    if url_match:
                                        sku = url_match.group(1)

                                    detail_page.close()
                                except Exception as e:
                                    print(f"[red]Error detalle {link}: {e}[/red]")
                                    if detail_page:
                                        detail_page.close()

                            final_category, final_subcategory = self._classify_product(
                                name, description, main_category, deterministic_subcategory, brand
                            )

                            site_folder = self.site_name.replace(" ", "_").lower()
                            if image_url:
                                local_thumb = self.download_image(image_url, subfolder=site_folder)
                                if local_thumb:
                                    image_url = local_thumb
                            if detail_image_url:
                                local_img = self.download_image(detail_image_url, subfolder=site_folder)
                                if local_img:
                                    detail_image_url = local_img

                            yield {
                                'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                'site_name': self.site_name,
                                'category': self.clean_text(final_category),
                                'subcategory': final_subcategory,
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
                                'description': description,
                            }

                        except Exception as e:
                            print(f"[red]Error extrayendo producto: {e}[/red]")
                            continue

                    next_page_num = page_num + 1
                    next_page_btn = page.locator(
                        f"ml-pagination div.rounded-full:has-text('{next_page_num}')"
                    )

                    if next_page_btn.count() > 0:
                        print(f"Navegando a página {next_page_num}...")
                        page.evaluate(
                            """(num) => {
                                const btns = Array.from(document.querySelectorAll('ml-pagination div.rounded-full'));
                                const btn = btns.find(b => b.innerText.trim() === String(num));
                                if (btn) btn.click();
                            }""",
                            next_page_num
                        )
                        page_num += 1
                        try:
                            page.wait_for_selector("ml-new-card-product", timeout=10000)
                        except:
                            time.sleep(3)
                    else:
                        print("No hay más páginas.")
                        has_more_pages = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()
    CruzVerdeScraperPart1(headless=args.headless).run()
