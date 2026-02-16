
from playwright.sync_api import sync_playwright
from StrongestScraper import StrongestScraper
import rich
from rich import print

def test_live_classification():
    with sync_playwright() as p:
        print("[bold green]Iniciando Test en Vivo de Clasificación Strongest (Solo Proteínas)[/bold green]")
        # Add arguments to reduce bot detection risk and prevent crashes
        browser = p.chromium.launch(
            headless=True, 
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-setuid-sandbox"]
        )
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = context.new_page()
        
        scraper = StrongestScraper()
        # Override to scrape ONLY Proteinas for the test
        scraper.category_urls = {
            "Proteinas": [{"url": "https://www.strongest.cl/collection/proteinas", "subcategory": "Proteinas"}]
        }
        
        # We manually iterate over the generator
        count = 0
        limit = 40 # Check first 10 products
        
        print(f"[blue]Scrapeando primeros {limit} productos...[/blue]")
        
        try:
            for product in scraper.extract_process(page):
                count += 1
                name = product['product_name']
                desc = product['description']
                subcat = product['subcategory']
                
                print(f"[white]--------------------------------------------------[/white]")
                print(f"[bold yellow]Producto {count}:[/bold yellow] {name}")
                print(f"[dim]Desc (extracto): {desc[:100]}...[/dim]")
                
                # Check what keywords triggered key classifications
                keywords_found = []
                check_text = (name + " " + desc).lower()
                
                if "concentrad" in check_text or "concentrate" in check_text or "blend" in check_text or "mezcla" in check_text:
                    keywords_found.append("[red]CONCENTRADO/BLEND[/red]")
                if "iso" in check_text or "isolate" in check_text or "aislada" in check_text:
                    keywords_found.append("[cyan]ISOLATE[/cyan]")
                if "hydro" in check_text or "hidrolizada" in check_text:
                    keywords_found.append("[blue]HYDRO[/blue]")
                
                if keywords_found:
                    print(f"Keywords detectadas: {', '.join(keywords_found)}")
                
                print(f"[bold green]RESULTADO CLASIFICACIÓN -> {subcat}[/bold green]")
                
                if count >= limit:
                    break
        except Exception as e:
            print(f"[red]Error durante el test iteración: {e}[/red]")
            # import traceback
            # traceback.print_exc()
            
        browser.close()
        print("\n[bold green]Test Finalizado.[/bold green]")

if __name__ == "__main__":
    test_live_classification()
