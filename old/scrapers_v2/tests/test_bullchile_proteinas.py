import sys
import os
import csv
from datetime import datetime
from rich import print

# Add parent dir to path to allow importing scrapers_v2 modules
# Path to scrapers_v2 (one level up from tests)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from SuplementosBullChileScraper import SuplementosBullChileScraper
from playwright.sync_api import sync_playwright

class TestBullChileProteinas(SuplementosBullChileScraper):
    def __init__(self):
        # Initialize with default URL
        base_url = "https://www.suplementosbullchile.cl"
        super().__init__(base_url, headless=False)
        
        # Filter ONLY Proteinas
        print("[yellow]Filtering categories to ONLY 'Proteinas' for testing...[/yellow]")
        self.category_urls = {k: v for k, v in self.category_urls.items() if k == "Proteinas"}

    def run(self):
        # Override run to save in CURRENT directory (scrapers_v2/tests)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()
            
            # Save in CURRENT directory
            output_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Timestamped filename
            csv_filename = f"test_bullchile_proteinas_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
            csv_file = os.path.join(output_dir, csv_filename)
            
            headers = ['date', 'site_name', 'category', 'subcategory', 'product_name', 'brand', 'price', 'link', 'rating', 'reviews', 'active_discount', 'thumbnail_image_url', 'image_url', 'sku', 'description']
            
            print(f"[green]Running TEST for Proteinas.[/green]")
            print(f"Saving to: {csv_file}")
            
            try:
                with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    
                    count = 0
                    for product in self.extract_process(page):
                        writer.writerow(product)
                        f.flush()
                        count += 1
                        print(f"  > Extracted: {product['product_name']} ({product['subcategory']})")
                        
                    print(f"\n[bold green]Test Finished. Total products: {count}[/bold green]")
                    print(f"File saved at: {csv_file}")
            except Exception as e:
                print(f"[red]Test Error: {e}[/red]")
            finally:
                browser.close()

if __name__ == "__main__":
    scraper = TestBullChileProteinas()
    scraper.run()
