import os
import sys
import csv
from datetime import datetime
from rich import print
from playwright.sync_api import sync_playwright

# Ensure current directory is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from SupleStoreScraper import SupleStoreScraper

# Custom class to override category_urls and handle output path
class TestSupleStoreScraper(SupleStoreScraper):
    def __init__(self, output_file="test_suplestore_proteinas.csv", headless=False):
        super().__init__(base_url="https://www.suplestore.cl", headless=headless)
        self.output_file = output_file
        
        # OMIT OTHER CATEGORIES, KEEP ONLY PROTEINAS with special subcategory
        # Note: The original SupleStoreScraper.__init__ sets self.category_urls.
        # We override it here to be just Proteinas.
        self.category_urls = {
             "Proteinas": [
                { "url": "https://www.suplestore.cl/collection/proteinas", "subcategory": "CATEGORIZAR_PROTEINA" }
            ]
        }
        
    def run_test(self):
        """
        Custom run method for testing, saves CSV in current directory.
        """
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()
            
            # Save in CURRENT directory (where the script is)
            csv_file = os.path.join(current_dir, self.output_file)
            
            csv_headers = ['date', 'site_name', 'category', 'subcategory', 'product_name', 'brand', 'price', 'link', 'rating', 'reviews', 'active_discount', 'thumbnail_image_url', 'image_url', 'sku', 'description']
            
            print(f"[bold green]Starting Test Scraper for SupleStore (Proteinas only)...[/bold green]")
            print(f"Saving data to: {csv_file}")
            
            try:
                with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=csv_headers)
                    writer.writeheader()
                    
                    count = 0
                    # Call extract_process from the parent class
                    for product_data in self.extract_process(page):
                        writer.writerow(product_data)
                        f.flush()
                        count += 1
                        print(f"  -> Scraped: {product_data['product_name']} ({product_data['subcategory']})")
                    
                    print(f"\n[bold green]Finished test scraping. Total products: {count}[/bold green]")
                        
            except Exception as e:
                print(f"[red]Error during test scraping: {e}[/red]")
            finally:
                browser.close()

if __name__ == "__main__":
    # Run the test scraper
    scraper = TestSupleStoreScraper(headless=False) # Headless False to see it working if needed
    scraper.run_test()
