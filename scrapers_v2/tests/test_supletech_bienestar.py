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

from SupleTechScraper import SupleTechScraper

class TestSupleTechBienestar(SupleTechScraper):
    def __init__(self, output_file="test_supletech_bienestar.csv", headless=False):
        super().__init__(base_url="https://www.supletech.cl", headless=headless)
        self.output_file = output_file
        
        # Override to test ONLY "Bienestar General"
        self.category_urls = {
             "Vitaminas y Minerales": [
                { "url": "https://www.supletech.cl/bienestar/descanso-y-sueno", "subcategory": "Bienestar General" }
            ]
        }
        
    def run_test(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()
            
            # Save in CURRENT directory
            csv_file = os.path.join(current_dir, self.output_file)
            
            csv_headers = ['date', 'site_name', 'category', 'subcategory', 'product_name', 'brand', 'price', 'link', 'rating', 'reviews', 'active_discount', 'thumbnail_image_url', 'image_url', 'sku', 'description']
            
            print(f"[bold green]Starting Test Scraper for SupleTech (Bienestar General)...[/bold green]")
            print(f"Saving data to: {csv_file}")
            
            try:
                with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=csv_headers)
                    writer.writeheader()
                    
                    count = 0
                    for product_data in self.extract_process(page):
                        writer.writerow(product_data)
                        f.flush()
                        count += 1
                        print(f"  -> Scraped: {product_data['product_name']} \n     [g]Cat:[/g] {product_data['category']} -> [g]Sub:[/g] {product_data['subcategory']}")
                    
                    print(f"\n[bold green]Finished test scraping. Total products: {count}[/bold green]")
                        
            except Exception as e:
                print(f"[red]Error during test scraping: {e}[/red]")
            finally:
                browser.close()

if __name__ == "__main__":
    scraper = TestSupleTechBienestar(headless=False)
    scraper.run_test()
