
from OneNutritionScraper import OneNutritionScraper
from playwright.sync_api import sync_playwright
import csv
import os
import sys

def run_test():
    print("Initializing OneNutritionScraper for testing...")
    base_url = "https://onenutrition.cl"
    scraper = OneNutritionScraper(base_url=base_url, headless=True)
    
    # Filter for only "Proteinas" and "Creatinas"
    target_categories = ["Proteinas", "Creatinas"]
    filtered_urls = {k: v for k, v in scraper.category_urls.items() if k in target_categories}
    scraper.category_urls = filtered_urls
    
    print(f"Filtered categories to: {list(scraper.category_urls.keys())}")
    
    results = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        
        # Run extraction
        # extract_process is a generator
        for item in scraper.extract_process(page):
            print(f"Scraped: {item.get('product_name', 'N/A')} - {item.get('subcategory', 'N/A')}")
            results.append(item)
            
        browser.close()
        
    # Save to CSV in the same folder
    if results:
        params = list(results[0].keys())
        filename = "test_onenutrition_results.csv"
        
        print(f"\nSaving {len(results)} results to {filename}...")
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=params)
                writer.writeheader()
                writer.writerows(results)
            print("Done.")
        except Exception as e:
            print(f"Error saving CSV: {e}")
    else:
        print("No results found.")

if __name__ == "__main__":
    # Force UTF-8 for console output
    sys.stdout.reconfigure(encoding='utf-8')
    run_test()
