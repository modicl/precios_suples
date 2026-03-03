
from OneNutritionScraper import OneNutritionScraper
from playwright.sync_api import sync_playwright
import csv
import os
import sys

def run_test():
    print("Initializing OneNutritionScraper for testing Vitamins...")
    base_url = "https://onenutrition.cl"
    scraper = OneNutritionScraper(base_url=base_url, headless=True)
    
    # Filter for only "Vitaminas y Minerales"
    # Note: We need to make sure this category key matches what's used in the scraper.
    # The scraper usually loads categories from DB or hardcoded.
    # In OneNutritionScraper.py, it seems it iterates `self.category_urls`.
    # Let's inspect `scraper.category_urls` to find the right key for Vitamins.
    
    target_key = None
    for k in scraper.category_urls.keys():
        if "vitamin" in k.lower():
            target_key = k
            break
    
    if target_key:
        print(f"Found Vitamins category key: {target_key}")
        scraper.category_urls = {target_key: scraper.category_urls[target_key]}
    else:
        print("Could not find 'Vitaminas' in category keys. Printing all keys:")
        print(list(scraper.category_urls.keys()))
        # Fallback: if not found, maybe it's not loaded or named differently.
        # But wait, the scraper logic depends on `final_category` being passed to `_classify_product`.
        # `extract_process` iterates `self.category_urls` where key is `main_category`.
        # So we need to ensure we run the process for the category that corresponds to Vitamins.
        return

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
        
    # Save to CSV
    if results:
        params = list(results[0].keys())
        filename = "test_onenutrition_vitamins_results.csv"
        
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
    sys.stdout.reconfigure(encoding='utf-8')
    run_test()
