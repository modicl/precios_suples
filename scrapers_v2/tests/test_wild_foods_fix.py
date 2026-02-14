from WildFoodsScraper import WildFoodsScraper
from playwright.sync_api import sync_playwright

class TestWildFoodsScraper(WildFoodsScraper):
    def __init__(self):
        # Override to just test one category
        super().__init__(headless=True)
        self.category_urls = {
            "Proteinas": [
                {
                    "url": "https://thewildfoods.com/collections/whey-protein",
                    "subcategory": "Proteína de Whey"
                }
            ]
        }

    def run_test(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()
            
            print("Running extraction test...")
            # We need to manually call extract_process or simulates what run() does
            # But extract_process is a generator.
            
            gen = self.extract_process(page)
            
            count = 0
            for product in gen:
                count += 1
                print(f"Product {count}:")
                print(f"  Name: {product['product_name']}")
                print(f"  Link: {product['link']}")
                print(f"  Image: {product['image_url']}")
                print(f"  Price: {product['price']}")
                
                if product['link'] == "N/D":
                    print("  [FAIL] Link is N/D")
                if "N/D" in product['image_url'] or not product['image_url']:
                     print("  [FAIL] Image invalid")
                
                if count >= 3:
                    break
            
            browser.close()

if __name__ == "__main__":
    test = TestWildFoodsScraper()
    test.run_test()
