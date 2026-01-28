import unittest
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_processing.brand_matcher import BrandMatcher

class TestBrandMatcher(unittest.TestCase):
    def setUp(self):
        # Reset singleton if needed (though it's tricky with Python singletons implemented via __new__)
        # For this simple test, we can just use the instance.
        self.matcher = BrandMatcher()
        
        # Inject known brands for testing logic (overriding loaded ones for specific test cases if needed)
        # But let's test with the real dictionary if it loaded, or inject a test set
        self.matcher._brands = ["Optimum Nutrition", "Dymatize", "BPI Sports", "Muscletech", "Jym", "Kevin Levrone"]
        self.matcher._brands.sort(key=len, reverse=True)

    def test_exact_match(self):
        self.assertEqual(self.matcher.get_best_match("Proteina Optimum Nutrition Gold Standard"), "Optimum Nutrition")

    def test_case_insensitive(self):
        self.assertEqual(self.matcher.get_best_match("dymatize iso 100"), "Dymatize")

    def test_partial_word_avoidance(self):
        # "Jym" should match "Pre Jym"
        self.assertEqual(self.matcher.get_best_match("Pre Jym"), "Jym")
        # "Jym" should NOT match "Gymnast" (if Gymnast was a product name) - relying on word boundary
        # Note: "Jym" is in my test list. 
        self.assertEqual(self.matcher.get_best_match("Gymnast equipment"), "N/D")

    def test_special_characters(self):
        self.assertEqual(self.matcher.get_best_match("BPI-Sports Best Protein"), "BPI Sports")
        
    def test_no_match(self):
        self.assertEqual(self.matcher.get_best_match("Manzana Verde"), "N/D")

    def test_prefer_longer_match(self):
        # Add a shorter brand that is a substring of a longer brand
        self.matcher._brands.append("Optimum")
        self.matcher._brands.sort(key=len, reverse=True)
        
        # Should match "Optimum Nutrition" because it's longer and sorted first
        self.assertEqual(self.matcher.get_best_match("Optimum Nutrition Whey"), "Optimum Nutrition")
        
        # Should match "Optimum" if "Nutrition" is missing
        self.assertEqual(self.matcher.get_best_match("Optimum Whey"), "Optimum")

if __name__ == '__main__':
    unittest.main()
