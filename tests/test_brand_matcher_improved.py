import unittest
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_processing.brand_matcher import BrandMatcher

class TestBrandMatcherImproved(unittest.TestCase):
    def setUp(self):
        self.matcher = BrandMatcher()
        # Force re-load aliases for test environment if singleton persisted
        self.matcher._load_aliases()
        
        # Ensure we have the brands loaded
        if not self.matcher._brands:
            self.matcher._load_brands()

    def test_alias_iso100(self):
        self.assertEqual(self.matcher.get_best_match("Dymatize ISO 100"), "Dymatize") # Standard match
        self.assertEqual(self.matcher.get_best_match("Proteina ISO100 Vainilla"), "Dymatize") # Alias match
        self.assertEqual(self.matcher.get_best_match("ISO 100 5lbs"), "Dymatize") # Alias match

    def test_alias_ultimate(self):
        self.assertEqual(self.matcher.get_best_match("Ultimate Prostar Whey"), "Ultimate Nutrition")

    def test_hangryboy(self):
        # Case insensitive check
        self.assertEqual(self.matcher.get_best_match("Hangryboy Protein"), "HANGRYBOY")
        self.assertEqual(self.matcher.get_best_match("HANGRYBOY Bar"), "HANGRYBOY")
        
        # If alias was added for 'hangry', check it
        self.assertEqual(self.matcher.get_best_match("Barra Hangry"), "HANGRYBOY")

    def test_empty_input(self):
        self.assertEqual(self.matcher.get_best_match(""), "N/D")
        self.assertEqual(self.matcher.get_best_match(None), "N/D")

    def test_special_chars_and_aliases(self):
        # "ISO-100" -> normalize -> "iso 100" -> match alias "iso 100"
        self.assertEqual(self.matcher.get_best_match("Dymatize ISO-100"), "Dymatize")

if __name__ == '__main__':
    unittest.main()
