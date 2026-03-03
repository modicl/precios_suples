import csv
import os
import re

class BrandMatcher:
    _instance = None
    _brands = []
    _aliases = {}

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(BrandMatcher, cls).__new__(cls)
            cls._instance._load_brands()
            cls._instance._load_aliases()
        return cls._instance

    def _load_brands(self):
        """Loads brands from marcas_dictionary.csv"""
        if self._brands:
            return

        # Possible locations for the file
        possible_paths = [
            'marcas_dictionary.csv',
            'processed_data/marcas_dictionary.csv',
            '../marcas_dictionary.csv',
            os.path.join(os.path.dirname(__file__), '..', 'marcas_dictionary.csv')
        ]

        file_path = None
        for path in possible_paths:
            if os.path.exists(path):
                file_path = path
                break
        
        if not file_path:
            print("Warning: marcas_dictionary.csv not found.")
            return

        try:
            with open(file_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    brand_name = row.get('nombre_marca')
                    if brand_name and brand_name != 'N/D':
                        # Store stripped version to avoid whitespace issues from CSV
                        self._brands.append(brand_name.strip())
            
            # Sort by length descending to prioritize longer matches
            self._brands.sort(key=len, reverse=True)
            print(f"Loaded {len(self._brands)} brands for matching.")
        except Exception as e:
            print(f"Error loading brands: {e}")

    def _load_aliases(self):
        """
        Loads hardcoded aliases or domain-specific mappings.
        Format: normalized_keyword -> correct_brand_name
        """
        self._aliases = {
            'iso100': 'Dymatize',
            'iso 100': 'Dymatize',
            'ultimate': 'Ultimate Nutrition',
            'hangry': 'HANGRYBOY', # Alias just in case "Hangryboy" matching fails, or for "Hangry" substring
            'hangryboy': 'HANGRYBOY',
            'king whey': 'Ronnie Coleman', # Example based on test data, usually RC King Whey
            'animal pak': 'Universal Nutrition', # Often associated
            'perfect nutricion': 'Perfect Nutrition', # Typo fix
            'fit supps': 'FitSupps', # Spacing fix
            'black line': 'Perfect Nutrition', # Line association based on data patterns
            'innovative': 'Innovative Fit', # Shortening
            '100 whey protein': 'Innovative Fit', # User specific request for generic name mapping
            'creatine premium plus': 'Innovative Fit', # User specific request
            'activ-on': 'Activ On', # Formatting
            'high protein bar': 'Activlab', # Product specific mapping
            'activ on': 'Activ On' # Ensure distinct word matching captures this
        }
    
    def normalize(self, text: str) -> str:
        """
        Normalizes text: lowercase, replaces special characters with spaces, keeps alphanumeric.
        """
        if not text:
            return ""
        # normalize to lowercase
        text = text.lower()
        # Replace punctuation like .,-! with space
        text = re.sub(r'[^\w\s]', ' ', text)
        # Collapse multiple spaces
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def get_best_match(self, product_name: str) -> str:
        """
        Finds the best matching brand in the product name.
        Returns the brand name or "N/D" if no match found.
        """
        if not product_name:
            return "N/D"

        normalized_product = self.normalize(product_name)
        
        # 1. Check Aliases first (High Priority specific overrides)
        # We look for the alias keyword in the product name
        for alias, brand_target in self._aliases.items():
            # Check if alias is present (using word boundaries)
            pattern = r'\b' + re.escape(alias) + r'\b'
            if re.search(pattern, normalized_product):
                return brand_target

        # 2. Standard Dictionary Match
        # Since _brands is sorted by length (descending), the first match is likely the "best"
        for brand in self._brands:
            # Normalize brand name for comparison
            normalized_brand = self.normalize(brand)
            if not normalized_brand:
                continue
            
            # Use word boundary check to avoid partial matches
            pattern = r'\b' + re.escape(normalized_brand) + r'\b'
            if re.search(pattern, normalized_product):
                return brand
        
        return "N/D"
