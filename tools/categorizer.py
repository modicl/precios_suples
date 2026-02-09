import os
import sys
import json
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
import requests
from google import genai
from google.genai import types

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

class ProductCategorizer:
    def __init__(self, db_connection=None, ollama_url="http://localhost:11434", model="qwen2.5:14b", enable_ai=True):
        # Configuration
        self.enable_ai = enable_ai
        self.provider = os.getenv("AI_PROVIDER", "ollama").lower()
        self.google_api_key = os.getenv("GOOGLE_API_KEY")
        self.ollama_url = ollama_url
        self.client = None
        
        # Select Model based on Provider
        if self.provider == "google":
            # Trying Gemini 2.5 Flash (Newest, likely open quota)
            self.model = "gemini-2.5-flash"
            print(f"[Categorizer] Using Google Gemini ({self.model})")
            if self.google_api_key:
                self.client = genai.Client(api_key=self.google_api_key)
        else:
            self.model = model # Default Ollama model
            print(f"[Categorizer] Using Ollama ({self.model})")

        self.subcategories_map = {} 
        self.categories_map = {}
        self.cache = {}
        
        # Check connectivity (only if AI enabled)
        if self.enable_ai:
            if self.provider == "google":
                if not self.google_api_key:
                    print("[Categorizer] WARNING: Provider is Google but GOOGLE_API_KEY is missing!")
                    self.enable_ai = False
            else:
                # Check Ollama connection
                try:
                    resp = requests.get(f"{self.ollama_url}/api/tags")
                    if resp.status_code == 200:
                        # ... existing check logic ...
                        pass
                except Exception as e:
                    print(f"[Categorizer] Warning: Could not verify Ollama: {e}")

        if db_connection:
            self.load_categories(db_connection)
        else:
            self._connect_local()
    
    def _connect_local(self):
        targets = get_targets()
        local_target = next((t for t in targets if t['name'] == 'Local'), None)
        if local_target:
            try:
                engine = sa.create_engine(local_target['url'])
                with engine.connect() as conn:
                    self.load_categories(conn)
            except Exception as e:
                print(f"[Categorizer] Error connecting to DB: {e}")

    def load_categories(self, conn):
        """Loads current valid subcategories from DB."""
        print("[Categorizer] Loading categories from DB...")
        
        # 1. Load Categories
        cats = conn.execute(sa.text("SELECT id_categoria, nombre_categoria FROM categorias")).fetchall()
        self.categories_map = {row.id_categoria: row.nombre_categoria for row in cats}
        
        # 2. Load Subcategories
        subs = conn.execute(sa.text("SELECT id_subcategoria, nombre_subcategoria, id_categoria FROM subcategorias")).fetchall()
        
        self.subcategories_map = {}
        for row in subs:
            norm_name = row.nombre_subcategoria.lower().strip()
            self.subcategories_map[norm_name] = {
                "id_subcategoria": row.id_subcategoria,
                "id_categoria": row.id_categoria,
                "nombre_subcategoria": row.nombre_subcategoria,
                "nombre_categoria": self.categories_map.get(row.id_categoria, "Unknown")
            }
            
        print(f"[Categorizer] Loaded {len(self.subcategories_map)} subcategories.")

    def classify_product(self, product_name, original_subcategory=None):
        """
        Classifies a product into a standardized subcategory.
        First checks exact match, then cache, then Ollama (if enabled).
        """
        # 1. Normalize inputs
        prod_norm = product_name.lower().strip()
        sub_norm = original_subcategory.lower().strip() if original_subcategory else ""
        
        # 2. Direct Match (Trust original if it exists in our DB)
        if sub_norm in self.subcategories_map:
            return self.subcategories_map[sub_norm]
            
        # 3. Check Cache (prod_name + sub_name)
        cache_key = f"{prod_norm}|{sub_norm}"
        if cache_key in self.cache:
            # print(f"[Categorizer] Cache hit for '{product_name}'")
            return self.cache[cache_key]

        # 4. Ask Ollama (ONLY if enabled)
        if self.enable_ai:
            result = self._ask_ollama(product_name, original_subcategory)
            
            # 5. Cache and Return
            if result:
                self.cache[cache_key] = result
                return result
        
        return None

    def classify_batch(self, items_list):
        """
        Classifies a batch of products using Ollama.
        Args:
            items_list: List of dicts [{'product': 'Name', 'context': 'Context'}, ...]
        Returns:
            List of result dicts or None for failures.
        """
        if not self.enable_ai:
            return [None] * len(items_list)

        valid_options = [data['nombre_subcategoria'] for data in self.subcategories_map.values()]
        # Remove duplicates for cleaner prompt
        valid_options = sorted(list(set(valid_options)))
        valid_options_str = ", ".join(valid_options)
        
        # Prepare batch prompt
        items_str = ""
        for i, item in enumerate(items_list):
            # Include BRAND in the prompt if available
            brand_ctx = item.get('brand', 'Unknown Brand')
            items_str += f"{i+1}. Product: {item['product']} | Brand: {brand_ctx} | Context: {item['context']}\n"
            
        system_msg = f"""You are an expert supplement classifier and data cleaner.
Valid Categories: [{valid_options_str}]

Task: 
1. Classify the product into ONE of the Valid Categories exactly.
2. Create a 'clean_name' for the product.
   - REMOVE the brand name from the title if present. Brand is explicitly provided in context.
   - REMOVE promotional text like 'Oferta', 'Promo', 'Bundle'.
   - DO NOT remove "Pack" if it indicates quantity (e.g. "Pack 2", "Pack de 3"). Only remove if it's generic "Pack Oferta".
   - DO NOT remove words like 'Muestra', 'Sachet', 'Sample'.
   - Keep the core product name, flavor, and size.
   - Example Input: "Optimum Nutrition Gold Standard 100% Whey 5lbs" | Brand: "Optimum Nutrition" -> Clean Name: "Gold Standard 100% Whey 5lbs"
   - Example Input: "Muestra Whey Protein" | Brand: "Generic" -> Clean Name: "Muestra Whey Protein"

Output a JSON array of objects with keys:
- "index" (integer)
- "category" (string)
- "clean_name" (string)"""

        user_msg = f"""Items to Classify (Format: Index. Product | Brand | Context):
{items_str}

Rules:
1. Use EXACT category names from the list.
2. Return ONLY the JSON array.
3. DO NOT use markdown code blocks (```json). Just raw JSON.
"""
        
        try:
            if self.provider == "google" and self.client:
                # GOOGLE GEMINI CALL (using official SDK)
                try:
                    full_prompt = system_msg + "\n\n" + user_msg
                    
                    response = self.client.models.generate_content(
                        model=self.model,
                        contents=full_prompt,
                        config=types.GenerateContentConfig(
                            response_mime_type="application/json",
                            temperature=0.0
                        )
                    )
                    
                    if response.text:
                        answer = response.text
                        results_json = json.loads(answer)
                        
                        # Process results (Shared logic)
                        final_results = [None] * len(items_list)
                        
                        # Handle wrapped
                        if isinstance(results_json, dict) and 'items' in results_json:
                            results_json = results_json['items']
                            
                        if isinstance(results_json, list):
                            for res in results_json:
                                idx = res.get('index', -1) - 1
                                cat_name = res.get('category', '').strip()
                                clean_name = res.get('clean_name', '').strip()
                                
                                if 0 <= idx < len(items_list):
                                    cat_norm = cat_name.lower().strip()
                                    if cat_norm in self.subcategories_map:
                                        result_obj = self.subcategories_map[cat_norm].copy()
                                        result_obj['ai_clean_name'] = clean_name
                                        final_results[idx] = result_obj
                                        
                                        p_norm = items_list[idx]['product'].lower().strip()
                                        c_norm = items_list[idx]['context'].lower().strip()
                                        self.cache[f"{p_norm}|{c_norm}"] = self.subcategories_map[cat_norm]
                        return final_results
                    else:
                        print(f"[Categorizer] Google Empty Response")
                        return [None] * len(items_list)
                        
                except Exception as e:
                    print(f"[Categorizer] Google SDK Error: {e}")
                    # If rate limit (429) happens, the SDK raises an exception.
                    # We catch it here.
                    return [None] * len(items_list)

            else:
                # OLLAMA CALL (Existing logic)
                # Use chat endpoint for potentially better instruction following
                payload = {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg}
                    ],
                    "stream": False,
                    "options": {"temperature": 0},
                    "format": "json"
                }
                
                # Switch to chat endpoint
                response = requests.post(f"{self.ollama_url}/api/chat", json=payload)
                
                if response.status_code == 200:
                    # Chat response structure is slightly different
                    answer = response.json().get("message", {}).get("content", "").strip()
                    
                    # Robust JSON extraction:
                    try:
                        # 1. If text contains ```json ... ``` block
                        import re
                        json_block = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', answer, re.DOTALL)
                        if json_block:
                            answer = json_block.group(1)
                        else:
                            # 2. Try finding first '[' and last ']'
                            start = answer.find('[')
                            end = answer.rfind(']')
                            if start != -1 and end != -1:
                                answer = answer[start:end+1]
                        
                        results_json = json.loads(answer)
                        
                        # Map results back to order
                        # The model might return a list of objects.
                        # We need to map index back to original list.
                        
                        final_results = [None] * len(items_list)
                        
                        # Handle if model returns wrapped json
                        if isinstance(results_json, dict) and 'items' in results_json:
                            results_json = results_json['items']
                            
                        if isinstance(results_json, list):
                            for res in results_json:
                                idx = res.get('index', -1) - 1 # 1-based to 0-based
                                cat_name = res.get('category', '').strip()
                                # NEW: Extract clean_name (Ollama support)
                                clean_name = res.get('clean_name', '').strip()
                                
                                if 0 <= idx < len(items_list):
                                    # Validate against map
                                    cat_norm = cat_name.lower().strip()
                                    if cat_norm in self.subcategories_map:
                                        # Copy base info from DB map
                                        result_obj = self.subcategories_map[cat_norm].copy()
                                        # Add the AI generated clean name
                                        result_obj['ai_clean_name'] = clean_name
                                        
                                        final_results[idx] = result_obj
                                        
                                        p_norm = items_list[idx]['product'].lower().strip()
                                        c_norm = items_list[idx]['context'].lower().strip()
                                        self.cache[f"{p_norm}|{c_norm}"] = self.subcategories_map[cat_norm]
                        
                        return final_results
                        
                    except json.JSONDecodeError:
                        print(f"[Categorizer] Failed to parse JSON response from batch.")
                        return [None] * len(items_list)
                else:
                    print(f"[Categorizer] Ollama Error: {response.status_code}")
                    return [None] * len(items_list)
                
        except Exception as e:
            print(f"[Categorizer] Connection Error: {e}")
            return [None] * len(items_list)


    def _ask_ollama(self, product_name, original_context):
        """Query Ollama to pick the best subcategory."""
        
        valid_options = [data['nombre_subcategoria'] for data in self.subcategories_map.values()]
        valid_options_str = ", ".join(valid_options)
        
        prompt = f"""
        You are an expert supplement classifier.
        Product: "{product_name}"
        Context: "{original_context}"
        
        Task: Classify this product into one of the following EXACT categories:
        [{valid_options_str}]
        
        Rules:
        1. Reply ONLY with the exact category name from the list.
        2. If it's a protein, check if it's Isolate, Hydrolyzed, etc.
        3. If unsure, pick the closest general category (e.g. 'Multivitamínicos').
        4. Do not add explanation or quotes.
        """
        
        # Helper to try generation
        try:
            payload = {
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0} # Deterministic
            }
            response = requests.post(f"{self.ollama_url}/api/generate", json=payload)
            if response.status_code == 200:
                answer = response.json().get("response", "").strip()
                
                # Cleanup answer (remove potential quotes/periods)
                answer_clean = answer.replace('"', '').replace('.', '').lower().strip()
                
                # Validate against map
                if answer_clean in self.subcategories_map:
                    print(f"[Categorizer] Ollama classified '{product_name}' -> '{self.subcategories_map[answer_clean]['nombre_subcategoria']}'")
                    return self.subcategories_map[answer_clean]
                else:
                    print(f"[Categorizer] Ollama failed: Returned '{answer}' which is not in map.")
                    return None
            else:
                print(f"[Categorizer] Ollama Error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"[Categorizer] Connection Error: {e}")
            return None

# Simple test block
if __name__ == "__main__":
    cat = ProductCategorizer()
    print("\n--- Testing ---")
    test_products = [
        ("Gabrielo Whey Isolate 5lb", "Proteinas"),
        ("Creatina Monohidrato 300g", "Creatinas"), 
        ("Multivitaminico One A Day", "Vitaminas"),
    ]
    
    for p, s in test_products:
        res = cat.classify_product(p, s)
        print(f"In: {p} ({s}) -> Out: {res['nombre_subcategoria'] if res else 'None'}")
