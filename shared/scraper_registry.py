"""
scraper_registry.py — Lista única de scrapers, compartida por RunAll.py (local)
y main.py (Cloud Run). Agregar o quitar tiendas SOLO aquí.
"""

# (nombre_display, script relativo a scrapers_v2/)
# Los runners manejan sus partes en paralelo internamente.
SCRAPERS = [
    ("ChileSuplementos",      "ChileSuplementosScraperRunner.py"),
    ("AllNutrition",          "AllNutritionScraperRunner.py"),
    ("Byon",                  "BYONScraperRunner.py"),
    ("CruzVerde",             "CruzVerdeScraperRunner.py"),
    # Decathlon desactivado (sep 2026): Cloudflare bloquea todo decathlon.cl (403).
    ("DrSimi",                "DrSimiScraper.py"),
    ("FarmaciaKnop",          "FarmaciaKnopScraperRunner.py"),
    ("KoteSport",             "KoteSportScraper.py"),
    ("Suples",                "SuplesScraperRunner.py"),
    ("SuplementosBullChile",  "SuplementosBullChileScraper.py"),
    ("SuplementosMayoristas", "SuplementosMayoristasScraper.py"),
    ("SportNutriShop",        "SportNutriShopScraperRunner.py"),
    ("SupleTech",             "SupleTechScraper.py"),
    ("SupleStore",            "SupleStoreScraper.py"),
    ("OneNutrition",          "OneNutritionScraper.py"),
    ("MuscleFactory",         "MuscleFactoryScraper.py"),
    ("FitMarketChile",        "FitMarketChileScraper.py"),
    ("Strongest",             "StrongestScraper.py"),
    ("WildFoods",             "WildFoodsScraper.py"),
    ("WinklerNutrition",      "WinklerNutritionScraper.py"),
    ("OutletFit",             "OutletFitScraper.py"),
]

# Reemplazos con --api_mode / API_MODE=1 (tiendas VTEX con API directa, sin browser)
SCRAPERS_API_OVERRIDES = {
    "SupleTech":             "SupleTechApiScraper.py",
    "SuplementosMayoristas": "SuplementosMayoristasApiScraper.py",
}

# Scripts que aceptan --headless como argumento CLI
RUNNER_SCRIPTS = {
    "ChileSuplementosScraperRunner.py",
    "AllNutritionScraperRunner.py",
    "BYONScraperRunner.py",
    "SuplesScraperRunner.py",
    "SportNutriShopScraperRunner.py",
    "FarmaciaKnopScraperRunner.py",
    "CruzVerdeScraperRunner.py",
}
