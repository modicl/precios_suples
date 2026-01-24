import subprocess
import sys
import time

def run_scrapers():
    scrapers = [
        "scrapers/AllNutritionScraper.py",
        "scrapers/ChileSuplementosScraper.py",
        "scrapers/OneNutritionScraper.py",
        "scrapers/SupleStoreScraper.py",
        "scrapers/SupleTechScraper.py",
        "scrapers/SuplesScraper.py",
        "scrapers/StrongestScraper.py",
        "scrapers/SuplementosBullChileScraper.py",
        "scrapers/MuscleFactoryScraper.py",
        "scrapers/FitMarketChileScraper.py",
        "scrapers/DecathlonScraper.py",
        "scrapers/DrSimiScraper.py",
        "scrapers/CruzVerdeScraper.py",
        "scrapers/SuplementosMayoristasScraper.py",
        "scrapers/FarmaciaKnopScraper.py"
    ]

    processes = []
    print(f"--- Iniciando {len(scrapers)} scrapers en paralelo ---")

    start_time = time.time()

    for scraper in scrapers:
        try:
            # subprocess.Popen runs the process in the background (non-blocking)
            # sys.executable ensures we use the same python interpreter as this script
            p = subprocess.Popen([sys.executable, scraper])
            processes.append((scraper, p))
            print(f"Started: {scraper} (PID: {p.pid})")
        except Exception as e:
            print(f"Error starting {scraper}: {e}")

    print("\nTodos los procesos han sido lanzados. Esperando a que terminen...\n")

    # Wait for all processes to complete
    completed_count = 0
    for scraper, p in processes:
        p.wait()
        completed_count += 1
        print(f"Finalizado ({completed_count}/{len(scrapers)}): {scraper}")

    end_time = time.time()
    duration = end_time - start_time
    print(f"\n--- Todos los scrapers han finalizado en {duration:.2f} segundos ---")

    # --- POST-PROCESSING & DB UPDATE ---
    print("\n--- Iniciando Procesamiento y Actualización de Base de Datos ---")
    
    steps = [
        ("Procesando Datos (Consolidación)", "data_processing/process_data.py"),
        ("Normalizando Productos (Fuzzy Match)", "data_processing/normalize_products.py"),
        ("Insertando Datos en BD", "data_processing/data_insertion.py"),
        ("Limpiando Marcas", "data_processing/clean_brands.py"),
        ("Limpiando Categorías", "data_processing/clean_categories.py")
    ]

    for description, script_path in steps:
        print(f"\n> {description} ({script_path})...")
        try:
            # Run sequentially, checking for errors
            result = subprocess.run([sys.executable, script_path], check=True)
            print(f"  [OK] {description} completado.")
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] Falló {description}: {e}")
            print("  Deteniendo la ejecución de pasos posteriores.")
            break
        except Exception as e:
            print(f"  [ERROR] Error inesperado en {description}: {e}")
            break

    print("\n--- Ejecución completa ---")

if __name__ == "__main__":
    run_scrapers()
