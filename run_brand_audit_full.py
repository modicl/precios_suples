import subprocess
import sys
import time
import os

def run_scrapers_audit():
    # List of scrapers to run (All of them as requested)
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
        "scrapers/FarmaciaKnopScraper.py",
        "scrapers/WildFoodsScraper.py"
    ]

    processes = []
    print(f"--- [AUDIT MODE] Iniciando {len(scrapers)} scrapers en paralelo ---")

    start_time = time.time()

    for scraper in scrapers:
        if os.path.exists(scraper):
            try:
                # subprocess.Popen runs the process in the background (non-blocking)
                p = subprocess.Popen([sys.executable, scraper])
                processes.append((scraper, p))
                print(f"Started: {scraper} (PID: {p.pid})")
            except Exception as e:
                print(f"Error starting {scraper}: {e}")
        else:
            print(f"Warning: Scraper not found: {scraper}")

    print("\nTodos los procesos han sido lanzados. Esperando a que terminen... (Esto puede tardar varios minutos)\n")

    # Wait for all processes to complete
    completed_count = 0
    for scraper, p in processes:
        p.wait()
        completed_count += 1
        print(f"Finalizado ({completed_count}/{len(processes)}): {scraper}")

    end_time = time.time()
    duration = end_time - start_time
    print(f"\n--- Todos los scrapers han finalizado en {duration:.2f} segundos ---")

    # --- DATA CONSOLIDATION ONLY ---
    print("\n--- Consolidando datos para revisión (Sin insertar en BD) ---")
    
    # Run process_data.py to merge raw CSVs into one
    try:
        subprocess.run([sys.executable, "data_processing/process_data.py"], check=True)
        print("  [OK] Datos consolidados exitosamente.")
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Falló la consolidación: {e}")

    print("\n--- Auditoría Lista ---")
    print("Revisa el archivo más reciente en 'processed_data/all_products_YYYY-MM-DD.csv'")
    print("Filtra por fecha de hoy para ver los resultados de esta ejecución.")

if __name__ == "__main__":
    run_scrapers_audit()
