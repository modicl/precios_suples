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
        "scrapers/StrongestScraper.py"
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

if __name__ == "__main__":
    run_scrapers()
