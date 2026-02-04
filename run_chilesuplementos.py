import subprocess
import sys
import time

def run_scrapers():
    scrapers = [
        "scrapers/ChileSuplementosScraperPart1.py",
        "scrapers/ChileSuplementosScraperPart2.py"
    ]

    processes = []
    print(f"--- Iniciando ChileSuplementos Scrapers (Split) ---")

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

    print("\nProceso lanzado. Esperando a que termine...\n")

    # Wait for all processes to complete
    completed_count = 0
    for scraper, p in processes:
        p.wait()
        completed_count += 1
        print(f"Finalizado ({completed_count}/{len(scrapers)}): {scraper}")

    end_time = time.time()
    duration = end_time - start_time
    print(f"\n--- ChileSuplementos ha finalizado en {duration:.2f} segundos ---")

    print("\n--- Ejecución de ChileSuplementos Completa (Sin Post-Procesamiento) ---")

if __name__ == "__main__":
    run_scrapers()
