import subprocess
import sys
import time

def run_full_process():
    print("--- Iniciando Proceso Completo de Scraping y Actualización ---")
    start_time_total = time.time()

    # 1. Launch scraper runners in parallel
    scraper_runners = [
        "run_chilesuplementos.py",
        "run_others_scrapers.py"
    ]
    
    processes = []
    
    print("\n--- Lanzando scripts de scraping en paralelo ---")
    for script in scraper_runners:
        try:
            print(f"Iniciando {script}...")
            # We use Popen to run them in parallel
            p = subprocess.Popen([sys.executable, script])
            processes.append((script, p))
        except Exception as e:
            print(f"Error al iniciar {script}: {e}")

    # 2. Wait for scraper runners to finish
    print("\nEsperando a que terminen los scrapers...")
    for script, p in processes:
        p.wait()
        print(f"Finalizado: {script}")

    # 3. Run post-processing
    print("\n--- Scrapers finalizados. Iniciando post-procesamiento ---")
    try:
        subprocess.run([sys.executable, "run_post_processing.py"], check=True)
        print("\n[OK] Run post_processing finalizado exitosamente.")
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] El post-procesamiento falló: {e}")

    end_time_total = time.time()
    duration_total = end_time_total - start_time_total
    print(f"\n--- Proceso Completo Finalizado en {duration_total:.2f} segundos ---")

if __name__ == "__main__":
    run_full_process()
