import subprocess
import sys
import time
import glob
import csv
import os

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

    # --- Post-procesamiento: Unir archivos CSV ---
    print("\n--- Uniendo archivos CSV parciales ---")
    current_date = time.strftime("%Y-%m-%d")
    output_dir = "raw_data"
    
    # Patrón para encontrar los archivos parciales
    
    final_filename = f"productos_chilesuplementos_{current_date}.csv"
    final_filepath = os.path.join(output_dir, final_filename)
    
    partial_files = glob.glob(os.path.join(output_dir, f"productos_chilesuplementos_*_{current_date}.csv"))
    # Excluir el archivo final si ya existe para evitar duplicación
    partial_files = [f for f in partial_files if os.path.basename(f) != final_filename]
    
    if not partial_files:
        print("[AMARILLO] No se encontraron archivos parciales para unir.")
    else:
        print(f"Archivos encontrados: {[os.path.basename(f) for f in partial_files]}")
        
        all_rows = []
        headers = []
        
        for p_file in partial_files:
            try:
                with open(p_file, mode='r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    if not headers:
                        headers = reader.fieldnames
                    
                    for row in reader:
                        all_rows.append(row)
            except Exception as e:
                print(f"[ROJO] Error leyendo {p_file}: {e}")

        if headers:
            try:
                with open(final_filepath, mode='w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(all_rows)
                print(f"[VERDE] Archivo unificado creado exitosamente: {final_filepath} ({len(all_rows)} productos)")
                
                # Eliminar archivos parciales
                for p_file in partial_files:
                    try:
                        os.remove(p_file)
                        print(f"Eliminado archivo temporal: {os.path.basename(p_file)}")
                    except Exception as e:
                        print(f"Error eliminando {p_file}: {e}")
                        
            except Exception as e:
                print(f"[ROJO] Error escribiendo archivo final: {e}")
        else:
             print("[AMARILLO] No se encontraron datos para unir.")

    print("\n--- Ejecución de ChileSuplementos Completa (Con Unificación) ---")

if __name__ == "__main__":
    run_scrapers()
