import subprocess
import sys
import time

def run_post_processing():
    print("\n--- Iniciando Procesamiento y Actualización de Base de Datos (Fast Scrapers) ---")
    
    steps = [
        ("Procesando Datos (Consolidación)", "data_processing/process_data.py"),
        ("Normalizando Productos (Fuzzy Match)", "data_processing/normalize_products.py"),
        ("Insertando Datos en BD", "data_processing/data_insertion.py"),
        ("Limpiando Marcas", "data_processing/clean_brands.py"),
        ("Limpiando Categorías", "data_processing/clean_categories.py"),
        ("Clasificando Productos (Vegano/Mujer)", "data_processing/classify_products.py"),
        ("Deduplicando Productos", "tools/fix_product_duplicates.py"),
        ("Refrescando Vistas Materializadas", "data_processing/refresh_materialized_views.py")
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

    print("\n--- Procesamiento y Actualización Completa ---")

if __name__ == "__main__":
    run_post_processing()
