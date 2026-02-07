import subprocess
import sys
import time

def run_post_processing_v2():
    print("\n--- Iniciando Procesamiento V2 y Actualización de Base de Datos (Optimized) ---")
    
    # All scripts are now in data_processing_v2
    # fix_product_duplicates was moved from tools/ to data_processing_v2/
    steps = [
        ("Procesando Datos (Consolidación)", "data_processing_v2/process_data.py"),
        ("Normalizando Productos (Fuzzy Match)", "data_processing_v2/normalize_products.py"),
        ("Insertando Datos en BD (Bulk Optimized)", "data_processing_v2/data_insertion.py"),
        ("Limpiando Marcas", "data_processing_v2/clean_brands.py"),
        ("Limpiando Categorías", "data_processing_v2/clean_categories.py"),
        ("Clasificando Productos (Vegano/Mujer)", "data_processing_v2/classify_products.py"),
        ("Deduplicando Productos", "data_processing_v2/fix_product_duplicates.py"),
        ("Refrescando Vistas Materializadas", "data_processing_v2/refresh_materialized_views.py")
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

    print("\n--- Procesamiento V2 Completo ---")

if __name__ == "__main__":
    run_post_processing_v2()
