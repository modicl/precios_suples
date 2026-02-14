import csv
import sys
import os
import pandas as pd

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_processing.brand_matcher import BrandMatcher

def analyze_failures():
    matcher = BrandMatcher()
    input_file = 'no_clasificados.csv'
    output_file = 'analisis_clasificacion.csv'
    
    if not os.path.exists(input_file):
        print(f"Error: No se encontró {input_file}")
        return

    print("Cargando marcas del diccionario...")
    # Verificar si marcas clave están cargadas
    key_brands = ["Innovative Fit", "Activlab", "Perfect Nutrition", "Pronutrition", "Amix", "PVL", "QNT", "FNL", "Natura Vit", "Revitta", "Wild Protein", "Wild Fit"]
    
    print("\n--- Verificación de Marcas Clave en Diccionario ---")
    loaded_brands_lower = [b.lower() for b in matcher._brands]
    for kb in key_brands:
        found = kb.lower() in loaded_brands_lower
        print(f"Marca '{kb}': {'ENCONTRADA' if found else 'NO ESTÁ'}")

    print("\n--- Analizando productos no clasificados ---")
    
    results = []
    
    try:
        with open(input_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                product_name = row.get('product_name', '')
                current_brand = row.get('brand', 'N/D')
                
                # Intentar match de nuevo
                new_match = matcher.get_best_match(product_name)
                
                # Diagnóstico simple
                status = "Mejorado" if new_match != "N/D" else "Sigue N/D"
                
                results.append({
                    'product_name': product_name,
                    'original_brand': current_brand,
                    'new_match': new_match,
                    'status': status
                })
    except Exception as e:
        print(f"Error leyendo CSV: {e}")
        return

    # Guardar resultados
    df_results = pd.DataFrame(results)
    df_results.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # Resumen
    improved = df_results[df_results['status'] == 'Mejorado']
    print(f"\nTotal analizados: {len(df_results)}")
    print(f"Logrados clasificar con lógica actual: {len(improved)}")
    print(f"Aún sin clasificar: {len(df_results) - len(improved)}")
    
    print(f"\nResultados guardados en: {output_file}")
    
    # Mostrar algunos ejemplos de lo que AÚN falla
    failures = df_results[df_results['status'] == 'Sigue N/D'].head(10)
    if not failures.empty:
        print("\n--- Top 10 fallos persistentes ---")
        for _, row in failures.iterrows():
            print(f"Producto: {row['product_name']} | Match: {row['new_match']}")

if __name__ == "__main__":
    analyze_failures()
