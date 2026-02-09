# Changelog - Pipeline V2

## [2026-02-09 23:45] - Optimización y Robustez Final

### Problemas Detectados
1.  **Duplicación de "Wild Foods"**: Productos como "Wild Protein" y "Wild Protein Vegana" aparecían separados debido a diferencias en el nombre ("Barra de proteína", "45g", etc.) y fragmentación de marcas ("Wild Foods", "Wild Protein", "WILD").
2.  **Violación de Claves Foráneas**: Al unificar marcas, el borrado de productos duplicados fallaba porque `click_analytics` referenciaba los IDs antiguos.
3.  **Errores de Atributos**: El limpiador de nombres fallaba (`NoneType has no attribute lower`) cuando el resultado de la limpieza era vacío.

### Soluciones Implementadas

#### 1. Unificación de Marcas (Automatizada)
*   **Script**: `step2_clean_names.py`
*   **Cambio**: Se implementó una lógica agresiva para la marca "WILD" (y sus variantes).
    *   Se eliminan palabras de relleno: "Barra de proteína", "Protein Bar", "Original", "Sabor".
    *   Se eliminan pesos específicos de barras ("45g", "40g") para agrupar sabores.
    *   Se eliminan sufijos como ", wild".
*   **Resultado**: "Wild Protein Barra de Proteína 45 gr" -> **"Wild Protein"**.

#### 2. Migración Segura de Datos (`unify_brands.py`)
*   **Script**: `debug_tools/unify_brands.py`
*   **Cambio**: Se agregó lógica para migrar `click_analytics` antes de eliminar productos duplicados durante la fusión de marcas.
*   **Resultado**: Fusión exitosa de "Wild Foods" y "Wild Protein" en la marca maestra "WILD" sin errores de FK.

#### 3. Robustez en Limpieza (`step2_clean_names.py`)
*   **Cambio**: Se agregaron chequeos de nulidad (`if not cleaned: return ""`) antes de aplicar métodos de string como `.title()`.
*   **Resultado**: Prevención de crashes por nombres vacíos.

#### 4. Organización del Proyecto
*   **Estructura**:
    *   `data/1_classified`: Salida de IA.
    *   `data/2_cleaned`: Salida de limpieza determinista.
    *   `data/3_normalized`: Salida de Fuzzy Match.
*   **Flujo**: Scripts renombrados secuencialmente (Step 1, 2, 3, 4, 5, 6).

#### 5. Auditoría del Diccionario de Marcas
*   **Script**: `audit_brands.py` (ejecutado manualmente para generar `marcas_v2.csv` y reemplazar el original).
*   **Problema**: El diccionario original contenía términos que NO eran marcas reales, como "Amino", "Beef", "ISO 100", "ISOFIT", "Elite", "Gold Standard".
*   **Consecuencia**: Al limpiar nombres, el script borraba partes vitales del producto (ej. "Dymatize Iso 100" -> Borraba "Dymatize" y "Iso 100" -> quedaba ""). O "Wild Protein" -> Borraba "Wild Protein" -> Quedaba "".
*   **Solución**: Se eliminaron ~80 entradas inválidas del diccionario.
*   **Resultado**: Ahora el limpiador borra la marca ("Dymatize") pero respeta el modelo ("Iso 100"), permitiendo una normalización correcta.

---

## [2026-02-09 18:30] - Implementación de Pipeline V2 Modular

### Problemas Detectados
1.  **Lentitud**: El scraping + IA en tiempo real era inviable.
2.  **Bloqueos de BD**: La inserción fila por fila causaba bloqueos y lentitud.
3.  **Datos Sucios**: Nombres con marcas repetidas ("Dymatize Iso 100 Dymatize").

### Soluciones Implementadas
1.  **Desacople**:
    *   Scraping Offline (Rápido).
    *   Procesamiento Batch (IA + Python).
2.  **Batch IA**: `step1_ai_classification.py` usa Ollama en lotes de 50 para clasificar y limpiar nombres.
3.  **Inserción Bulk**: `step4_db_insertion.py` inserta miles de registros en 3 consultas masivas (Upsert).
4.  **Limpieza Determinista**: `step2_clean_names.py` usa un diccionario auditado (`marcas_dictionary.csv`) para borrar marcas del nombre.

### Estado Final
El sistema es capaz de procesar ~3000 productos en minutos, con una tasa de éxito de inserción del 100% y una alta calidad de agrupación de productos.
