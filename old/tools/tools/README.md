# Documentación de Scripts de Herramientas (`tools/`)

Este directorio contiene scripts de utilidad escritos en Python para realizar tareas de mantenimiento de base de datos, limpieza de datos, corrección de archivos, depuración de lógica y migración de datos.

A continuación se detalla la función de cada script y cómo utilizarlo.

## 🧹 Scripts de Limpieza y Deduplicación Avanzada (Nuevos)

### 1. `clean_db_emojis.py`
**Función:**
Elimina emojis y caracteres no deseados de las columnas de texto en la base de datos (Marcas, Categorías, Subcategorías y Nombres de Productos). Además, fusiona registros que quedan idénticos tras la limpieza (ej: "Creatina ⚡" y "Creatina" se convierten en uno solo).
*   Preserva acentos y puntuación básica.
*   Actualiza automáticamente las referencias (Foreign Keys) en tablas dependientes.

**Uso:**
```bash
python tools/clean_db_emojis.py
```

### 2. `evaluate_db_clusters.py`
**Función:**
Script de **solo lectura** que analiza la base de datos en busca de productos duplicados utilizando lógica avanzada de "Fuzzy Matching" (similitud difusa).
*   Detecta productos con nombres similares (ej: "Whey Protein 1kg" vs "Whey Protein 1000g").
*   Considera atributos clave: gramaje, sabor, marca y tipo de pack.
*   Genera un reporte visual con `rich` mostrando qué productos se fusionarían.

**Uso:**
```bash
python tools/evaluate_db_clusters.py
```

### 3. `deduplicate_db_products.py`
**Función:**
Ejecuta la **fusión física** de los duplicados detectados por la lógica de clustering.
*   **Modo Seguro (Dry-Run):** Por defecto, solo simula las operaciones y muestra qué pasaría.
*   **Gestión de Conflictos:** Si dos productos duplicados están en la misma tienda, fusiona sus historiales de precios inteligentemente.
*   **Atomicidad:** Usa transacciones SQL; si algo falla, revierte todos los cambios.

**Uso:**
Simulacro (Recomendado primero):
```bash
python tools/deduplicate_db_products.py
```
Aplicar cambios reales:
```bash
python tools/deduplicate_db_products.py --execute
```

### 4. `clean_exact_duplicates.py`
**Función:**
Elimina filas redundantes en la tabla `historia_precios`.
*   Detecta registros que tienen idéntico `producto`, `tienda`, `fecha` y `precio`.
*   Conserva una sola copia y elimina el resto para liberar espacio en disco.

**Uso:**
Simulacro:
```bash
python tools/clean_exact_duplicates.py
```
Aplicar cambios:
```bash
python tools/clean_exact_duplicates.py --execute
```
*Sugerencia: Ejecutar `VACUUM FULL historia_precios;` en PostgreSQL tras su uso.*

---

## 🛠️ Scripts de Mantenimiento General

### 5. `clear_database.py`
**Función:**
Elimina **TODOS** los datos de las tablas principales de la base de datos (`productos`, `marcas`, `categorias`, `tiendas`, `precios`, etc.). Utiliza `TRUNCATE ... CASCADE`.

**Uso:**
```bash
python tools/clear_database.py
```
> ⚠️ **Advertencia:** Acción destructiva e irreversible.

### 6. `add_unique_constraints.py`
**Función:**
Aplica restricciones `UNIQUE` a las columnas de nombre en las tablas principales para prevenir duplicados futuros a nivel de esquema.

**Uso:**
```bash
python tools/add_unique_constraints.py
```

### 7. `fix_product_duplicates.py` (Legacy)
**Función:**
Versión anterior del script de deduplicación. Se recomienda usar `deduplicate_db_products.py` para una lógica más robusta con fuzzy matching.

## 📂 Scripts de Archivos e Imágenes

### 8. `upload_to_s3.py`
**Función:**
Sube imágenes locales almacenadas en `assets/img` a un bucket de AWS S3 y actualiza las URLs en la base de datos.

**Uso:**
Requiere credenciales AWS en `.env`.
```bash
python tools/upload_to_s3.py
```

### 9. `fix_gzip_images.py`
**Función:**
Repara imágenes que tienen extensión `.jpg/.png` pero están comprimidas con GZIP internamente.

**Uso:**
```bash
python tools/fix_gzip_images.py
```

## 🔍 Scripts de Análisis y Depuración

### 10. `evaluate_matches.py`
**Función:**
Analiza los archivos CSV de productos normalizados para evaluar la efectividad del matching antes de la inserción.

**Uso:**
```bash
python tools/evaluate_matches.py
```

### 11. `debug_insertion_logic.py`
**Función:**
Simula la lógica de inserción de productos. Útil para desarrolladores al probar cambios en `data_insertion.py`.

**Uso:**
```bash
python tools/debug_insertion_logic.py
```

### 12. `inspect_prices.py`
**Función:**
Inspecciona un archivo CSV buscando precios para un término específico.

**Uso:**
```bash
python tools/inspect_prices.py
```

### 13. `query_db_debug.py`
**Función:**
Herramienta rápida para consultar la BD por consola buscando productos por nombre y viendo sus relaciones.

**Uso:**
```bash
python tools/query_db_debug.py
```
