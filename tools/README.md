# Documentación de Scripts de Herramientas (`tools/`)

Este directorio contiene scripts de utilidad escritos en Python para realizar tareas de mantenimiento de base de datos, corrección de archivos, depuración de lógica y migración de datos.

A continuación se detalla la función de cada script y cómo utilizarlo.

## 🛠️ Scripts de Mantenimiento de Base de Datos

### 1. `clear_database.py`
**Función:**
Elimina **TODOS** los datos de las tablas principales de la base de datos (`productos`, `marcas`, `categorias`, `tiendas`, `precios`, etc.). Utiliza `TRUNCATE ... CASCADE` para limpiar las tablas y reiniciar los contadores de ID.

**Uso:**
```bash
python tools/clear_database.py
```
> ⚠️ **Advertencia:** Esta acción es destructiva e irreversible. El script pedirá confirmación ('si') antes de proceder.

### 2. `add_unique_constraints.py`
**Función:**
Aplica restricciones `UNIQUE` a las columnas de nombre en las tablas `categorias`, `marcas` y `tiendas` para asegurar que no se inserten duplicados a nivel de base de datos.

**Uso:**
```bash
python tools/add_unique_constraints.py
```

### 3. `fix_product_duplicates.py`
**Función:**
Detecta y fusiona productos duplicados en la tabla `productos`.
*   Identifica grupos de duplicados (mismo nombre y marca).
*   Selecciona un producto "maestro" basado en un puntaje de calidad (prioridad de categoría, penalización por palabras clave como "oferta").
*   Reasigna los enlaces de tiendas (`producto_tienda`) e historias de precios al maestro.
*   Elimina los productos duplicados sobrantes.

**Uso:**
```bash
python tools/fix_product_duplicates.py
```

## 📂 Scripts de Archivos e Imágenes

### 4. `upload_to_s3.py`
**Función:**
Sube imágenes locales almacenadas en `assets/img` a un bucket de AWS S3.
*   Verifica si la imagen ya existe en S3 para evitar resubidas.
*   Actualiza las columnas `url_imagen` y `url_thumb_imagen` en la base de datos con la nueva URL de S3.
*   Mantiene la estructura de carpetas local en el bucket.

**Uso:**
Requiere credenciales AWS en `.env`.
```bash
python tools/upload_to_s3.py
```

### 5. `fix_gzip_images.py`
**Función:**
Escanea el directorio `assets/img` buscando imágenes que tienen extensión de imagen (.jpg, .png) pero que en realidad están comprimidas con GZIP (magic bytes `1f 8b`). Las descomprime in-place para que sean imágenes válidas.

**Uso:**
```bash
python tools/fix_gzip_images.py
```

## 🔍 Scripts de Análisis y Depuración

### 6. `evaluate_matches.py`
**Función:**
Analiza los archivos CSV de productos normalizados (`processed_data/fuzzy_matched/`) para evaluar la efectividad del "Fuzzy Matching".
*   Muestra estadísticas de reducción de productos.
*   Lista los clústers de productos que han sido fusionados bajo un mismo nombre normalizado.

**Uso:**
```bash
python tools/evaluate_matches.py
```

### 7. `debug_insertion_logic.py`
**Función:**
Simula la lógica de inserción de productos sin conectar a la base de datos real.
*   Lee un CSV procesado.
*   Filtra por un término (ej. "Foodtech") y simula cómo se crearían los IDs y las relaciones si se ejecutara la inserción.
*   Útil para verificar lógica antes de correr `data_insertion.py`.

**Uso:**
```bash
python tools/debug_insertion_logic.py
```

### 8. `inspect_prices.py`
**Función:**
Inspecciona un archivo CSV de productos normalizados buscando un término específico (hardcoded, ej: "Foodtech") para mostrar una tabla comparativa de precios encontradas en diferentes sitios para esos productos.

**Uso:**
```bash
python tools/inspect_prices.py
```

### 9. `query_db_debug.py`
**Función:**
Realiza consultas directas a la base de datos para inspeccionar un producto específico.
*   Busca por nombre (ej. ilike '%AMINO WHEY...%').
*   Muestra IDs, relaciones con tiendas y el historial de precios almacenado.

**Uso:**
```bash
python tools/query_db_debug.py
```
