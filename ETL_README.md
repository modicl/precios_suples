# Documentación del Proceso ETL: Precios de Suplementos

Este documento detalla la arquitectura y el flujo de trabajo **ETL (Extract, Transform, Load)** utilizado en el proyecto `precios_suples`. El objetivo del sistema es monitorear, limpiar y estructurar precios de suplementos deportivos de múltiples tiendas en Chile para detectar oportunidades y tendencias.

---

## 🏗️ Resumen de Arquitectura

El flujo de datos sigue estos pasos:

1.  **Extracción (Scraping):** 16 bots navegan sitios web diariamente usando Playwright.
2.  **Transformación (Cleaning & Normalization):** Limpieza de emojis, estandarización de unidades (kg, lb, gr) y detección de atributos.
3.  **Carga (Loading):** Inserción inteligente en PostgreSQL, manejando historiales de precios y relaciones entre productos y tiendas.
4.  **Mantenimiento (Deduplication):** Procesos batch para fusionar duplicados usando Fuzzy Matching.

---

## 1. Extracción (Scrapers)

Ubicación: `scrapers/`

El proyecto utiliza **Playwright** para la navegación web, permitiendo manejar sitios dinámicos (SPA, React, Vtex, Shopify). Todos los scrapers heredan de una clase padre robusta.

### `BaseScraper.py`
Es el núcleo de la extracción. Provee funcionalidades comunes:
*   **Manejo de Browser:** Inicialización de Chrome/Firefox headless.
*   **Navegación Resiliente:** Reintentos automáticos, manejo de Timeouts.
*   **Scroll Infinito:** Método `scroll_to_bottom` para cargar grillas dinámicas "Lazy Load".
*   **Limpieza en Origen:** Método `clean_text()` que elimina emojis y caracteres invisibles antes de que los datos toquen la memoria.
*   **Exportación:** Guarda los datos crudos en CSV (`data/raw/`) como respaldo intermedio.

### Listado de Scrapers
Cada scraper está especializado en la estructura HTML de una tienda específica:

| Scraper | Tienda | Tecnología Detectada | Desafíos Particulares |
| :--- | :--- | :--- | :--- |
| `AllNutrition` | All Nutrition | Vtex | Carga dinámica por scroll. |
| `DrSimi` | Dr. Simi | Vtex Legacy | Selectores de marca no estandarizados. |
| `Decathlon` | Decathlon | Custom | Requiere parseo profundo de atributos JSON-LD. |
| `WildFoods` | Wild Foods | Shopify | Nombres de productos suelen incluir "Pack". |
| `SupleTech` | Suple Tech | WooCommerce | Variaciones de precios por sabor. |
| *...y 11 más* | (Ver carpeta `scrapers/`) | | |

**Datos Extraídos por Producto:**
*   Nombre Original
*   Marca
*   Categoría y Subcategoría
*   Precio Normal y Precio Oferta
*   URL del producto y URL de la imagen

---

## 2. Transformación (Limpieza y Normalización)

Ubicación: `data_processing/normalize_products.py`

Antes o durante la inserción, los datos crudos pasan por un proceso de refinamiento para asegurar consistencia.

### Limpieza de Texto
*   **Eliminación de Emojis:** Se eliminan caracteres no alfanuméricos (ej: "Creatina 🔥" -> "Creatina").
*   **Espaciado:** Se corrigen dobles espacios y trim de bordes.

### Extracción de Atributos (Feature Extraction)
El sistema intenta "entender" qué es el producto basándose en su nombre.
1.  **Gramaje/Peso:** Detecta patrones como `2lb`, `1 kg`, `500g`, `90 caps`.
    *   *Normalización:* "2.2 libras", "2,2 lbs", "1kg" se tratan matemáticamente para comparación.
2.  **Sabor:** Detecta sabores comunes ("Chocolate", "Vainilla", "Blue Raspberry") para diferenciarlos o agruparlos.
3.  **Packs:** Detecta si es un "Pack", "Dúo", "3x", etc.

---

## 3. Diferenciación de Productos (Lógica de Identidad)

Uno de los mayores desafíos es determinar si el producto "Whey Gold 5lbs" de la Tienda A es el mismo que "Gold Standard Whey 2.2kg" de la Tienda B.

### Estrategia Actual
El sistema utiliza un enfoque híbrido:

1.  **Normalización Determinista:**
    *   Se genera un "nombre limpio" (lowercase, sin puntuación, sin "promo", sin gramaje variable).
    *   Se compara: `Marca` + `Nombre Normalizado` + `Subcategoría`.
    
2.  **Deduplicación Difusa (Fuzzy Matching):**
    *   Ubicación: `tools/evaluate_db_clusters.py`
    *   Como el scraping diario puede introducir ligeras variaciones, se ejecuta un proceso de mantenimiento.
    *   **Algoritmo:** Utiliza `rapidfuzz` (Distancia de Levenshtein) agrupando por Marca y Subcategoría.
    *   **Criterios de Fusión:** Dos productos se consideran el mismo si:
        *   Similitud de texto > 87%.
        *   Mismo gramaje detectado (o ambos nulos).
        *   Mismo tipo de pack.
        *   No contienen palabras clave críticas opuestas (ej: "Isolate" vs "Concentrate").

---

## 4. Carga de Datos (Insertion)

Ubicación: `data_processing/data_insertion.py`

Los datos se insertan en una base de datos **PostgreSQL** utilizando SQLAlchemy.

### Schema Relacional (Simplificado)

*   `marcas`: (id, nombre) - *Unique*
*   `tiendas`: (id, nombre) - *Unique*
*   `productos`: (id, nombre, id_marca, id_subcategoria...) - *Producto Maestro*
*   `producto_tienda`: (id, id_producto, id_tienda, url) - *Tabla Pivot: Un producto puede estar en muchas tiendas.*
*   `historia_precios`: (id, id_producto_tienda, fecha, precio) - *Registro temporal.*

### Lógica de Inserción
1.  **Check Existencia:** ¿Existe la Marca? ¿Existe la Tienda? (Si no, crear).
2.  **Búsqueda de Producto:**
    *   Busca coincidencia exacta o normalizada en la tabla `productos`.
    *   Si existe -> Obtiene su ID.
    *   Si no existe -> Crea nuevo registro en `productos`.
3.  **Vinculación Tienda:**
    *   Verifica si existe la relación en `producto_tienda` (este producto en esta tienda específica).
    *   Si no, la crea.
4.  **Registro de Precio:**
    *   Inserta una nueva fila en `historia_precios` con la fecha actual.
    *   *Optimización:* Periódicamente se ejecuta `tools/clean_exact_duplicates.py` para borrar registros redundantes si el precio no cambió y el scraper corrió varias veces el mismo día.

---

## 5. Mantenimiento y Herramientas

El sistema cuenta con una suite de herramientas en la carpeta `tools/` para mantener la salud de los datos:

*   **`deduplicate_db_products.py`**: Fusiona productos duplicados que la lógica de inserción no detectó, unificando historiales de precios.
*   **`clean_db_emojis.py`**: Sanea la base de datos histórica eliminando caracteres basura.
*   **`inspect_prices.py`**: Permite auditar visualmente los precios recolectados.

---

## Resumen del Flujo Diario

1.  🕒 **Cronjob:** Ejecuta los 16 scrapers.
2.  💾 **Raw Data:** Se generan 16 archivos CSV en `data/raw/`.
3.  ⚙️ **Processing:** Se ejecuta script de inserción que lee CSVs y puebla la BD.
4.  🧹 **Maintenance (Post-Process):**
    *   Se corre `deduplicate_db_products.py` para fusionar variaciones nuevas.
    *   Se corre `clean_exact_duplicates.py` para ahorrar espacio.
