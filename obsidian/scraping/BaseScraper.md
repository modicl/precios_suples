# BaseScraper

**Archivo:** `scrapers_v2/BaseScraper.py`

Clase base Playwright para todos los scrapers de tiendas. Cada scraper hereda de esta clase e implementa `extract_process(page)` como generador.

---

## Anti-Bot

Playwright lanza Chromium con configuraciones que reducen la huella de automatización:

```python
launch_args = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]

context = browser.new_context(
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0 Safari/537.36",
    locale="es-CL",
    timezone_id="America/Santiago",
    viewport={"width": 1366, "height": 768},  # resolución más frecuente en Chile (StatCounter 2024)
)

# Inyectado antes de cualquier script del sitio:
context.add_init_script("""
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['es-CL', 'es', 'en-US', 'en'] });
    window.chrome = { runtime: {} };
""")
```

---

## Métodos Clave

### `run()`
Inicia el browser, abre el CSV de salida, consume el generador `extract_process()` y guarda cada producto con `f.flush()` (guardado progresivo — no se pierde nada si el scraper falla a mitad).

### `extract_process(page)` — abstracto
Debe ser implementado por cada scraper hijo como generador (`yield`). Retorna dicts con las columnas del CSV.

### `enrich_brand(brand, product_name, scan_title=False)`
Normaliza la marca usando `BrandClassifier`. Si `brand` es `N/D`/vacío y `scan_title=True`, intenta extraer la marca del título del producto (útil para scrapers sin marca en el DOM, ej. DrSimi). Por defecto `scan_title=False` para scrapers deterministas.

### `clean_text(text)`
Elimina emojis y caracteres no deseados. Preserva acentos, `/`, `-`, `.`, `,`, `+`, `%`.

### `clean_description(text)`
Normaliza espacios raros en descripciones de `inner_text()`. Convierte `&nbsp;` (U+00A0), tabs, zero-width spaces y separadores Unicode a espacios normales. Colapsa líneas en blanco consecutivas.

### `download_image(url, subfolder)` — Estrategia Fire and Forget
Ver sección "Gestión de Imágenes S3" abajo.

---

## Gestión de Imágenes S3

```
1. Hash MD5(url sin query params) → filename estable entre ejecuciones
2. Cargar inventario lazy del bucket (una vez por subfolder, en caché de memoria)
3. Si filename en caché "resized/" → retornar URL canónica de resized (0 latencia)
4. Si no → descargar a RAM → subir a "originals/" → retornar URL de resized
   (La imagen dará 404 en resized hasta que Lambda la procese)
```

**Nota:** La BD guarda siempre la URL de `resized/` como URL canónica, aunque la imagen todavía no exista allí. Esto es intencional: Lambda procesa los originals y genera los resized en background.

Fallback local (sin credenciales AWS): guarda en `assets/img/{subfolder}/`.

---

## Logging

Cada scraper escribe en `logs/{site_name}/execution_{YYYY-MM-DD}.log`. El logger evita duplicar handlers si la clase se instancia varias veces en el mismo proceso.

---

## CSV Output

Archivo: `raw_data/productos_{site_name}_{YYYY-MM-DD}.csv`

Columnas: `date, site_name, category, subcategory, product_name, brand, price, link, rating, reviews, active_discount, thumbnail_image_url, image_url, sku, description`

---

## Referencias

- [[scraping/SharedSeenUrls]]
- [[arquitectura/Flujo de Datos]]
- [[bugs/Filtro Brand ND]]
