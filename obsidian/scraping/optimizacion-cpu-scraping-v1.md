# Optimización de CPU en el scraping (rama `optimizado-v1`)

## Problema
Con `RunAll` sin `--workers`, las ~20 tiendas corrían a la vez y los runners (ver [[division-scrapers-3-partes]]) sumaban partes: se medieron **59 procesos `chrome-headless-shell` (~7,8 GB)** y la CPU (Ryzen 7 9800X3D, 16 hilos) al 99%. Los navegadores sí se cierran (`browser.close()` en `finally` de [[BaseScraper]]); el problema era la concurrencia real.

## Cambios
1. **Límite global de navegadores** — `BrowserSlots` en [[BaseScraper]]. Archivos de lock a nivel SO (`msvcrt`/`fcntl`) en `%TEMP%/precios_suples_browser_slots`; si un proceso muere el SO libera el slot. Cuenta partes de runners, no solo tiendas. Variable `MAX_BROWSERS` (la fija `RunAll --max-browsers`, default = hilos/2; 0 = sin límite). Sin la variable, los scrapers sueltos no tienen límite.
2. **Prioridad baja** — `RunAll._run_one` lanza con `BELOW_NORMAL_PRIORITY_CLASS`; runners, partes, node y Chromium la heredan.
3. **Bloqueo de recursos** (opt-in con `BLOCK_RESOURCES=1`, que activa el `.bat`) — `_install_resource_blocking`:
   - `image` → GIF 1x1 vía `route.fulfill` (no `abort`, para no disparar `onerror` que cambia el `src` por un placeholder).
   - `media` y `font` → abortados. Trackers (GA, GTM, Meta, Hotjar, etc.) → abortados.
   - HTML, CSS, JS y XHR/fetch **no** se tocan.

## Por qué es seguro
Auditoría de `scrapers_v2/`: todos los scrapers leen atributos (`src`, `data-src`, `srcset`, `data-srcset`, `data-zoom`) y texto; ninguno usa `naturalWidth`, `.complete`, screenshots ni `expect_response` sobre imágenes. Las rutas se registran en el `context`, así que cubren también las `detail_page`. Verificado con una prueba local: el `src` queda intacto, el `onerror` no se dispara y el JS corre normal.

## Operación
`run_scrapers.bat` hace checkout de `optimizado-v1`, pregunta el máximo de navegadores y exporta `BLOCK_RESOURCES=1`. Ver también [[SharedSeenUrls]] (Part3 de ChileSuplementos corre después de Part1+2, así que no hay deadlock con los slots).
