# Bug: Farmacia Knopp — Nombres de Producto Vacíos (81.9%)

## Síntoma
En el run del 2026-03-06, `raw_data/productos_farmacia_knopp_*.csv` tenía 145/177 filas con `product_name = NaN`, `brand = N/D`, `price = 0`, pero con `link` válido.

## Causa Raíz
**Problema de timing en Playwright headless.** El selector `div.product-name a.product-title-link` encontraba el elemento (`count() > 0`), pero `inner_text()` devolvía `""` porque el texto no estaba renderizado aún en headless al momento de la consulta.

El atributo HTML `title` en ese mismo elemento SÍ contiene el nombre correctamente (es HTML estático, no depende del render JS).

## Por qué el link sí se extraía
El selector `a.product-link` apuntaba al wrapper de imagen, cuyo atributo `href` es HTML estático → siempre se extraía correctamente. El nombre en cambio depende del contenido textual del nodo, que es dinámico.

## Fix aplicado (`FarmaciaKnopScraper.py`)
1. Usar `get_attribute("title")` como fuente primaria del nombre (en lugar de `inner_text()`)
2. Fallback: `aria-label` del link de imagen (`"Ver detalles del producto {nombre}"`)
3. Guard del skip cambiado de `name == "N/D"` a `not name or name == "N/D"` para cubrir strings vacíos

## Notas adicionales
- Los selectores CSS no cambiaron (el sitio migró de Bootic a Bolder pero mantuvo las mismas clases: `product-item`, `product-name`, `product-title-link`, `product-brand`, `bootic-price`)
- El botón de sin-stock cambió texto de "Notificarme" a "Avisar disponibilidad", pero mantiene la clase `notify-in-stock` → el selector `button.notify-in-stock` sigue funcionando

## Relacionado
- [[check_data_quality]] — el QA report fue quien detectó el problema masivo
