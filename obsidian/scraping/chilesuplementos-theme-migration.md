# ChileSuplementos — Migración de Tema (Porto → CS Theme)

**Fecha:** 2026-05-25

## Problema

ChileSuplementos rediseñó completamente su sitio, reemplazando el tema Porto por un tema personalizado con prefijo `cs-`. Todos los selectores CSS anteriores dejaron de existir, causando **0 productos capturados** en todas las categorías de Part1, Part2 y Part3.

Selectores rotos (Porto Theme):
- `.archive-products .porto-tb-item` — lista de productos
- `.post-title` — nombre del producto
- `.price ins .amount`, `.price .amount` — precio
- `.star-rating` — rating
- `pwb-brand-*` clases CSS en el `li` — marca

## Solución aplicada

### 1. Nuevos selectores (CS Theme)

```python
selectors = {
    'product_card':    'li.cs-product-card',
    'product_name':    '.cs-product-card__body h3',
    'price':           '.cs-price',
    'link':            '.cs-product-card__body h3 a',
    'rating':          '.cs-rating',
    'active_discount': '.onsale',
    'next_button':     'button.cs-loadmore-btn',
    'thumbnail':       '.cs-product-card__image img'
}
```

### 2. Extracción de rating (aria-label)

```python
aria = rating_elem.first.get_attribute("aria-label") or ""
m = re.search(r'[\d.]+', aria)
if m:
    rating = m.group(0)
```

### 3. Extracción de marca — Brand Filter URL

Las clases `pwb-brand-*` ya no aparecen en los `li` de producto. Sin embargo, el plugin WooCommerce Brands (`perfect-woocommerce-brands`) sigue activo y expone un filtro de marcas en el sidebar.

**Estrategia:** por cada subcategoría, leer el sidebar para obtener los slugs de marca, luego navegar a `{url}?tax_pwb-brand={slug}`. Todos los productos listados en esa página pertenecen definitivamente a esa marca.

Selector del filtro en el sidebar:
```
input[name="tax_pwb-brand[]"]   →   value = slug
parent element title             →   display name de la marca
```

Método inyectado en la clase:
```python
def _get_brand_map(self, page):
    brand_map = {}
    try:
        for cb in page.locator('input[name="tax_pwb-brand[]"]').all():
            slug = cb.get_attribute("value") or ""
            name = (cb.locator("..").get_attribute("title") or "").strip()
            if slug:
                brand_map[slug] = name or slug.replace("-", " ").title()
    except Exception:
        pass
    return brand_map
```

Fallback si la categoría no tiene filtro de marcas:
```python
if not brand_map:
    brand_map = {"__sin_filtro__": "N/D"}
```

### 4. Estructura del loop resultante

```python
brand_map = self._get_brand_map(page)
if not brand_map:
    brand_map = {"__sin_filtro__": "N/D"}

for _brand_slug, _brand_name in brand_map.items():
    if _brand_slug != "__sin_filtro__":
        page.goto(f"{url}?tax_pwb-brand={_brand_slug}", ...)

    last_product_count = 0
    no_change_counter = 0

    while True:
        # ... scroll + extracción normal ...
        # brand = _brand_name  (ya determinada por el filtro)
```

### 5. Descuento activo

```python
active_discount = "sale" in card_classes.split()
```

## Archivos afectados

- [[ChileSuplementosScraperPart1]] — `scrapers_v2/ChileSuplementosScraperPart1.py`
- [[ChileSuplementosScraperPart2]] — `scrapers_v2/ChileSuplementosScraperPart2.py`
- [[ChileSuplementosScraperPart3]] — `scrapers_v2/ChileSuplementosScraperPart3.py`

## Fix adicional — User-Agent 403 (BaseScraper)

El contexto de BaseScraper usaba Chrome 131 como user-agent, que ChileSuplementos (Cloudflare) bloquea con **403 Forbidden**. Los productos no cargaban aunque los selectores eran correctos.

Resultado de prueba por user-agent (mismo contexto, headless):
| User-Agent | Resultado |
|---|---|
| Default Playwright | ✅ OK |
| Chrome 120 Win | ❌ 403 |
| Chrome 122 Win | ❌ 403 |
| **Chrome 124 Win** | ✅ OK |
| Chrome 131 Win | ❌ 403 |
| Chrome 131 Mac | ❌ 403 |
| Firefox 128 Win | ✅ OK |
| Edge 131 Win | ❌ 403 |

Fix: cambiar el user-agent global en `BaseScraper.py` de Chrome 131 a **Chrome 124**. Todos los scrapers se benefician.

## Fix adicional — RunAll.py stuck (psycopg2)

Problema relacionado en la misma sesión: `RunAll.py` se quedaba colgado sin poder hacer Ctrl+C porque `psycopg2` (extensión C) ignora SIGINT. Fix: agregar `options="-c statement_timeout=8000"` a la conexión en `_get_historical_averages()`.

Ver [[BaseScraper]] para contexto de SharedSeenUrls entre las partes.
