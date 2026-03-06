# Bug: Farmacia Knopp — Precio 0 en productos lazy-loaded

**Fecha:** 2026-03-06
**Afectados:** `FarmaciaKnopScraperPart1.py`, `FarmaciaKnopScraperPart2.py`
**Severidad:** Alta — ~80% de productos de Vitaminas y Minerales quedaban con precio 0

## Síntoma

El reporte post-scrapeo mostraba muchos productos de Farmacia Knopp con `price = 0`, especialmente en categorías con más de ~8 productos (Vitaminas y Minerales tiene 39+).

## Causa Raíz

El sitio usa la plataforma **Bolder** (bootic), que implementa lazy rendering de precios via JS. El elemento `span.bootic-price` existe en el DOM desde el HTML inicial, pero su `innerText` queda **vacío** para los cards que están fuera del viewport al momento de carga. El JS del sitio solo rellena el texto cuando el card entra al viewport.

El scraper hacía scroll completo para cargar todos los cards, pero el JS de Bolder no populaba el `innerText` de todos los spans a tiempo antes de que el scraper llamara `inner_text()`.

### DOM real (inspeccionado con Playwright)

```html
<!-- Card visible: innerText presente -->
<span class="bootic-price with-price-comparison" data-initial-value="$12.190">$12.190</span>

<!-- Card lazy: innerText vacío pero data-initial-value siempre disponible -->
<span class="bootic-price with-price-comparison" data-initial-value="$12.990"></span>
```

**Diagnóstico en /types/multivitaminico (39 productos):**
- 8 con precio visible → `innerText` populado
- 31 con precio 0 → `innerText` vacío, pero `data-initial-value` correcto
- 29 con variantes (select/options)

## Fix Aplicado

Usar `get_attribute('data-initial-value')` con fallback a `inner_text()`:

```python
# ANTES
price_text = current_price_el.inner_text()

# DESPUÉS
price_text = current_price_el.get_attribute('data-initial-value') or current_price_el.inner_text()
```

El atributo `data-initial-value` siempre está en el HTML estático, independiente del estado de renderizado JS.

## Archivos Modificados

- `scrapers_v2/FarmaciaKnopScraperPart1.py` — línea ~179
- `scrapers_v2/FarmaciaKnopScraperPart2.py` — línea ~184

## Ver también

- [[bug-farmacia-knopp-nombres-vacios]]
- [[division-scrapers-3-partes]]
