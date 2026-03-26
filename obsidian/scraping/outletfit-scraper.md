# OutletFit Scraper

## Resumen

Scraper monolítico (sin runner/partes) para **outletfit.cl**, tienda chilena de suplementos deportivos basada en **Jumpseller**.

**Archivo:** `scrapers_v2/OutletFitScraper.py`
**Site name:** `OutletFit`
**Registrado en:** [[RunAll]] como `("OutletFit", "OutletFitScraper.py")`

## Estrategia de extracción

A diferencia de otros scrapers que extraen datos del DOM del listado, OutletFit usa un enfoque de **navegación a detalle + JSON-LD**:

1. **Listado:** Navega a cada URL de categoría y extrae los links de productos desde los `<article>` elements
2. **Detalle:** Visita cada producto individualmente y extrae datos desde el `<script type="application/ld+json">` embebido
3. **Marca:** Viene en el JSON-LD field `brand.name` (fuente: junto al SKU en la página de detalle)

### Por qué JSON-LD y no DOM del listado

- El listado **no tiene marca** visible por producto
- El JSON-LD de detalle tiene: nombre, marca, precio, imagen, SKU, descripción, rating, reviews
- Estructura consistente (Jumpseller genera JSON-LD estándar Schema.org)

### Nota sobre JSON-LD de Jumpseller

El JSON-LD viene como array anidado: `[[BreadcrumbList, Product]]`. Se usa búsqueda recursiva para encontrar el objeto `@type: "Product"`.

## Categorías mapeadas

| URL de la tienda | Categoría | Subcategoría determinista |
|---|---|---|
| `/catalogo/proteinas` | Proteinas | Proteína de Whey |
| `/catalogo/creatinas` | Creatinas | Creatina Monohidrato |
| `/catalogo/pre-entrenos` | Pre Entrenos | Otros Pre Entrenos |
| `/catalogo/vitaminas` | Vitaminas y Minerales | Multivitamínicos |
| `/catalogo/snack-proteico` | Snacks y Comida | Barritas Y Snacks Proteicas |
| `/aminoacidos/bcaa` | Aminoácidos | BCAAs |
| `/catalogo/ganadores-de-peso` | Ganadores de Peso | Ganadores de Peso |
| `/catalogo/pro-hormonales` | Pro-Hormonales | Pro-Hormonales |
| `/catalogo/bebida-energetica` | Bebidas Nutricionales | Bebidas Energéticas |
| `/catalogo/termogenicos` | Pérdida de Grasa | Termogénicos / Quemadores |

**Excluidas:** `Ropa y Accesorios`, `Packs Ofertas` (los packs se reclasifican automáticamente por [[CategoryClassifier]])

## Clasificación heurística

Se aplica `CategoryClassifier.classify()` post-extracción para refinar subcategorías. Ejemplo:
- "Proteína ISO-XP" en categoría Proteinas -> reclasificado a **Proteína Aislada** (no Whey)
- "Beta Alanina" en Pre Entrenos -> reclasificado a **Beta Alanina** (no Otros)
- Productos tipo pack -> reclasificados a **Packs/Packs**

## Volumen

~73 productos (test 2026-03-08). Tienda pequeña, sin paginación en categorías.

## Rendimiento

- Sin paginación (cada categoría cabe en una página)
- ~1 segundo de delay entre requests de detalle
- Tiempo total estimado: ~3-4 minutos (headless)

## Dependencias

- [[BaseScraper]] (Playwright, S3, BrandClassifier)
- [[CategoryClassifier]] (refinamiento de subcategorías)
- JSON-LD Schema.org (fuente de datos principal)
