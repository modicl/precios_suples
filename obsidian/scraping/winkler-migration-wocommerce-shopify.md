# Winkler Nutrition: Migración WooCommerce → Shopify

**Fecha:** 2026-03-24
**Síntoma:** Scraper traía 0 productos en todos los scrapeos recientes.

## Causa Raíz

La tienda migró su plataforma de **WooCommerce** a **Shopify**. Todas las URLs de categoría del tipo `/categoria-producto/...` retornan 404.

## Cambios en la Arquitectura del Sitio

| Aspecto | WooCommerce (antes) | Shopify (ahora) |
|---------|---------------------|-----------------|
| URLs de categorías | `/categoria-producto/proteinas-wk/` | N/A — una sola colección |
| Colección principal | N/A | `/collections/nuestros-productos` |
| Paginación | `a.next.page-numbers` → URL nueva | `?page=N` en query param |
| Card de producto | `article.product` | `.card-product` (div) |
| Nombre/Link | `a.entry-link-mask` + `.woocommerce-loop-product__title` | `a.x-card-title` (contiene ambos) |
| Precio oferta | `ins .woocommerce-Price-amount bdi` | `span.price-sale` |
| Precio normal | `.price .woocommerce-Price-amount bdi` | `p.price` |
| Precio limpio (entero) | N/A | `div.target-price` (centavos, ej: `8499000` = $84.990) |
| Thumbnail | `img.attachment-woocommerce_thumbnail` | `img` dentro de `.card-product` (CDN Shopify, param `?width=N`) |
| URLs de producto | `/product/nombre-producto/` | `/collections/nuestros-productos/products/nombre-producto` |

## Cambios Implementados en el Scraper

### `WinklerNutritionScraper.py`

1. **`category_urls`**: Reemplazado por una única entrada apuntando a `/collections/nuestros-productos`. La clasificación por categoría/subcategoría queda 100% a cargo de `CategoryClassifier`.

2. **`_extract_price`**: Reescrito para Shopify:
   - Detecta descuento con `span.price-sale`
   - Usa `div.target-price` como fuente primaria (entero en centavos, sin formato → más robusto)
   - Fallback a texto visible si `target-price` no está disponible

3. **`extract_process`**: Reescrito para scraping de colección única:
   - Navega `?page=N` de forma iterativa
   - Para paginación: verifica existencia de `a[href*="?page=N+1"]`
   - Limpia URLs de imágenes Shopify CDN (remueve `&width=N`)
   - Imagen full-size sigue viniendo del OG tag en página de detalle

4. **Thumbnail URL**: Shopify CDN agrega `?v=timestamp&width=150` → se remueve `&width=N` para obtener imagen a resolución completa sin redimensionar.

## Notas de la Estructura Shopify

- 6 páginas de paginación, ~20 productos por página, ~120 productos totales
- Productos con variantes muestran "Desde $X" en precio — `target-price` da el precio mínimo
- OG image sigue funcionando para la imagen de detalle
- La tienda solo vende su propia marca (Winkler Nutrition), sin cambios ahí

## Relacionado

- [[reporte-runall-html]]
