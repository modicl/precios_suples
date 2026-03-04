# Bug: Productos con Brand "N/D" corrompen datos

**Fecha:** Marzo 2026
**Estado:** Resuelto
**Commit:** `43c1002 update : proteccion contra productos "N/D" que corrompian datos`

---

## Problema

Algunos scrapers retornaban productos con `brand == "N/D"` (sin marca identificable). Estos productos entraban al pipeline de normalización y podían causar:

1. **Falsos clusters en Step 2:** productos sin marca con nombres similares se agrupaban incorrectamente porque el filtro de brand del fuzzy matcher solo bloquea pares cuando *ambos* tienen marca válida.
2. **Inserción de datos basura en la BD:** productos sin marca real ocupaban espacio y diluían la calidad del catálogo.
3. **Brand inheritance fallida:** un producto `N/D` heredaba la marca de cualquier gemelo de nombre similar en la BD, potencialmente asignando marcas incorrectas.

---

## Solución

Filtro en el scraper (o en Step 1 de limpieza): productos con `brand == "N/D"` se descartan **antes** de entrar al pipeline.

```python
# Aplicado en la etapa de limpieza del CSV
df = df[df['brand'] != 'N/D']
```

**Decisión de diseño:** el filtro opera en el CSV de entrada, no en la BD. Es más barato descartar antes de normalizar que limpiar después de insertar.

---

## Casos especiales que quedan permitidos

La brand inheritance en [[etl/Step 3 - Insercion Bulk]] aún maneja productos que entran con `N/D` y tienen un gemelo real en la BD → heredan la marca correcta. El filtro "N/D" solo aplica cuando no hay posibilidad de inheritance.

---

## Referencias

- [[etl/Normalizacion Fuzzy]]
- [[etl/Step 3 - Insercion Bulk]]
- [[scraping/BaseScraper]]
