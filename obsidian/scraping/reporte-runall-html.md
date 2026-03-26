# Reporte RunAll — Dashboard HTML Interactivo

## Resumen

`scrapers_v2/RunAll.py` genera al finalizar un **reporte HTML interactivo** en `logs/reporte_total/reporte_YYYY-MM-DD_HH-MM-SS.html`.

Ver también: [[RunAll]]

## Funcionalidades

### Filtros
- **Por tipo de problema:** Todos / Sin productos (0) / Anomalía baja (<70%) / Anomalía alta (>150%) / Fallo de scraper / Sin histórico / OK
- **Por tienda:** Dropdown con todas las tiendas scrapeadas
- Ambos filtros funcionan en conjunto (AND)
- Botón **"Limpiar filtros"** para resetear
- Contador `Mostrando N de M tiendas` se actualiza dinámicamente

### KPI Cards
7 tarjetas de resumen en la parte superior:
- Productos totales
- Tiendas OK
- Anomalías bajas
- Sin productos
- Fallos scraper
- Scrapers OK / total
- Duración total

Las tarjetas cambian de color según el estado (rojo si hay errores, verde/amarillo si OK).

### Tabla de tiendas
Columnas: Tienda | Productos | Hist. 14d | vs. Hist. | Estado | Scraper | Duración

Cada fila es **colapsable**: click en la fila expande un panel de detalle con:
- Exit code, duración, path del log del scraper
- Tabla de archivos CSV individuales con su conteo (útil para runners con múltiples partes)

### Botones de colapso masivo
- **"Abrir todos"**: expande todas las filas actualmente visibles
- **"Cerrar todos"**: colapsa todas las filas

## Lógica de estados

| Estado | Condición |
|--------|-----------|
| `zero` | count == 0 |
| `anomaly_low` | count < 70% del promedio histórico 14d |
| `anomaly_high` | count > 150% del promedio histórico 14d |
| `fail` | returncode != 0 del proceso scraper |
| `no_hist` | Sin registro en `historia_precios` últimos 14d |
| `ok` | Ninguna de las anteriores |

Una tienda puede tener múltiples estados simultáneos (ej. `ok fail` si el scraper falló pero los datos se guardaron).

## Funciones en RunAll.py

| Función | Descripción |
|---------|-------------|
| `_count_products_from_csvs(start_time)` | Dict `{site_name: count}` desde CSVs modificados |
| `_count_products_from_csvs_detailed(start_time)` | Dict `{site_name: {filename: count}}` para detalle por CSV |
| `_get_historical_averages()` | Dict `{nombre_tienda: avg_daily}` desde BD local (14d) |
| `_generate_report(results, start_time, elapsed)` | Genera el HTML completo |

## Implementación técnica

- HTML self-contained (sin dependencias externas, sin CDN)
- CSS definido como string Python (sin f-string, sin escape de llaves)
- JS definido como string Python con placeholder `TOTAL_STORES_PH` reemplazado en runtime
- Compatibilidad: todos los browsers modernos (usa `var` en lugar de `const`/`let` para max compat)
- Tema oscuro

## Cambios (Mar 2026)

- Reemplazó el reporte `.txt` plano por dashboard HTML interactivo
- Añadió filtros por tipo de problema y por tienda
- Añadió filas colapsables con detalle de CSVs y log paths
- Añadió KPI cards con colores condicionales
- Añadió detección de anomalía alta (>150%) además de la baja (<70%)
