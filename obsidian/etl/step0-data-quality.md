# step0 — Validación de Calidad de Datos (Pre-carga)

## Propósito
Script de QA ejecutado **antes** del pipeline ETL (steps 1–7). No modifica ni detiene el flujo; solo analiza y reporta.

Archivo: `check_data_quality.py` (raíz del proyecto, NO dentro de `local_processing_testing/`)

## Checks implementados

| Check | Severidad | Descripción |
|-------|-----------|-------------|
| Schema | Crítico | Columnas requeridas presentes; `price` es numérico |
| Nulos | Crítico | `product_name`, `price`, `url_link` no vacíos |
| Precio inválido | Crítico | Precio ≤ 0 |
| Precio outlier | Warning | Desviación >50% de la mediana de la subcategoría |
| Basura HTML | Warning | Tags HTML / secuencias escape en campos de texto |
| URLs duplicadas | Warning | `url_link` repetida dentro del mismo CSV |

## Salidas
- **Consola**: resumen por tienda con conteo de críticos/warnings
- **HTML**: `logs/reporte_calidad/reporte_calidad_YYYY-MM-DD_HH-MM-SS.html`
  - Dashboard dark-mode con 5 cards de resumen
  - Tabla filtrable por severidad (JS vanilla)
  - Sección por tienda con badge OK/Warning/Crítico

## Posicion en el flujo
```
1. run_scrapers.bat              <- genera raw_data/*.csv
2. python check_data_quality.py  <- ESTE SCRIPT (revisar antes de subir a prod)
3. run_pipeline.bat              <- inserta a BD produccion
```

## Uso
```bash
python check_data_quality.py                        # analiza raw_data/
python check_data_quality.py --dir raw_data
python check_data_quality.py --file raw_data/mi_tienda.csv
```

## Relaciones
- [[BaseScraper]] — genera los CSVs que este script valida
- [[step3-db-insertion]] — carga a BD; este paso detecta problemas antes de llegar ahí
- [[RunAll]] — el reporte post-scrapeo de volumen es complementario a este QA

## Decisiones de diseño
- No levanta excepciones ni modifica DataFrames; solo observa.
- El outlier de precio usa la mediana de `subcategory` (fallback a `category`) para evitar falsos positivos entre productos de distinto tipo.
- HTML completamente estático (CSS + JS incrustado) — no requiere servidor.
