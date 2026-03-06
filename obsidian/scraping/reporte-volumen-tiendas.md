# Reporte de Volumen por Tienda

**Fecha de análisis:** 2026-03-06
**Objetivo:** Identificar las tiendas con mayor carga de productos para priorizar optimizaciones de scraping.
**Fuente de datos:** `historia_precios` + `producto_tienda` + `tiendas` (BD local `suplementos`)

---

## Tabla 1 — Productos activos en catálogo (snapshot actual)

| # | Tienda | Activos | Inactivos | Total |
|---|--------|--------:|----------:|------:|
| 1 | SportNutriShop | 399 | 102 | 501 |
| 2 | AllNutrition | 417 | 0 | 417 |
| 3 | ChileSuplementos | 346 | 33 | 379 |
| 4 | OneNutrition | 335 | 6 | 341 |
| 5 | BYON | 270 | 1 | 271 |
| 6 | Suples.cl | 173 | 88 | 261 |
| 7 | Farmacia Knopp | 176 | 4 | 180 |
| 8 | SupleTech | 162 | 16 | 178 |
| 9 | SuplementosBullChile | 140 | 7 | 147 |
| 10 | SupleStore | 116 | 7 | 123 |
| 11 | Cruz Verde | 109 | 0 | 109 |
| 12 | MuscleFactory | 107 | 3 | 110 |
| 13 | KoteSport | 94 | 0 | 94 |
| 14 | Wild Foods | 92 | 0 | 92 |
| 15 | Winkler Nutrition | 80 | 0 | 80 |
| 16 | FitMarketChile | 72 | 6 | 78 |
| 17 | Strongest | 57 | 13 | 70 |
| 18 | SuplementosMayoristas | 57 | 2 | 59 |
| 19 | Dr Simi | 54 | 16 | 70 |
| 20 | Decathlon | 19 | 6 | 25 |

**Total activos en catálogo: ~3.275 productos**

---

## Tabla 2 — Promedio de productos scrapeados por dia (ultimos 14 dias)

| Rank | Tienda | Promedio/dia | Maximo | Minimo | % del total |
|------|--------|------------:|-------:|-------:|------------:|
| 1 | SportNutriShop | 405 | 437 | 376 | 14,5% |
| 2 | AllNutrition | 354 | 417 | 2 | 12,7% |
| 3 | ChileSuplementos | 347 | 354 | 341 | 12,5% |
| 4 | BYON | 270 | 270 | 270 | 9,7% |
| 5 | Suples.cl | 197 | 256 | 178 | 7,1% |
| 6 | OneNutrition | 167 | 335 | 59 | 6,0% |
| 7 | SupleTech | 164 | 167 | 162 | 5,9% |
| 8 | SuplementosBullChile | 145 | 147 | 140 | 5,2% |
| 9 | Farmacia Knopp | 122 | 175 | 56 | 4,4% |
| 10 | SupleStore | 116 | 119 | 114 | 4,2% |
| 11 | Cruz Verde | 108 | 109 | 106 | 3,9% |
| 12 | MuscleFactory | 105 | 107 | 102 | 3,8% |
| 13 | Wild Foods | 92 | 92 | 92 | 3,3% |
| 14 | KoteSport | 86 | 94 | 16 | 3,1% |
| 15 | FitMarketChile | 75 | 78 | 73 | 2,7% |
| 16 | Winkler Nutrition | 73 | 80 | 13 | 2,6% |
| 17 | Strongest | 60 | 66 | 53 | 2,2% |
| 18 | SuplementosMayoristas | 57 | 58 | 56 | 2,0% |
| 19 | Dr Simi | 47 | 53 | 45 | 1,7% |
| 20 | Decathlon | 19 | 19 | 19 | 0,7% |

**Total promedio diario estimado: ~2.789 productos/dia**

---

## Top 5 — Las tiendas mas pesadas (~57% del volumen total)

```
SportNutriShop   ████████████████████  405/dia  (14,5%)
AllNutrition     █████████████████▌    354/dia  (12,7%)
ChileSuplementos █████████████████     347/dia  (12,5%)
BYON             █████████████         270/dia   (9,7%)
Suples.cl        █████████▌            197/dia   (7,1%)
─────────────────────────────────────────────────────────
Top 5 acumulado: 1.573/dia                       (56,4%)
```

---

## Observaciones importantes

### Scraper de ChileSuplementos ya esta dividido
- `ChileSuplementosScraperPart1.py` + `ChileSuplementosScraperPart2.py` + `ChileSuplementosScraperRunner.py`
- Unico scraper con arquitectura multi-parte. Buen modelo para replicar en los demas pesados.

### AllNutrition — variabilidad anomala
- Minimo de **2 productos** el 2026-02-26 (posible fallo silencioso o timeout).
- Promedio muy estable ~415 en condiciones normales.
- El **2026-03-05 no aparece en `historia_precios`** — no fue scrapeado ese dia.

### OneNutrition — alta variabilidad
- Rango entre 59 y 335 en 14 dias.
- Probablemente hay paginacion dinamica o bloqueos intermitentes.

### BYON — perfectamente estable
- Exactamente 270 productos todos los dias. Sin ninguna variacion.
- Scraper robusto o catalogo estatico.

---

## Recomendaciones para optimizar el pipeline (~25 min locales)

### Prioridad ALTA — paralizar scrapers pesados
El Top 5 es el **57% del tiempo total** y deberia correr en paralelo o dividido:

1. **SportNutriShop** (405/dia) — candidato #1 para dividir en Part1/Part2 como ChileSuplementos
2. **AllNutrition** (354/dia) — investigar causa de fallo 2026-03-05 antes de tocar
3. **ChileSuplementos** (347/dia) — ya dividida, revisar si vale la pena un Part3
4. **BYON** (270/dia) — estable, medir tiempo de ejecucion real para decidir si necesita split

### Prioridad MEDIA — monitoreo de inestables
- **OneNutrition**: variabilidad 59–335 sugiere problema intermitente de scraping
- **Farmacia Knopp**: variabilidad 56–175 similar
- **Suples.cl**: algunos dias arroja 256 vs promedio 197 (paginacion inconsistente?)

### Metrica clave antes de refactorizar
Medir tiempo real de ejecucion de cada scraper con `time python scrapers_v2/X.py` y cruzar con este reporte. Un scraper de 400 productos rapido puede ser mejor que uno de 100 con muchos reintentos.

---

## Archivos de scraper por tienda

| Tienda | Scraper |
|--------|---------|
| SportNutriShop | `SportNutriShopScraper.py` |
| AllNutrition | `AllNutritionScraper.py` |
| ChileSuplementos | `ChileSuplementosScraperRunner.py` (Part1 + Part2) |
| BYON | `BYONScraper.py` |
| Suples.cl | `SuplesScraper.py` |
| OneNutrition | `OneNutritionScraper.py` |
| SupleTech | `SupleTechScraper.py` |
| SuplementosBullChile | `SuplementosBullChileScraper.py` |
| Farmacia Knopp | `FarmaciaKnopScraper.py` |
| SupleStore | `SupleStoreScraper.py` |
| Cruz Verde | `CruzVerdeScraper.py` |
| MuscleFactory | `MuscleFactoryScraper.py` |
| KoteSport | `KoteSportScraper.py` |
| Wild Foods | `WildFoodsScraper.py` |
| Winkler Nutrition | `WinklerNutritionScraper.py` |
| FitMarketChile | `FitMarketChileScraper.py` |
| Strongest | `StrongestScraper.py` |
| SuplementosMayoristas | `SuplementosMayoristasScraper.py` |
| Dr Simi | `DrSimiScraper.py` |
| Decathlon | `DecathlonScraper.py` |

---

## Links

- [[SharedSeenUrls]] — mecanismo de deduplicacion de URLs entre procesos paralelos
