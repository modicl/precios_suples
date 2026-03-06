# Division de Scrapers en 3 Partes

**Fecha:** 2026-03-06
**Contexto:** [[reporte-volumen-tiendas]] identifico que ChileSuplementos (347/dia) y SportNutriShop (405/dia) representan el 27% del volumen total del pipeline. Dividirlos en 3 partes paralelas reduce el tiempo de scraping de estos dos sitios en aprox. 2/3.

---

## ChileSuplementos

### Distribucion de categorias

| Parte | Categorias | URLs | Estrategia |
|-------|-----------|------|------------|
| Part1 | Proteinas (todas las subcategorias) | 8 | Paralela con Part2 |
| Part2 | Creatinas + Vitaminas + Pre Entrenos + Ganadores | 21 | Paralela con Part1 |
| Part3 | Aminoacidos + Perdida de Grasa + Snacks + **Ofertas** + Packs | 7 | **Secuencial despues de Part1+Part2** |

### Por que Part3 es secuencial

La categoria **Ofertas** agrega todos los productos del sitio sin importar categoria. Sin deduplicacion, los mismos productos aparecerian en el CSV 2 veces (una en su categoria real y otra en Ofertas).

`SharedSeenUrls("chilesuplementos_ofertas")` es el mecanismo de deduplicacion cross-proceso. Funciona con un archivo JSON + `.lock` en `raw_data/`. Para que funcione correctamente, Part3 DEBE esperar a que Part1 y Part2 hayan registrado todas sus URLs.

**Flujo del Runner:**
```
Part1 ─┐
       ├─ (paralelo) ─→ wait() ─→ Part3 (Ofertas dedup completo) ─→ cleanup
Part2 ─┘
```

### Archivos
- `ChileSuplementosScraperPart1.py` — clase `ChileSuplementosScraperPart1`, output_suffix=`_part1`
- `ChileSuplementosScraperPart2.py` — clase `ChileSuplementosScraperPart2`, output_suffix=`_part2`
- `ChileSuplementosScraperPart3.py` — clase `ChileSuplementosScraperPart3`, output_suffix=`_part3`
- `ChileSuplementosScraperRunner.py` — orquestador con ejecucion en 2 fases

---

## SportNutriShop

### Distribucion de categorias

| Parte | Categorias | URLs | Estrategia |
|-------|-----------|------|------------|
| Part1 | Proteinas + Ganadores de Peso | 2 | Paralela (las mas densas con paginacion) |
| Part2 | Pre Entrenos + Creatinas + Aminoacidos + Perdida de Grasa | 7 | Paralela |
| Part3 | Bebidas + Snacks + Vitaminas + Packs | 6 | Paralela |

### Sin estado compartido

SportNutriShop no tiene categoria de "Ofertas" ni ninguna categoria que agregue productos de otras. Las 3 partes pueden correr en paralelo completo sin coordinacion.

**Flujo del Runner:**
```
Part1 ─┐
Part2 ─┼─ (paralelo total) ─→ wait() ─→ fin
Part3 ─┘
```

### Archivos
- `SportNutriShopScraperPart1.py` — output_suffix=`_part1`
- `SportNutriShopScraperPart2.py` — output_suffix=`_part2`
- `SportNutriShopScraperPart3.py` — output_suffix=`_part3`
- `SportNutriShopScraperRunner.py` — orquestador paralelo puro
- `SportNutriShopScraper.py` — scraper monolitico original (mantenido como fallback)

---

## Cambios en RunAll.py

- `"SportNutriShop"` ahora apunta a `SportNutriShopScraperRunner.py` en vez del scraper monolitico
- `RUNNER_SCRIPTS` set ampliado para propagar `--headless` a ambos runners

---

## Impacto esperado

Antes de la division, el tiempo del pipeline estaba dominado por los scrapers mas grandes corriendo secuencialmente dentro de cada proceso. Con 3 partes paralelas:

- **ChileSuplementos**: tiempo teorico dividido en ~3x (Part1+Part2 en paralelo son el cuello de botella)
- **SportNutriShop**: tiempo teorico dividido en ~3x (paralelo total)

El tiempo real dependera del profiling. Ver [[reporte-volumen-tiendas]] para datos base.

---

## Links

- [[reporte-volumen-tiendas]] — datos que justificaron esta decision
- [[SharedSeenUrls]] — mecanismo de deduplicacion cross-proceso usado por ChileSuplementos
