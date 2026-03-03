# Reporte de Tiendas por Productos Unicos

**Fecha:** 03/03/2026
**Base de datos:** suplementos (localhost:5432)

---

## Ranking por Productos Unicos (activos)

| #   | Tienda                | Activos | Inactivos | Total |
| --- | --------------------- | ------: | --------: | ----: |
| 1   | AllNutrition          |     417 |         0 |   417 |
| 2   | SportNutriShop        |     399 |        99 |   498 |
| 3   | ChileSuplementos      |     341 |        32 |   373 |
| 4   | BYON                  |     270 |         0 |   270 |
| 5   | Suples.cl             |     172 |        87 |   259 |
| 6   | SupleTech             |     161 |        16 |   177 |
| 7   | SuplementosBullChile  |     140 |         7 |   147 |
| 8   | Farmacia Knopp        |     123 |        10 |   133 |
| 9   | SupleStore            |     116 |         6 |   122 |
| 10  | Cruz Verde            |     109 |         0 |   109 |
| 11  | MuscleFactory         |     107 |         3 |   110 |
| 12  | KoteSport             |      94 |         0 |    94 |
| 13  | Wild Foods            |      92 |         0 |    92 |
| 14  | Winkler Nutrition     |      80 |         0 |    80 |
| 15  | FitMarketChile        |      72 |         6 |    78 |
| 16  | Strongest             |      60 |         9 |    69 |
| 17  | OneNutrition          |      59 |       202 |   261 |
| 18  | SuplementosMayoristas |      57 |         2 |    59 |
| 19  | Dr Simi               |      49 |        18 |    67 |
| 20  | Decathlon             |      19 |         6 |    25 |

---

## Resumen

| Metrica                          |              Valor |
| -------------------------------- | -----------------: |
| Total de tiendas                 |                 21 |
| Total productos activos (suma)   |              2,946 |
| Total productos inactivos (suma) |                503 |
| Tienda con mas activos           | AllNutrition (417) |
| Tienda con mas inactivos         | OneNutrition (202) |
| Tiendas sin productos activos    |     1 (KOTE SPORT) |

---

## Notas

- **Activos**: productos con `is_active = true` en `producto_tienda`
- **Inactivos**: productos con `is_active = false` en `producto_tienda`
- **OneNutrition** tiene 202 productos inactivos vs 59 activos — posible scraper caido o tienda con rotacion alta de catalogo
