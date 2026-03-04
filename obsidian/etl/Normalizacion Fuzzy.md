# Normalización Fuzzy (Step 2)

**Archivo:** `shared/normalize_products.py` → función `normalize_names()`

**Objetivo:** Agrupar nombres de productos equivalentes scrapeados desde distintas tiendas bajo un único nombre canónico, reduciendo duplicados antes de insertar en la BD.

---

## Algoritmo

### Scorer dual

```python
# Scorer primario: token_set_ratio (captura reordenamientos, ej. "Whey Gold 5lb" vs "Gold Whey 5lb")
matches = process.extract(name, normalized_names,
    scorer=fuzz.token_set_ratio,
    score_cutoff=83)   # threshold = 83

# Scorer secundario (guard): ambos deben pasar
tsr = fuzz.token_sort_ratio(name, candidate)
if tsr < 65:
    continue  # falso positivo por subset puro ("Protein Bar" dentro de "Protein Bar Cereal")
```

**Por qué 83/65:** `token_set_ratio` es muy permisivo con subsets. El 83 atrapa la mayoría de variantes de nombre; el 65 de `token_sort_ratio` evita que un nombre corto matchee dentro de uno más largo.

---

## Filtros de Bloqueo (Hard Blockers)

Si cualquiera falla → el par NO se clusteriza.

### 1. `check_critical_mismatch()` — Keywords discriminantes
Si una keyword está en un nombre pero no en el otro, son productos distintos.

Ejemplos críticos: `vegan`, `isolate`, `hydro`, `zero`, `whey`, `iso`, `bar`, `women`, `keto`, `hardcore`, `nighttime`, formas de presentación (`polvo`, `caps`, `tabs`), sub-marcas.

### 2. `check_percentage_mismatch()` — Porcentajes
`70% Cacao` ≠ `79% Cacao`. Detecta tanto `70%` como `70 cacao` (sin símbolo).

### 3. `extract_sizes()` — Tamaños
Extrae **todos** los tamaños del texto: `500 g`, `60 caps`, `2 lbs`, etc.
Los tuples de tamaños deben ser **idénticos** para mergear.

Unidades soportadas: `lb`, `kg`, `g`, `oz`, `tabs`, `caps`, `serv`, `scoop`, `sachet`, `unid`, `mcg`, `mg`, `iu`, `ml`, `l`, `amp`, `billion`.

### 4. `detect_packaging()` — Tipo de empaque
`caja`, `display`, `bandeja`, `pack` → si difieren, no se clusteriza.

### 5. `extract_pack_quantity()` — Cantidad de pack
`5x`, `Pack 2`, `Pack de 2` → deben coincidir.

### 6. `extract_flavors()` — Sabores
Si uno tiene `chocolate` y el otro `vainilla` → son distintos.

### 7. Brand strictness — Marcas
Si ambos tienen marca válida (no `N/D`/`nan`), se requiere `fuzz.ratio(b1, b2) >= 85`. Esto evita mezclar `Muscletech` con `Muscle Tech Pro Series`.

---

## Resultado

```
N nombres únicos → M nombres canónicos  (M < N)
Cada nombre original queda mapeado a su representante canónico.
```

El CSV resultante agrega columna `normalized_name`, que Step 3 usa como `nombre_producto` en la BD.

---

## Umbrales actuales

| Parámetro | Valor | Motivo |
|-----------|-------|--------|
| `threshold` (token_set_ratio) | 83 | Balance entre recall y precisión |
| `threshold_sort` (token_sort_ratio) | 65 | Guard permisivo; los blockers semánticos hacen el trabajo pesado |
| Brand ratio | 85 | Permite variantes tipográficas de marca |

---

## Referencias

- [[etl/Step 3 - Insercion Bulk]]
- [[arquitectura/Flujo de Datos]]
