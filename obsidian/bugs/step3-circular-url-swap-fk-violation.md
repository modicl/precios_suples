# Bug: FK Violation en step3 por Swap Circular de URLs

**Fecha descubierta:** 2026-04-13  
**Afectó:** Local + Production  
**Impacto:** Step3 fallaba silenciosamente desde el 1 de abril — no se insertaban precios en `historia_precios`

## Síntoma

Las vistas materializadas no mostraban datos nuevos. `historia_precios` estaba congelada en el 29 de marzo a pesar de que el pipeline (`run_pipeline.bat`) corría diariamente y terminaba sin error visible en el `.bat`.

El error real estaba en el log del pipeline:

```
[ERROR FATAL] Local: update or delete on table "producto_tienda" violates foreign key constraint 
"fk_producto_tienda" on table "historia_precios"
DETAIL:  Key (id_producto_tienda)=(56712) is still referenced from table "historia_precios".
```

El `[ERROR FATAL]` no aborta el `.bat` (el exit code no se propagaba correctamente), por lo que el pipeline continuaba a step4-step8 pero sin datos nuevos.

## Causa Raíz

### Trigger del bug
El scraper de Farmacia Knopp empezó a retornar `brand=FDC` (en vez de `N/D`) para el producto "Calcio 600mg + Vitamina D X 60 Comprimidos FDC" el 1 de abril.

### La normalización crea el ciclo
Step2 (`token_set_ratio ≥ 83`) clusteó bajo el mismo `normalized_name = "Calcio 600mg + Vitamina D X 60 Comprimidos Fdc"` dos productos distintos:
- "Trical-D Calcio 600mg + Vitamina D..." (brand=Knop, url=`trical-d-...`)
- "Calcio 600mg + Vitamina D..." (brand=FDC, url=`calcio-600-...`)

### El swap circular en `_batch_links`
Esto generaba DOS entradas en `_batch_links` para la misma tienda con URLs distintas:
- `(pid=1019/FDC, tid=16, url=calcio-600)`
- `(pid=61864/Knop, tid=16, url=trical-d)`

Y en la BD había:
- Row 56712: `(id_produto=61864/Knop, url=calcio-600)` — 220 historia_precios
- Row 1019: `(id_produto=1019/FDC, url=trical-d)` — 53 historia_precios

### El mecanismo del fallo
El bloque de URL remap en step3 identifica:
- Row 56712 como **orphan** para batch entry FDC (winner=row 1019)
- Row 1019 como **orphan** para batch entry Knop (winner=row 56712)

**Step 2 (migración):** Intercambia los historia_precios de ambos rows simultáneamente:
- 220 registros de 56712 → 1019
- 53 registros de 1019 → 56712

**Step 3 (DELETE):** Intenta borrar AMBOS rows, pero ahora ambos tienen historia_precios del otro → **FK violation**.

La excepción provoca rollback del remap y se propaga como `[ERROR FATAL]` — sin insertar precios.

## Fix Aplicado

**Archivo:** `local_processing_testing/step3_db_insertion.py`

Se dividió el paso A2 (DELETE de huérfanos) en dos sub-pasos:

1. **3a: Si el huérfano tiene historia_precios** → `UPDATE url_link = NULL, is_active = false`  
   Libera el slot de `uq_tienda_url` sin violar la FK. El paso B luego asigna la URL correcta vía `ON CONFLICT DO UPDATE`.

2. **3b: Si el huérfano no tiene historia_precios** → `DELETE` (comportamiento original, sin riesgo de FK).

**Log al correr con el fix:**
```
[Local] [URL remap] 2 enlace(s) huérfanos con historial: URL nullificada (swap circular detectado).
```

## Resultado del Fix

- `historia_precios` pasó de 435,075 registros (hasta 29 Mar) a **493,085 registros** (hasta 11 Abr)
- Todas las vistas materializadas refrescadas correctamente
- Los datos de abril (1, 2, 4, 7, 9, 11 de abril) se insertaron de una sola vez

## Relaciones

- [[step3_db_insertion]] — script afectado
- [[reporte-imagenes-faltantes]] — otro bug en step3 corregido en Mar 2026
