# Trigger de Notificaciones de Precio post-Pipeline

## Qué hace

Al final del Cloud Run Job (en `main.py`), después de que el pipeline completa step3 (`step3_db_insertion`) con éxito, se dispara automáticamente una llamada HTTP POST al backend para procesar las alertas de precio del día.

## Dónde vive el código

- **Función:** `trigger_notificaciones()` en `main.py`
- **Invocación:** dentro de `main()`, justo después de `run_pipeline()` retorna, antes del resumen final

## Condición de disparo

```python
step3_ok = any(r["name"] == "step3_db_insertion" and r["ok"] for r in pipeline_results)
```

Solo se llama si `step3_db_insertion` completó exitosamente (i.e., los inserts a `historia_precios` fueron confirmados). Si step3 falló o fue abortado por un step anterior, no se dispara.

## Endpoint

```
POST {BACKEND_URL}/api/v1/notificaciones/process
Header: x-comparafit-signature: {INTERNAL_API_SECRET}
```

Respuesta esperada: `{ "procesados": N, "notificaciones": M }`

## Variables de entorno requeridas

| Variable | Descripción |
|----------|-------------|
| `BACKEND_URL` | URL base del backend en Cloud Run |
| `INTERNAL_API_SECRET` | Secret compartido entre scraper y backend |

Configuradas en `.env` local y deben estar en los secrets de Cloud Run.

## Comportamiento ante fallo

La función captura **toda** excepción y solo loguea el error. Nunca lanza excepción hacia el job padre, de modo que un fallo en las notificaciones no marca el Cloud Run Job como fallido ni interrumpe el flujo.

## Relaciones

- [[step3_db_insertion]] — confirma los inserts a `historia_precios` que gatillan las alertas
- [[main-cloud-run-entrypoint]] — entrypoint donde vive esta lógica
