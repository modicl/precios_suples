# LLM de los steps 5 y 6: migración de OpenAI a Gemini

Los pasos de IA del pipeline (ver [[Flujo de Datos]]) dejaron de usar GPT-4o-mini y ahora usan **Google Gemini** a través de un cliente compartido.

## Qué cambió
- Nuevo `shared/llm_client.py` con `GeminiClient.generate(system, user, max_tokens, temperature, json_output)`, sobre el SDK `google-genai` (reemplaza `openai` en `requirements.txt`).
- `step5_generate_descriptions.py` (descripciones, ver [[Schema de Base de Datos]] columna `descripcion_llm`) y `step6_tag_keywords.py` (tags por categoría) llaman a ese cliente. Los prompts, reintentos con backoff y la escritura en BD no se tocaron.
- Step 6 pide `response_mime_type="application/json"` para que el array de keywords venga siempre válido; el filtro contra el diccionario sigue igual.

## Configuración (.env)
- `GOOGLE_API_KEY` (también `GEMINI_API_KEY`): clave `AQ...` o `AIza...`.
- `GEMINI_MODEL`: opcional, por defecto `gemini-3.5-flash-lite`.
- Validar sin tocar la BD: `python shared/llm_client.py --probar`.

## Decisiones
- **Límites de tokens holgados** (1024 en descripciones, 512 en tags, antes 200 y 80): los modelos Gemini 3 "piensan" antes de responder y esos tokens cuentan contra el máximo; con límites chicos la respuesta podía salir vacía. La longitud real la sigue controlando el prompt (máx. 60 palabras). Una respuesta vacía lanza error y entra al reintento.
- **Claves `AQ.`**: formato nuevo de AI Studio. Hay reportes de `401 ACCESS_TOKEN_TYPE_UNSUPPORTED` con algunas cuentas; Google indica que es soportado y que el error suele deberse a clave truncada, proyecto sin la API habilitada o un modelo antiguo. Por eso existe `--probar`.
- **429**: el plan gratuito tiene límites por minuto; si aparecen errores, bajar `--concurrencia`.

## Logs (`logs/llm/`)
Cada ejecución crea `logs/llm/<step>_<timestamp>.log` (`step5_descripciones_…`, `step6_tags_…`, `probar_…`; la carpeta está en `.gitignore`). Lo escribe `GeminiClient` en cada llamada, así que cubre ambos steps:
- `OK` por llamada: id y nombre del producto, intento, latencia, tokens (entrada/salida/pensando), `finish_reason` y la respuesta.
- `ERROR` (excepción de la API, ej. 429 o 403), `VACIA` y `INCOMPLETA` (cortada por `MAX_TOKENS`), seguidos de `REINTENTO n/3` y, si se agotan, `FALLO`.
- Step 6 además: `TAGS` asignados y `TAGS_DESCARTADOS` (keywords que el LLM inventó fuera del diccionario).
- `RESUMEN` final: llamadas, ok/errores, latencia media, tokens totales, ids fallidos.
- La API key nunca se escribe.

Cómo revisarlo rápido: `grep -c " OK " <log>`, `grep -E "FALLO|INCOMPLETA|VACIA" <log>` y mirar la línea `RESUMEN`.

Una respuesta con `finish_reason` distinto de `STOP` (ej. `MAX_TOKENS`) se trata como error y se reintenta: antes habría guardado descripciones cortadas en la BD.

Relacionado: [[optimizacion-cpu-scraping-v1]] (misma rama `optimizado-v1`).
