# SharedSeenUrls

**Archivo:** `scrapers_v2/BaseScraper.py` — clase `SharedSeenUrls`

---

## Problema que resuelve

Algunas tiendas tienen la misma categoría scrapeada por dos scripts en paralelo (ej. `ChileSuplementos Part1` y `Part2` comparten la categoría "Ofertas"). Sin coordinación, ambos procesos insertarían los mismos productos duplicados en el CSV.

Un `set()` en memoria no funciona porque cada proceso tiene su propio espacio de memoria.

---

## Solución: File-based lock + JSON

```
raw_data/{name}.json   ← almacén de URLs vistas (fecha + lista)
raw_data/{name}.lock   ← mutex entre procesos (O_CREAT|O_EXCL atómico)
```

### Operación atómica `register(url) → bool`

```python
shared = SharedSeenUrls("chilesuplementos_ofertas")
if not shared.register(url):
    continue  # ya fue procesada por el otro proceso
```

Internamente:
1. `_acquire_lock()` — crea el `.lock` con `os.O_CREAT | os.O_EXCL | os.O_WRONLY` (atómico en todos los OS)
2. Lee el JSON, verifica si la URL existe
3. Si no existe, la agrega y guarda
4. `_release_lock()` — elimina el `.lock`

**Por qué no `contains() + add()` separados:** entre las dos operaciones otro proceso podría insertar la misma URL → race condition. `register()` es la única operación pública válida.

---

## Auto-expiración Diaria

El JSON incluye `{"date": "YYYY-MM-DD", "urls": [...]}`. Si el archivo es de otro día, se descarta y se empieza de cero. Esto evita acumulación entre ejecuciones diarias.

---

## Gestión de Lock Stale

Si el proceso dueño del lock murió sin liberar, `_acquire_lock()` espera `LOCK_TIMEOUT = 10` segundos y luego elimina el `.lock` stale y reintenta.

---

## Referencias

- [[scraping/BaseScraper]]
- [[arquitectura/Flujo de Datos]]
