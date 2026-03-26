"""
main.py — Entrypoint de Cloud Run Job para ComparaFit Scrapers
==============================================================

Lee las variables de entorno START_INDEX y END_INDEX para seleccionar
un subconjunto de la lista maestra de scrapers (sharding horizontal).

Esto permite dividir la carga en múltiples tareas de Cloud Run Job:
  Tarea 0: START_INDEX=0  END_INDEX=4   → scrapers 0..4
  Tarea 1: START_INDEX=5  END_INDEX=9   → scrapers 5..9
  Tarea 2: START_INDEX=10 END_INDEX=19  → scrapers 10..19  (resto)

Luego de que todos los scrapers del shard terminan, ejecuta el pipeline
completo de inserción de datos (step1 → step7).

Variables de entorno requeridas:
  START_INDEX   int  — índice inicial (inclusivo), default 0
  END_INDEX     int  — índice final (inclusivo), default len(SCRAPERS)-1
  DB_HOST_PROD  str  — connection string Neon/Postgres (desde Secret Manager)

Variables opcionales (mismas que db_multiconnect.py):
  DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME  — BD local/alternativa
"""

import os
import sys
import subprocess
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("main")

# ---------------------------------------------------------------------------
# Rutas base
# ---------------------------------------------------------------------------
ROOT          = Path(__file__).parent.resolve()
SCRAPERS_DIR  = ROOT / "scrapers_v2"
PIPELINE_DIR  = ROOT / "local_processing_testing"

# ---------------------------------------------------------------------------
# Lista maestra de scrapers (mismo orden que RunAll.py)
# Modifica este listado para agregar o quitar tiendas.
# ---------------------------------------------------------------------------
SCRAPERS = [
    # (nombre_display, script relativo a SCRAPERS_DIR)
    ("ChileSuplementos",       "ChileSuplementosScraperRunner.py"),
    ("AllNutrition",           "AllNutritionScraper.py"),
    ("Byon",                   "BYONScraper.py"),
    ("CruzVerde",              "CruzVerdeScraper.py"),
    ("Decathlon",              "DecathlonScraper.py"),
    ("DrSimi",                 "DrSimiScraper.py"),
    ("FarmaciaKnop",           "FarmaciaKnopScraper.py"),
    ("KoteSport",              "KoteSportScraper.py"),
    ("Suples",                 "SuplesScraper.py"),
    ("SuplementosBullChile",   "SuplementosBullChileScraper.py"),
    ("SuplementosMayoristas",  "SuplementosMayoristasScraper.py"),
    ("SportNutriShop",         "SportNutriShopScraper.py"),
    ("SupleTech",              "SupleTechScraper.py"),
    ("SupleStore",             "SupleStoreScraper.py"),
    ("OneNutrition",           "OneNutritionScraper.py"),
    ("MuscleFactory",          "MuscleFactoryScraper.py"),
    ("FitMarketChile",         "FitMarketChileScraper.py"),
    ("Strongest",              "StrongestScraper.py"),
    ("WildFoods",              "WildFoodsScraper.py"),
    ("WinklerNutrition",       "WinklerNutritionScraper.py"),
]

# Pipeline de inserción — pasos en orden estricto
PIPELINE_STEPS = [
    ("step1_clean_names",        "step1_clean_names.py"),
    ("step2_normalization",      "step2_normalization.py"),
    ("step3_db_insertion",       "step3_db_insertion.py"),
    ("step4_deduplication",      "step4_deduplication.py"),
    ("step5_descriptions",       "step5_generate_descriptions.py"),
    ("step6_tag_keywords",           "step6_tag_keywords.py"),
    ("step7_refresh_views",          "step7_refresh_views.py"),
    ("step8_trigger_notificaciones", "step8_trigger_notificaciones.py"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_subprocess(name: str, script: Path, cwd: Path, extra_args: list = None) -> dict:
    """
    Lanza un script Python como subproceso, captura su salida en tiempo real
    y la reenvía al logger. Retorna un dict con el resultado.
    """
    cmd = [sys.executable, str(script)] + (extra_args or [])
    log.info(f"[{name}] Iniciando → {script.name}")
    t_start = time.monotonic()

    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=os.environ.copy(),
    )

    # Stream de salida línea a línea hacia el log de Cloud Run
    for raw in proc.stdout:
        line = raw.decode("utf-8", errors="replace").rstrip()
        log.info(f"[{name}] {line}")

    ret      = proc.wait()
    elapsed  = time.monotonic() - t_start
    ok       = ret == 0

    level = logging.INFO if ok else logging.ERROR
    log.log(level, f"[{name}] Terminado — exit={ret}  duración={elapsed:.1f}s")

    return {"name": name, "script": script.name, "ok": ok, "elapsed": elapsed, "returncode": ret}


# ---------------------------------------------------------------------------
# Fase 1: Scrapers (paralelo, con límite de workers para Cloud Run)
# ---------------------------------------------------------------------------

def run_scrapers(shard: list[tuple]) -> list[dict]:
    """
    Ejecuta el shard de scrapers en paralelo.
    El límite de workers se configura via MAX_SCRAPER_WORKERS (default: 4).
    Con 2–4GB de RAM en Cloud Run, más de 4 Chromiums simultáneos es arriesgado.
    """
    max_workers = int(os.environ.get("MAX_SCRAPER_WORKERS", "4"))
    log.info(f"Ejecutando {len(shard)} scraper(s) con max_workers={max_workers}")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_run_subprocess, name, SCRAPERS_DIR / script, SCRAPERS_DIR): name
            for name, script in shard
        }
        for future in as_completed(futures):
            results.append(future.result())

    return results


# ---------------------------------------------------------------------------
# Fase 2: Pipeline de inserción (secuencial — cada step depende del anterior)
# ---------------------------------------------------------------------------

def run_pipeline() -> list[dict]:
    """
    Ejecuta los pasos de inserción en orden estricto.
    Si un step falla, se aborta el pipeline y se retorna con error.
    """
    results = []
    for name, script_name in PIPELINE_STEPS:
        script = PIPELINE_DIR / script_name
        result = _run_subprocess(name, script, ROOT)
        results.append(result)
        if not result["ok"]:
            log.error(f"Pipeline abortado en '{name}' (exit={result['returncode']})")
            break
    return results


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    # ── Leer variables de sharding ──────────────────────────────────────────
    start_idx = int(os.environ.get("START_INDEX", "0"))
    end_idx   = int(os.environ.get("END_INDEX",   str(len(SCRAPERS) - 1)))

    # Validar rango
    if start_idx < 0 or end_idx >= len(SCRAPERS) or start_idx > end_idx:
        log.error(
            f"Rango inválido: START_INDEX={start_idx}, END_INDEX={end_idx}. "
            f"Scrapers disponibles: 0..{len(SCRAPERS)-1}"
        )
        sys.exit(1)

    shard = SCRAPERS[start_idx : end_idx + 1]

    log.info("=" * 60)
    log.info(f"ComparaFit Cloud Run Job — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Shard: índices {start_idx}..{end_idx} ({len(shard)} scrapers)")
    for i, (name, _) in enumerate(shard):
        log.info(f"  [{start_idx + i}] {name}")
    log.info("=" * 60)

    # ── Fase 1: Scrapers ────────────────────────────────────────────────────
    t0 = time.monotonic()
    scraper_results = run_scrapers(shard)

    ok_scrapers   = [r for r in scraper_results if r["ok"]]
    fail_scrapers = [r for r in scraper_results if not r["ok"]]

    log.info(f"Scrapers: {len(ok_scrapers)} OK / {len(fail_scrapers)} FAIL")
    for r in fail_scrapers:
        log.error(f"  FAIL: {r['name']} (exit={r['returncode']})")

    # Abortar si ningún scraper tuvo éxito (no hay datos para insertar)
    if not ok_scrapers:
        log.error("Todos los scrapers fallaron. Abortando pipeline.")
        sys.exit(1)

    # ── Fase 2: Pipeline de inserción ───────────────────────────────────────
    log.info("-" * 60)
    log.info("Iniciando pipeline de inserción...")
    pipeline_results = run_pipeline()

    fail_pipeline = [r for r in pipeline_results if not r["ok"]]

    # ── Resumen final ────────────────────────────────────────────────────────
    total_elapsed = time.monotonic() - t0
    mins, secs = divmod(int(total_elapsed), 60)
    log.info("=" * 60)
    log.info(f"Job completado en {mins}m {secs}s")
    log.info(f"  Scrapers OK:   {len(ok_scrapers)}/{len(shard)}")
    log.info(f"  Pipeline OK:   {len(pipeline_results) - len(fail_pipeline)}/{len(PIPELINE_STEPS)}")
    log.info("=" * 60)

    # Exit code 1 si hubo algún fallo (Cloud Run marca la tarea como fallida)
    if fail_scrapers or fail_pipeline:
        sys.exit(1)


if __name__ == "__main__":
    main()
