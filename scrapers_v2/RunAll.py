"""
RunAll.py
---------
Ejecuta todos los scrapers en paralelo como subprocesos independientes.

Cada tienda corre en su propio proceso con su propio browser Chromium,
por lo que no comparten IP de salida con otras tiendas (sin riesgo de ban).
ChileSuplementos se delega al ChileSuplementosScraperRunner que ya gestiona
sus dos partes en paralelo internamente.

Uso:
    python RunAll.py              # todos los scrapers, modo headless
    python RunAll.py --visible    # abre ventanas de browser (útil para debug)
    python RunAll.py --seq        # secuencial, un scraper a la vez
    python RunAll.py --workers 4  # paralelo limitado a N scrapers simultáneos
"""

import subprocess
import sys
import os
import time
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from rich import print
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    _RICH = True
except ImportError:
    _RICH = False
    class Console:
        def print(self, *a, **kw): print(*a)
    class Panel:
        def __init__(self, text, **kw): self._text = text
        def __str__(self): return self._text

console = Console()

SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRAPERS_DIR, ".."))

# ---------------------------------------------------------------------------
# Lista de scrapers
# ChileSuplementos se ejecuta via su Runner (que maneja Part1+Part2 en paralelo
# con el JSON compartido de deduplicación de Ofertas).
# ---------------------------------------------------------------------------
SCRAPERS = [
    # (nombre_display, script_relativo_a_SCRAPERS_DIR)
    ("ChileSuplementos",      "ChileSuplementosScraperRunner.py"),
    ("AllNutrition",          "AllNutritionScraper.py"),
    ("CruzVerde",             "CruzVerdeScraper.py"),
    ("Decathlon",             "DecathlonScraper.py"),
    ("DrSimi",                "DrSimiScraper.py"),
    ("FarmaciaKnop",          "FarmaciaKnopScraper.py"),
    ("Suples",                "SuplesScraper.py"),
    ("SuplementosBullChile",  "SuplementosBullChileScraper.py"),
    ("SuplementosMayoristas", "SuplementosMayoristasScraper.py"),
    ("SupleTech",             "SupleTechScraper.py"),
    ("SupleStore",            "SupleStoreScraper.py"),
    ("OneNutrition",          "OneNutritionScraper.py"),
    ("MuscleFactory",         "MuscleFactoryScraper.py"),
    ("FitMarketChile",        "FitMarketChileScraper.py"),
    ("Strongest",             "StrongestScraper.py"),
    ("WildFoods",             "WildFoodsScraper.py"),
]


def _launch(name: str, script: str, headless: bool) -> tuple[str, subprocess.Popen, float]:
    """Lanza un scraper como subproceso. Retorna (nombre, proceso, t_inicio)."""
    script_path = os.path.join(SCRAPERS_DIR, script)
    cmd = [sys.executable, script_path]
    # Solo ChileSuplementosScraperRunner.py acepta --headless como argumento CLI.
    # Los demás scrapers tienen headless=True hardcodeado en su __main__.
    if headless and script == "ChileSuplementosScraperRunner.py":
        cmd.append("--headless")

    # Prefijo en stdout/stderr para identificar de qué scraper viene cada línea
    # (usamos un archivo de log por scraper para no mezclar salidas)
    log_dir = os.path.join(PROJECT_ROOT, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(
        log_dir,
        f"{name.lower()}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    )

    log_file = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        cwd=SCRAPERS_DIR,
        stdout=log_file,
        stderr=log_file,
    )
    return name, proc, time.monotonic(), log_file, log_path


def _run_one(name: str, script: str, headless: bool) -> dict:
    """Lanza un scraper y espera a que termine. Retorna dict con resultado."""
    name, proc, t_start, log_file, log_path = _launch(name, script, headless)
    ret = proc.wait()
    elapsed = time.monotonic() - t_start
    log_file.close()
    return {
        "name":     name,
        "script":   script,
        "returncode": ret,
        "elapsed":  elapsed,
        "log":      log_path,
        "ok":       ret == 0,
    }


def _print_results(results: list[dict], total_elapsed: float):
    if _RICH:
        table = Table(title="Resultados", show_lines=True)
        table.add_column("Scraper",     style="bold")
        table.add_column("Estado",      justify="center")
        table.add_column("Duración",    justify="right")
        table.add_column("Exit code",   justify="center")
        table.add_column("Log")

        for r in sorted(results, key=lambda x: x["name"]):
            mins, secs = divmod(int(r["elapsed"]), 60)
            estado = "[green]OK[/green]" if r["ok"] else "[red]FAIL[/red]"
            table.add_row(
                r["name"],
                estado,
                f"{mins}m {secs}s",
                str(r["returncode"]),
                os.path.basename(r["log"]),
            )

        console.print(table)
    else:
        print("\n" + "=" * 60)
        print(f"{'Scraper':<30} {'Estado':<8} {'Duración':<12} {'Exit'}")
        print("-" * 60)
        for r in sorted(results, key=lambda x: x["name"]):
            mins, secs = divmod(int(r["elapsed"]), 60)
            estado = "OK" if r["ok"] else "FAIL"
            print(f"{r['name']:<30} {estado:<8} {mins}m {secs}s          {r['returncode']}")

    ok_count   = sum(1 for r in results if r["ok"])
    fail_count = len(results) - ok_count
    mins, secs = divmod(int(total_elapsed), 60)

    summary = (
        f"[bold green]{ok_count} exitosos[/bold green]  "
        f"[bold red]{fail_count} fallidos[/bold red]  "
        f"Tiempo total: {mins}m {secs}s"
    ) if _RICH else (
        f"{ok_count} exitosos  {fail_count} fallidos  Tiempo total: {mins}m {secs}s"
    )

    if _RICH:
        console.print(Panel(summary, title="Resumen", expand=False))
    else:
        print("\n" + summary)

    if fail_count:
        failed_names = [r["name"] for r in results if not r["ok"]]
        msg = "Scrapers con error: " + ", ".join(failed_names)
        if _RICH:
            print(f"[red]{msg}[/red]")
        else:
            print(msg)
        print("Revisa los archivos .log en la carpeta logs/")


def main():
    parser = argparse.ArgumentParser(description="Ejecuta todos los scrapers.")
    parser.add_argument(
        "--visible", action="store_true",
        help="Abre ventanas de browser (headless=False). Por defecto corre headless."
    )
    parser.add_argument(
        "--seq", action="store_true",
        help="Ejecuta los scrapers uno a la vez (secuencial)."
    )
    parser.add_argument(
        "--workers", type=int, default=None,
        help="Número máximo de scrapers en paralelo (por defecto: todos)."
    )
    args = parser.parse_args()

    headless = not args.visible

    # Determinar modo de ejecución
    if args.seq:
        workers = 1
        mode_label = "secuencial"
    elif args.workers:
        workers = args.workers
        mode_label = f"paralelo ({workers} simultáneos)"
    else:
        workers = len(SCRAPERS)
        mode_label = f"paralelo total ({workers} simultáneos)"

    header = (
        f"[bold green]RunAll — Todos los Scrapers[/bold green]\n"
        f"Modo:     {mode_label}\n"
        f"Headless: {headless}\n"
        f"Scrapers: {len(SCRAPERS)}\n"
        f"Inicio:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    if _RICH:
        console.print(Panel(header, title="RunAll", expand=False))
    else:
        print(header)

    t_global = time.monotonic()
    results = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_run_one, name, script, headless): name
            for name, script in SCRAPERS
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            status = "[green]OK[/green]" if result["ok"] else "[red]FAIL[/red]"
            mins, secs = divmod(int(result["elapsed"]), 60)
            if _RICH:
                print(f"  {status} [bold]{result['name']}[/bold] ({mins}m {secs}s)")
            else:
                print(f"  {'OK' if result['ok'] else 'FAIL'} {result['name']} ({mins}m {secs}s)")

    total_elapsed = time.monotonic() - t_global
    _print_results(results, total_elapsed)

    failed = [r for r in results if not r["ok"]]
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
