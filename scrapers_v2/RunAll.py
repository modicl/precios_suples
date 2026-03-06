"""
RunAll.py
---------
Ejecuta todos los scrapers en paralelo como subprocesos independientes.

Cada tienda corre en su propio proceso con su propio browser Chromium,
por lo que no comparten IP de salida con otras tiendas (sin riesgo de ban).
ChileSuplementos se delega al ChileSuplementosScraperRunner que ya gestiona
sus partes en paralelo internamente.

Uso:
    python RunAll.py              # todos los scrapers, modo headless
    python RunAll.py --visible    # abre ventanas de browser (útil para debug)
    python RunAll.py --seq        # secuencial, un scraper a la vez
    python RunAll.py --workers 4  # paralelo limitado a N scrapers simultáneos
"""

import subprocess
import sys
import os
import csv
import time
import argparse
import threading
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
# Runners manejan sus partes en paralelo internamente.
# ---------------------------------------------------------------------------
SCRAPERS = [
    # (nombre_display, script_relativo_a_SCRAPERS_DIR)
    ("ChileSuplementos",      "ChileSuplementosScraperRunner.py"),
    ("AllNutrition",          "AllNutritionScraperRunner.py"),
    ("Byon",                  "BYONScraperRunner.py"),
    ("CruzVerde",             "CruzVerdeScraperRunner.py"),
    ("Decathlon",             "DecathlonScraper.py"),
    ("DrSimi",                "DrSimiScraper.py"),
    ("FarmaciaKnop",          "FarmaciaKnopScraperRunner.py"),
    ("KoteSport",             "KoteSportScraper.py"),
    ("Suples",                "SuplesScraperRunner.py"),
    ("SuplementosBullChile",  "SuplementosBullChileScraper.py"),
    ("SuplementosMayoristas", "SuplementosMayoristasScraper.py"),
    ("SportNutriShop",        "SportNutriShopScraperRunner.py"),
    ("SupleTech",             "SupleTechScraper.py"),
    ("SupleStore",            "SupleStoreScraper.py"),
    ("OneNutrition",          "OneNutritionScraper.py"),
    ("MuscleFactory",         "MuscleFactoryScraper.py"),
    ("FitMarketChile",        "FitMarketChileScraper.py"),
    ("Strongest",             "StrongestScraper.py"),
    ("WildFoods",             "WildFoodsScraper.py"),
    ("WinklerNutrition",      "WinklerNutritionScraper.py"),
]

# Reemplazos cuando se usa --api_mode (tiendas VTEX con API directa, sin browser)
SCRAPERS_API_OVERRIDES = {
    "SupleTech":             "SupleTechApiScraper.py",
    "SuplementosMayoristas": "SuplementosMayoristasApiScraper.py",
}

# Scripts que aceptan --headless como argumento CLI
RUNNER_SCRIPTS = {
    "ChileSuplementosScraperRunner.py",
    "AllNutritionScraperRunner.py",
    "BYONScraperRunner.py",
    "SuplesScraperRunner.py",
    "SportNutriShopScraperRunner.py",
    "FarmaciaKnopScraperRunner.py",
    "CruzVerdeScraperRunner.py",
}

_print_lock = threading.Lock()


def _stream_output(name: str, stream, log_file, prefix_color: str = "cyan"):
    """Lee líneas de 'stream' y las imprime en consola con prefijo [name], además de escribirlas al log."""
    prefix = f"[{name}]"
    for raw_line in stream:
        try:
            line = raw_line.decode("utf-8", errors="replace").rstrip("\n\r")
        except Exception:
            line = repr(raw_line)
        log_file.write(line + "\n")
        log_file.flush()
        with _print_lock:
            if _RICH:
                console.print(f"[{prefix_color}]{prefix}[/{prefix_color}] {line}")
            else:
                print(f"{prefix} {line}", flush=True)


def _run_one(name: str, script: str, headless: bool) -> dict:
    """Lanza un scraper, transmite su salida en tiempo real y espera a que termine."""
    script_path = os.path.join(SCRAPERS_DIR, script)
    cmd = [sys.executable, script_path]
    if headless and script in RUNNER_SCRIPTS:
        cmd.append("--headless")

    log_dir = os.path.join(PROJECT_ROOT, "logs", name.lower())
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(
        log_dir,
        f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    )

    log_file = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        cwd=SCRAPERS_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    t_start = time.monotonic()

    reader = threading.Thread(
        target=_stream_output,
        args=(name, proc.stdout, log_file),
        daemon=True,
    )
    reader.start()
    ret = proc.wait()
    reader.join()
    elapsed = time.monotonic() - t_start
    log_file.close()

    return {
        "name":       name,
        "script":     script,
        "returncode": ret,
        "elapsed":    elapsed,
        "log":        log_path,
        "ok":         ret == 0,
    }


def _print_results(results: list, total_elapsed: float):
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


def _count_products_from_csvs(start_time: float) -> dict:
    """
    Lee todos los CSVs en raw_data/ modificados después de start_time.
    Retorna un dict {site_name: count}.
    """
    raw_data_dir = os.path.join(PROJECT_ROOT, "raw_data")
    counts = {}
    if not os.path.isdir(raw_data_dir):
        return counts

    for fname in os.listdir(raw_data_dir):
        if not fname.endswith(".csv"):
            continue
        fpath = os.path.join(raw_data_dir, fname)
        if os.path.getmtime(fpath) < start_time:
            continue
        try:
            with open(fpath, encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                if "site_name" not in (reader.fieldnames or []):
                    continue
                for row in reader:
                    site = row.get("site_name", "").strip()
                    if site:
                        counts[site] = counts.get(site, 0) + 1
        except Exception:
            pass

    return counts


def _get_historical_averages() -> dict:
    """
    Consulta la BD local para obtener el promedio diario de productos
    por tienda en los últimos 14 días.
    Retorna {site_name: avg_daily_count} o {} si falla.
    """
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="suplementos", user="root", password="root",
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT
                t.nombre_tienda,
                ROUND(COUNT(*)::numeric / 14, 1) AS avg_daily
            FROM historia_precios hp
            JOIN producto_tienda pt ON pt.id_producto_tienda = hp.id_producto_tienda
            JOIN tiendas t ON t.id_tienda = pt.id_tienda
            WHERE hp.fecha_precio >= CURRENT_DATE - INTERVAL '14 days'
            GROUP BY t.nombre_tienda
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {row[0]: float(row[1]) for row in rows}
    except Exception as e:
        print(f"[yellow]No se pudo consultar histórico de BD: {e}[/yellow]")
        return {}


def _generate_report(results: list, start_time: float, total_elapsed: float):
    """
    Genera un reporte en logs/reporte_total/ con:
    - Productos encontrados por tienda (desde CSVs)
    - Duración total
    - Anomalías detectadas (count < 70% del promedio histórico)
    """
    run_dt = datetime.fromtimestamp(start_time)
    report_dir = os.path.join(PROJECT_ROOT, "logs", "reporte_total")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(
        report_dir,
        f"reporte_{run_dt.strftime('%Y-%m-%d_%H-%M-%S')}.txt"
    )

    counts = _count_products_from_csvs(start_time)
    historical = _get_historical_averages()

    total_mins, total_secs = divmod(int(total_elapsed), 60)
    total_products = sum(counts.values())

    lines = []
    lines.append("=" * 60)
    lines.append("  REPORTE DE SCRAPEO — RUNALL")
    lines.append(f"  Fecha/Hora: {run_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"  Duración total: {total_mins}m {total_secs}s")
    lines.append("=" * 60)
    lines.append("")
    lines.append("PRODUCTOS ENCONTRADOS POR TIENDA:")
    lines.append("-" * 60)
    lines.append(f"  {'Tienda':<30} {'Productos':>10}  {'Histórico (14d)':>16}  {'Estado'}")
    lines.append(f"  {'-'*30}  {'-'*10}  {'-'*16}  {'-'*10}")

    anomalies = []

    # Normalizar nombres para match entre CSV site_name y BD nombre_tienda
    # (ambos pueden diferir ligeramente; usamos best-effort)
    all_sites = set(counts.keys()) | set(historical.keys())

    def _best_hist(site_name):
        if site_name in historical:
            return historical[site_name]
        # Intento case-insensitive
        low = site_name.lower()
        for k, v in historical.items():
            if k.lower() == low:
                return v
        return None

    for site in sorted(all_sites):
        count = counts.get(site, 0)
        hist = _best_hist(site)
        if hist is not None and hist > 0:
            pct = count / hist
            if pct < 0.70:
                estado = f"ANOMALIA ({pct*100:.0f}% del promedio)"
                anomalies.append((site, count, hist, pct))
            else:
                estado = f"OK ({pct*100:.0f}%)"
            hist_str = f"{hist:.1f}"
        else:
            estado = "sin histórico"
            hist_str = "N/D"

        lines.append(f"  {site:<30} {count:>10}  {hist_str:>16}  {estado}")

    lines.append("-" * 60)
    lines.append(f"  {'TOTAL':<30} {total_products:>10}")
    lines.append("")

    lines.append("ESTADO DE SCRAPERS:")
    lines.append("-" * 60)
    for r in sorted(results, key=lambda x: x["name"]):
        mins, secs = divmod(int(r["elapsed"]), 60)
        estado = "OK" if r["ok"] else f"FALLO (exit {r['returncode']})"
        lines.append(f"  {r['name']:<30} {mins}m {secs}s  {estado}")

    lines.append("")

    if anomalies:
        lines.append("ANOMALIAS DETECTADAS (< 70% del promedio historico):")
        lines.append("-" * 60)
        for site, count, hist, pct in anomalies:
            lines.append(f"  !! {site}: {count} productos (esperado ~{hist:.0f}, {pct*100:.0f}%)")
        lines.append("")
    else:
        lines.append("Sin anomalias detectadas.")
        lines.append("")

    lines.append("=" * 60)

    report_text = "\n".join(lines)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    # Imprimir resumen en consola
    if _RICH:
        console.print(Panel(
            f"[bold]Reporte guardado:[/bold] {os.path.relpath(report_path, PROJECT_ROOT)}\n"
            f"Total productos: [bold]{total_products}[/bold] de {len(counts)} tiendas\n"
            + (f"[red bold]⚠ {len(anomalies)} anomalia(s) detectada(s)[/red bold]" if anomalies else "[green]Sin anomalias[/green]"),
            title="Reporte Final", expand=False,
        ))
        if anomalies:
            for site, count, hist, pct in anomalies:
                print(f"  [red]!! {site}: {count} productos (esperado ~{hist:.0f}, {pct*100:.0f}%)[/red]")
    else:
        print(f"\nReporte: {report_path}")
        print(f"Total productos: {total_products}")
        if anomalies:
            print("ANOMALIAS:")
            for site, count, hist, pct in anomalies:
                print(f"  !! {site}: {count} (esperado ~{hist:.0f}, {pct*100:.0f}%)")


def _run_quality_check():
    """Ejecuta check_data_quality.py en la raíz del proyecto al finalizar el scraping."""
    script = os.path.join(PROJECT_ROOT, "check_data_quality.py")
    if not os.path.exists(script):
        if _RICH:
            print("[yellow]check_data_quality.py no encontrado, omitiendo.[/yellow]")
        else:
            print("check_data_quality.py no encontrado, omitiendo.")
        return

    if _RICH:
        console.print(Panel("[bold]Ejecutando QA de datos...[/bold]", title="Data Quality Check", expand=False))
    else:
        print("\n" + "=" * 60)
        print("  DATA QUALITY CHECK")
        print("=" * 60)

    subprocess.run([sys.executable, script], cwd=PROJECT_ROOT)


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
    parser.add_argument(
        "--api_mode", action="store_true",
        help="Usar scrapers API para tiendas VTEX (SupleTech + SuplementosMayoristas). Sin browser, ~10x más rápido."
    )
    args = parser.parse_args()

    headless = not args.visible

    # Aplicar overrides API si corresponde
    scrapers = list(SCRAPERS)
    if args.api_mode:
        scrapers = [
            (name, SCRAPERS_API_OVERRIDES.get(name, script))
            for name, script in scrapers
        ]

    if args.seq:
        workers = 1
        mode_label = "secuencial"
    elif args.workers:
        workers = args.workers
        mode_label = f"paralelo ({workers} simultáneos)"
    else:
        workers = len(scrapers)
        mode_label = f"paralelo total ({workers} simultáneos)"

    api_label = "  [API mode: SupleTech + SuplementosMayoristas sin browser]\n" if args.api_mode else ""
    header = (
        f"[bold green]RunAll — Todos los Scrapers[/bold green]\n"
        f"{api_label}"
        f"Modo:     {mode_label}\n"
        f"Headless: {headless}\n"
        f"Scrapers: {len(scrapers)}\n"
        f"Inicio:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    if _RICH:
        console.print(Panel(header, title="RunAll", expand=False))
    else:
        print(header)

    t_global = time.monotonic()
    start_time_abs = time.time()  # timestamp absoluto para filtrar CSVs por mtime
    results = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_run_one, name, script, headless): name
            for name, script in scrapers
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
    _generate_report(results, start_time_abs, total_elapsed)
    _run_quality_check()

    failed = [r for r in results if not r["ok"]]
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
