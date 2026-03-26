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
    ("OutletFit",             "OutletFitScraper.py"),
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


def _count_products_from_csvs_detailed(start_time: float) -> dict:
    """
    Lee todos los CSVs en raw_data/ modificados después de start_time.
    Retorna un dict {site_name: {csv_filename: count}}.
    """
    raw_data_dir = os.path.join(PROJECT_ROOT, "raw_data")
    detail = {}
    if not os.path.isdir(raw_data_dir):
        return detail

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
                        if site not in detail:
                            detail[site] = {}
                        detail[site][fname] = detail[site].get(fname, 0) + 1
        except Exception:
            pass

    return detail


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
    Genera un reporte HTML interactivo en logs/reporte_total/ con:
    - Filtros por tipo de problema y tienda
    - Tabla de productos por tienda con filas colapsables
    - KPI vs promedio histórico 14 días
    - Botones abrir/cerrar todos
    """
    run_dt = datetime.fromtimestamp(start_time)
    report_dir = os.path.join(PROJECT_ROOT, "logs", "reporte_total")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(
        report_dir,
        f"reporte_{run_dt.strftime('%Y-%m-%d_%H-%M-%S')}.html"
    )

    counts     = _count_products_from_csvs(start_time)
    csv_detail = _count_products_from_csvs_detailed(start_time)
    historical = _get_historical_averages()

    total_mins, total_secs = divmod(int(total_elapsed), 60)
    total_products = sum(counts.values())

    def _best_hist(site_name):
        if site_name in historical:
            return historical[site_name]
        low = site_name.lower()
        for k, v in historical.items():
            if k.lower() == low:
                return v
        return None

    def _best_result(site_name):
        low = site_name.lower().replace(" ", "")
        for r in results:
            if r["name"].lower().replace(" ", "") == low:
                return r
        return None

    all_sites  = set(counts.keys()) | set(historical.keys())
    anomalies  = []
    store_rows = []

    for site in sorted(all_sites):
        count  = counts.get(site, 0)
        hist   = _best_hist(site)
        result = _best_result(site)
        statuses = []
        pct = None

        if count == 0:
            statuses.append("zero")
        elif hist is not None and hist > 0:
            pct = count / hist
            if pct < 0.70:
                statuses.append("anomaly_low")
                anomalies.append((site, count, hist, pct))
            elif pct > 1.50:
                statuses.append("anomaly_high")
            else:
                statuses.append("ok")
        else:
            statuses.append("no_hist")

        if result and not result["ok"]:
            statuses.append("fail")

        if not statuses:
            statuses.append("ok")

        store_rows.append({
            "site":     site,
            "count":    count,
            "hist":     hist,
            "pct":      pct,
            "statuses": statuses,
            "result":   result,
            "csvs":     csv_detail.get(site, {}),
        })

    # ── Summary counts ────────────────────────────────────────────────
    n_zero         = sum(1 for r in store_rows if "zero"         in r["statuses"])
    n_anomaly_low  = sum(1 for r in store_rows if "anomaly_low"  in r["statuses"])
    n_anomaly_high = sum(1 for r in store_rows if "anomaly_high" in r["statuses"])
    n_fail         = sum(1 for r in store_rows if "fail"         in r["statuses"])
    n_ok           = sum(1 for r in store_rows if r["statuses"]  == ["ok"])
    n_stores       = len(store_rows)
    ok_scrapers    = sum(1 for r in results if r["ok"])
    fail_scrapers  = len(results) - ok_scrapers

    # ── Build HTML rows ───────────────────────────────────────────────
    rows_html = ""
    for i, row in enumerate(store_rows):
        site     = row["site"]
        count    = row["count"]
        hist     = row["hist"]
        pct      = row["pct"]
        statuses = row["statuses"]
        result   = row["result"]
        csvs     = row["csvs"]

        if "zero" in statuses:
            badge_cls, badge_text = "badge-danger",     "SIN PRODUCTOS"
        elif "anomaly_low" in statuses:
            badge_cls, badge_text = "badge-warning",    f"ANOMALÍA {pct*100:.0f}%"
        elif "anomaly_high" in statuses:
            badge_cls, badge_text = "badge-info",       f"ALTO {pct*100:.0f}%"
        elif "fail" in statuses:
            badge_cls, badge_text = "badge-danger",     "FALLO SCRAPER"
        elif "no_hist" in statuses:
            badge_cls, badge_text = "badge-secondary",  "SIN HIST."
        else:
            pct_label = f" {pct*100:.0f}%" if pct is not None else ""
            badge_cls, badge_text = "badge-success",    f"OK{pct_label}"

        hist_str = f"{hist:.1f}" if hist is not None else "N/D"
        pct_str  = f"{pct*100:.1f}%" if pct is not None else "—"

        if result:
            mins, secs   = divmod(int(result["elapsed"]), 60)
            duration_str = f"{mins}m {secs}s"
            log_rel      = os.path.relpath(result["log"], PROJECT_ROOT).replace("\\", "/")
            exit_str     = str(result["returncode"])
            scr_badge    = (
                '<span class="badge badge-success">OK</span>'
                if result["ok"] else
                f'<span class="badge badge-danger">FALLO ({result["returncode"]})</span>'
            )
        else:
            duration_str = "—"
            log_rel      = "—"
            exit_str     = "—"
            scr_badge    = '<span class="badge badge-secondary">—</span>'

        statuses_data = " ".join(statuses)

        csv_rows_html = "".join(
            f"<tr><td>{fn}</td><td class='num'>{cnt:,}</td></tr>"
            for fn, cnt in sorted(csvs.items())
        )
        csv_section = ""
        if csv_rows_html:
            csv_section = (
                '<div class="detail-block">'
                '<div class="detail-label">Archivos CSV</div>'
                '<table class="csv-inner">'
                '<tr><th>Archivo</th><th>Productos</th></tr>'
                + csv_rows_html +
                '</table></div>'
            )

        scraper_section = ""
        if result:
            scraper_section = (
                '<div class="detail-block">'
                '<div class="detail-label">Scraper</div>'
                '<div class="detail-meta">'
                f'<span><b>Exit:</b> {exit_str}</span>'
                f'<span><b>Duración:</b> {duration_str}</span>'
                f'<span><b>Log:</b> <code>{log_rel}</code></span>'
                '</div></div>'
            )

        rows_html += (
            f'<tr class="store-row" data-idx="{i}" data-site="{site}" '
            f'data-statuses="{statuses_data}" onclick="toggleDetail({i})">'
            f'<td class="expand-cell"><span class="expand-icon" id="icon-{i}">&#9654;</span>'
            f'<span class="site-name">{site}</span></td>'
            f'<td class="num">{count:,}</td>'
            f'<td class="num">{hist_str}</td>'
            f'<td class="num">{pct_str}</td>'
            f'<td><span class="badge {badge_cls}">{badge_text}</span></td>'
            f'<td>{scr_badge}</td>'
            f'<td class="num">{duration_str}</td>'
            f'</tr>'
            f'<tr class="detail-row" id="detail-{i}">'
            f'<td colspan="7"><div class="detail-content">'
            f'{scraper_section}{csv_section}'
            f'</div></td></tr>\n'
        )

    store_options = "\n".join(
        f'<option value="{r["site"]}">{r["site"]}</option>'
        for r in store_rows
    )

    run_date_str   = run_dt.strftime("%Y-%m-%d %H:%M:%S")
    duration_label = f"{total_mins}m {total_secs}s"
    card_ok_cls    = "green"  if n_ok == n_stores           else "blue"
    card_anom_cls  = "yellow" if n_anomaly_low > 0          else "blue"
    card_zero_cls  = "red"    if n_zero > 0                 else "blue"
    card_fail_cls  = "red"    if fail_scrapers > 0          else "blue"

    # ── CSS (plain string — no f-string needed) ───────────────────────
    css = (
        "*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }"
        "body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; }"
        ".header { background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); padding: 28px 32px 20px; border-bottom: 1px solid #334155; }"
        ".header h1 { font-size: 1.6rem; font-weight: 700; color: #f1f5f9; letter-spacing: -0.3px; }"
        ".header .subtitle { color: #64748b; font-size: 0.875rem; margin-top: 4px; }"
        ".cards { display: flex; flex-wrap: wrap; gap: 12px; padding: 20px 32px; background: #0f1117; }"
        ".card { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 14px 20px; min-width: 130px; flex: 1; }"
        ".card .card-val { font-size: 1.75rem; font-weight: 700; line-height: 1; }"
        ".card .card-label { font-size: 0.72rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }"
        ".card.red   .card-val { color: #f87171; }"
        ".card.yellow .card-val { color: #fbbf24; }"
        ".card.green  .card-val { color: #4ade80; }"
        ".card.blue   .card-val { color: #60a5fa; }"
        ".card.white  .card-val { color: #f1f5f9; }"
        ".filters { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; padding: 14px 32px; background: #0f1117; border-bottom: 1px solid #1e293b; }"
        ".filters label { font-size: 0.8rem; color: #94a3b8; }"
        ".filters select { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 6px; padding: 7px 28px 7px 12px; font-size: 0.85rem; cursor: pointer; appearance: none; -webkit-appearance: none; background-image: url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='8' viewBox='0 0 12 8'%3E%3Cpath fill='%2394a3b8' d='M1.41 0L6 4.58 10.59 0 12 1.41l-6 6-6-6z'/%3E%3C/svg%3E\"); background-repeat: no-repeat; background-position: right 10px center; }"
        ".filters select:focus { outline: none; border-color: #3b82f6; }"
        ".btn-group { margin-left: auto; display: flex; gap: 8px; }"
        ".btn { background: #1e293b; border: 1px solid #334155; color: #cbd5e1; border-radius: 6px; padding: 7px 14px; font-size: 0.82rem; cursor: pointer; transition: all 0.15s; }"
        ".btn:hover { background: #334155; color: #f1f5f9; }"
        ".btn.primary { background: #1d4ed8; border-color: #1d4ed8; color: white; }"
        ".btn.primary:hover { background: #2563eb; }"
        ".count-label { font-size: 0.8rem; color: #64748b; padding: 10px 32px 4px; }"
        ".table-wrap { padding: 12px 32px 40px; }"
        "table.main-table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }"
        "table.main-table thead th { padding: 10px 14px; text-align: left; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.6px; color: #64748b; background: #1e293b; border-bottom: 1px solid #334155; }"
        "table.main-table thead th.num { text-align: right; }"
        ".store-row { cursor: pointer; border-bottom: 1px solid #1e293b; transition: background 0.1s; }"
        ".store-row:hover { background: #1a2332; }"
        ".store-row td { padding: 11px 14px; vertical-align: middle; }"
        ".store-row td.num { text-align: right; font-variant-numeric: tabular-nums; color: #cbd5e1; }"
        ".expand-cell { display: flex; align-items: center; gap: 8px; }"
        ".expand-icon { font-size: 0.6rem; color: #475569; transition: transform 0.2s; display: inline-block; user-select: none; min-width: 12px; }"
        ".expand-icon.open { transform: rotate(90deg); color: #60a5fa; }"
        ".site-name { font-weight: 500; color: #f1f5f9; }"
        ".detail-row { display: none; border-bottom: 1px solid #1e293b; }"
        ".detail-row.visible { display: table-row; }"
        ".detail-row > td { padding: 0; }"
        ".detail-content { padding: 14px 14px 14px 40px; background: #131c2e; display: flex; flex-wrap: wrap; gap: 20px; }"
        ".detail-block { min-width: 220px; }"
        ".detail-label { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.5px; color: #64748b; margin-bottom: 6px; font-weight: 600; }"
        ".detail-meta { display: flex; flex-wrap: wrap; gap: 16px; font-size: 0.82rem; color: #94a3b8; }"
        ".detail-meta span b { color: #cbd5e1; }"
        ".detail-meta code { font-family: monospace; font-size: 0.78rem; color: #7dd3fc; background: #1e293b; padding: 2px 6px; border-radius: 3px; word-break: break-all; }"
        "table.csv-inner { font-size: 0.8rem; border-collapse: collapse; margin-top: 2px; }"
        "table.csv-inner th { color: #64748b; padding: 4px 14px; text-align: left; border-bottom: 1px solid #334155; font-weight: 600; font-size: 0.7rem; text-transform: uppercase; }"
        "table.csv-inner td { padding: 4px 14px; color: #94a3b8; border-bottom: 1px solid #1e293b; }"
        "table.csv-inner td.num { text-align: right; }"
        ".badge { display: inline-block; padding: 3px 9px; border-radius: 100px; font-size: 0.72rem; font-weight: 600; letter-spacing: 0.3px; white-space: nowrap; }"
        ".badge-success  { background: #14532d; color: #4ade80; }"
        ".badge-warning  { background: #451a03; color: #fbbf24; }"
        ".badge-danger   { background: #450a0a; color: #f87171; }"
        ".badge-info     { background: #0c2a4a; color: #60a5fa; }"
        ".badge-secondary { background: #1e293b; color: #64748b; border: 1px solid #334155; }"
        ".store-row.hidden { display: none !important; }"
    )

    # ── JS (plain string — no f-string needed) ────────────────────────
    js_template = (
        "var totalStores = TOTAL_STORES_PH;"
        "function toggleDetail(idx) {"
        "  var d = document.getElementById('detail-' + idx);"
        "  var ic = document.getElementById('icon-' + idx);"
        "  if (d.classList.contains('visible')) {"
        "    d.classList.remove('visible'); ic.classList.remove('open');"
        "  } else {"
        "    d.classList.add('visible'); ic.classList.add('open');"
        "  }"
        "}"
        "function expandAll() {"
        "  document.querySelectorAll('.store-row:not(.hidden)').forEach(function(row) {"
        "    var idx = row.getAttribute('data-idx');"
        "    var d = document.getElementById('detail-' + idx);"
        "    var ic = document.getElementById('icon-' + idx);"
        "    if (d && !d.classList.contains('visible')) {"
        "      d.classList.add('visible'); ic.classList.add('open');"
        "    }"
        "  });"
        "}"
        "function collapseAll() {"
        "  document.querySelectorAll('.detail-row.visible').forEach(function(d) {"
        "    d.classList.remove('visible');"
        "  });"
        "  document.querySelectorAll('.expand-icon.open').forEach(function(ic) {"
        "    ic.classList.remove('open');"
        "  });"
        "}"
        "function applyFilters() {"
        "  var typeF = document.getElementById('filterType').value;"
        "  var storeF = document.getElementById('filterStore').value;"
        "  var visible = 0;"
        "  document.querySelectorAll('.store-row').forEach(function(row) {"
        "    var site = row.getAttribute('data-site');"
        "    var sts = row.getAttribute('data-statuses').split(' ');"
        "    var idx = row.getAttribute('data-idx');"
        "    var d = document.getElementById('detail-' + idx);"
        "    var typeOk = (typeF === 'all') || (typeF === 'ok' ? (sts.length === 1 && sts[0] === 'ok') : sts.indexOf(typeF) !== -1);"
        "    var storeOk = (storeF === 'all') || (site === storeF);"
        "    if (typeOk && storeOk) {"
        "      row.classList.remove('hidden');"
        "      if (d) d.style.removeProperty('display');"
        "      visible++;"
        "    } else {"
        "      row.classList.add('hidden');"
        "      if (d) {"
        "        d.style.display = 'none';"
        "        d.classList.remove('visible');"
        "        var ic = document.getElementById('icon-' + idx);"
        "        if (ic) ic.classList.remove('open');"
        "      }"
        "    }"
        "  });"
        "  document.getElementById('countLabel').textContent ="
        "    'Mostrando ' + visible + ' de ' + totalStores + ' tiendas';"
        "}"
        "function resetFilters() {"
        "  document.getElementById('filterType').value = 'all';"
        "  document.getElementById('filterStore').value = 'all';"
        "  applyFilters();"
        "}"
    )
    js = js_template.replace("TOTAL_STORES_PH", str(n_stores))

    # ── Assemble HTML ─────────────────────────────────────────────────
    html = (
        f'<!DOCTYPE html><html lang="es"><head>'
        f'<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">'
        f'<title>Reporte RunAll — {run_date_str}</title>'
        f'<style>{css}</style></head><body>'

        f'<div class="header">'
        f'<h1>Reporte de Scrapeo — RunAll</h1>'
        f'<div class="subtitle">{run_date_str} &nbsp;·&nbsp; Duración: {duration_label} &nbsp;·&nbsp; {n_stores} tiendas</div>'
        f'</div>'

        f'<div class="cards">'
        f'<div class="card white"><div class="card-val">{total_products:,}</div><div class="card-label">Productos totales</div></div>'
        f'<div class="card {card_ok_cls}"><div class="card-val">{n_ok}</div><div class="card-label">Tiendas OK</div></div>'
        f'<div class="card {card_anom_cls}"><div class="card-val">{n_anomaly_low}</div><div class="card-label">Anomalías bajas</div></div>'
        f'<div class="card {card_zero_cls}"><div class="card-val">{n_zero}</div><div class="card-label">Sin productos</div></div>'
        f'<div class="card {card_fail_cls}"><div class="card-val">{fail_scrapers}</div><div class="card-label">Fallos scraper</div></div>'
        f'<div class="card blue"><div class="card-val">{ok_scrapers}/{len(results)}</div><div class="card-label">Scrapers OK</div></div>'
        f'<div class="card white"><div class="card-val">{duration_label}</div><div class="card-label">Duración total</div></div>'
        f'</div>'

        f'<div class="filters">'
        f'<label>Problema:</label>'
        f'<select id="filterType" onchange="applyFilters()">'
        f'<option value="all">Todos</option>'
        f'<option value="zero">Sin productos (0)</option>'
        f'<option value="anomaly_low">Anomalía baja (&lt;70%)</option>'
        f'<option value="anomaly_high">Anomalía alta (&gt;150%)</option>'
        f'<option value="fail">Fallo de scraper</option>'
        f'<option value="no_hist">Sin histórico</option>'
        f'<option value="ok">OK</option>'
        f'</select>'
        f'<label>Tienda:</label>'
        f'<select id="filterStore" onchange="applyFilters()">'
        f'<option value="all">Todas las tiendas</option>'
        f'{store_options}'
        f'</select>'
        f'<div class="btn-group">'
        f'<button class="btn" onclick="expandAll()">Abrir todos</button>'
        f'<button class="btn" onclick="collapseAll()">Cerrar todos</button>'
        f'<button class="btn primary" onclick="resetFilters()">Limpiar filtros</button>'
        f'</div></div>'

        f'<div class="count-label" id="countLabel">Mostrando {n_stores} de {n_stores} tiendas</div>'

        f'<div class="table-wrap">'
        f'<table class="main-table">'
        f'<thead><tr>'
        f'<th>Tienda</th>'
        f'<th class="num">Productos</th>'
        f'<th class="num">Hist. 14d</th>'
        f'<th class="num">vs. Hist.</th>'
        f'<th>Estado</th>'
        f'<th>Scraper</th>'
        f'<th class="num">Duración</th>'
        f'</tr></thead>'
        f'<tbody>\n{rows_html}</tbody>'
        f'</table></div>'

        f'<script>{js}</script>'
        f'</body></html>'
    )

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    # ── Console output ────────────────────────────────────────────────
    if _RICH:
        console.print(Panel(
            f"[bold]Reporte guardado:[/bold] {os.path.relpath(report_path, PROJECT_ROOT)}\n"
            f"Total productos: [bold]{total_products:,}[/bold] de {len(counts)} tiendas\n"
            + (f"[red bold]⚠ {len(anomalies)} anomalia(s) detectada(s)[/red bold]" if anomalies else "[green]Sin anomalias[/green]"),
            title="Reporte Final", expand=False,
        ))
        if anomalies:
            for site, count, hist, pct in anomalies:
                print(f"  [red]!! {site}: {count} productos (esperado ~{hist:.0f}, {pct*100:.0f}%)[/red]")
    else:
        print(f"\nReporte: {report_path}")
        print(f"Total productos: {total_products:,}")
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
