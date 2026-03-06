"""
check_data_quality.py
=====================
Validacion de calidad de datos post-scraping, PRE-pipeline ETL.

Ejecutar DESPUES de run_scrapers.bat y ANTES de run_pipeline.bat.
No modifica datos ni detiene el flujo; solo analiza y reporta.

Salidas:
  - Resumen en consola (developer-friendly)
  - logs/reporte_calidad/reporte_calidad_YYYY-MM-DD_HH-MM-SS.html

Flujo recomendado:
    1. run_scrapers.bat              <- genera raw_data/*.csv
    2. python check_data_quality.py  <- ESTE SCRIPT (revisar antes de subir a prod)
    3. run_pipeline.bat              <- inserta a BD produccion

Uso:
    python check_data_quality.py
    python check_data_quality.py --dir raw_data
    python check_data_quality.py --file raw_data/mi_tienda.csv
"""

import os
import sys
import re
import glob
import argparse
from datetime import datetime

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS    = ["product_name", "brand", "price", "link", "category", "subcategory"]
MANDATORY_NON_NULL  = ["product_name", "price", "link"]   # campos criticos
TEXT_FIELDS         = ["product_name", "brand", "category", "subcategory"]

PRICE_MEDIAN_DEVIATION = 0.50   # >50% de desviacion vs mediana de categoria => Warning
MIN_VALID_PRICE        = 1      # precios menores son Critico

HTML_JUNK_RE = re.compile(
    r"<[^>]+>|&nbsp;|&amp;|&lt;|&gt;|&quot;|&#\d+;|\\n|\\t|\\r",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_issue(row_idx, product_name, store, column, message, severity):
    return {
        "row":          row_idx,
        "product_name": str(product_name)[:80],
        "store":        store,
        "column":       column,
        "message":      message,
        "severity":     severity,
    }

# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_schema(df, store):
    issues = []
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        issues.append(make_issue("N/A", "(todo el archivo)", store,
                                 ", ".join(missing),
                                 f"Columna(s) requerida(s) ausente(s): {missing}",
                                 "Critico"))

    if "price" in df.columns and not pd.api.types.is_numeric_dtype(df["price"]):
        coerced = pd.to_numeric(df["price"], errors="coerce")
        bad = df[coerced.isna() & df["price"].notna()]
        for idx, row in bad.iterrows():
            issues.append(make_issue(idx, row.get("product_name", ""), store,
                                     "price",
                                     f"Precio no numerico: '{row['price']}'",
                                     "Critico"))
    return issues


def check_nulls(df, store):
    issues = []
    present = [c for c in MANDATORY_NON_NULL if c in df.columns]
    for col in present:
        mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        for idx, row in df[mask].iterrows():
            issues.append(make_issue(idx, row.get("product_name", ""), store,
                                     col,
                                     f"Valor nulo / vacio en campo obligatorio '{col}'",
                                     "Critico"))
    return issues


def check_prices(df, store):
    issues = []
    if "price" not in df.columns:
        return issues

    prices = pd.to_numeric(df["price"], errors="coerce")
    df = df.copy()
    df["_price_num"] = prices

    # Precio negativo → Critico
    for idx, row in df[df["_price_num"] < 0].iterrows():
        issues.append(make_issue(idx, row.get("product_name", ""), store,
                                 "price",
                                 f"Precio negativo: {row['_price_num']} (dato corrupto)",
                                 "Critico"))

    # Precio cero → Warning (producto sin stock)
    for idx, row in df[df["_price_num"] == 0].iterrows():
        issues.append(make_issue(idx, row.get("product_name", ""), store,
                                 "price",
                                 f"Precio 0 — posiblemente sin stock",
                                 "Warning"))

    # Outlier vs mediana de subcategoria
    valid = df[df["_price_num"] > 0].copy()
    group_col = "subcategory" if "subcategory" in df.columns else "category"
    if group_col in df.columns and not valid.empty:
        medians = valid.groupby(group_col)["_price_num"].median()
        for idx, row in valid.iterrows():
            grp = row.get(group_col, "")
            if grp not in medians or medians[grp] == 0:
                continue
            ratio = abs(row["_price_num"] - medians[grp]) / medians[grp]
            if ratio > PRICE_MEDIAN_DEVIATION:
                direction = "alto" if row["_price_num"] > medians[grp] else "bajo"
                issues.append(make_issue(
                    idx, row.get("product_name", ""), store, "price",
                    (f"Precio {direction} ({row['_price_num']:,.0f}) — "
                     f"mediana '{grp}': {medians[grp]:,.0f} "
                     f"(desviacion: {ratio:.0%})"),
                    "Warning",
                ))
    return issues


def check_html_junk(df, store):
    issues = []
    present = [c for c in TEXT_FIELDS if c in df.columns]
    for col in present:
        for idx, row in df.iterrows():
            val = row.get(col, "")
            if not isinstance(val, str):
                continue
            if HTML_JUNK_RE.search(val):
                snippet = val[:60].replace("\n", "\\n").replace("\r", "\\r")
                issues.append(make_issue(idx, row.get("product_name", ""), store,
                                         col,
                                         f"Basura HTML/escape en '{col}': \"{snippet}\"",
                                         "Warning"))
    return issues


def check_duplicate_urls(df, store):
    issues = []
    if "link" not in df.columns:
        return issues
    dupes = df[df.duplicated(subset=["link"], keep=False) & df["link"].notna()]
    for idx, row in dupes.iterrows():
        issues.append(make_issue(idx, row.get("product_name", ""), store,
                                 "link",
                                 f"URL duplicada en el archivo: {str(row['link'])[:80]}",
                                 "Warning"))
    return issues

# ---------------------------------------------------------------------------
# Per-file analysis
# ---------------------------------------------------------------------------

def analyze_file(filepath):
    store = os.path.splitext(os.path.basename(filepath))[0]
    try:
        df = pd.read_csv(filepath, dtype=str)
    except Exception as e:
        return None, [make_issue("N/A", "(lectura fallida)", store, "archivo", str(e), "Critico")], store

    issues = []
    issues += check_schema(df, store)
    issues += check_nulls(df, store)
    issues += check_prices(df, store)
    issues += check_html_junk(df, store)
    issues += check_duplicate_urls(df, store)
    return df, issues, store

# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

BOLD   = "\033[1m"
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
RESET  = "\033[0m"


def print_console_report(all_results):
    total_rows     = sum(len(df) for df, _, _ in all_results if df is not None)
    total_criticos = sum(1 for _, iss, _ in all_results for i in iss if i["severity"] == "Critico")
    total_warnings = sum(1 for _, iss, _ in all_results for i in iss if i["severity"] == "Warning")
    total_ok       = max(total_rows - (total_criticos + total_warnings), 0)

    print(f"\n{BOLD}{CYAN}{'='*60}")
    print("  REPORTE DE CALIDAD DE DATOS — PRECIOS SUPLES")
    print(f"{'='*60}{RESET}")
    print(f"  Archivos analizados : {len(all_results)}")
    print(f"  Total de registros  : {BOLD}{total_rows}{RESET}")
    print(f"  Errores criticos    : {BOLD}{RED}{total_criticos}{RESET}")
    print(f"  Warnings            : {BOLD}{YELLOW}{total_warnings}{RESET}")
    print(f"  Registros OK (aprox): {BOLD}{GREEN}{total_ok}{RESET}")
    print(f"{CYAN}{'='*60}{RESET}\n")

    for df, issues, store in all_results:
        rows = len(df) if df is not None else 0
        criticos = sum(1 for i in issues if i["severity"] == "Critico")
        warnings  = sum(1 for i in issues if i["severity"] == "Warning")
        if criticos:
            status = f"{RED}[{criticos} crit]{RESET}"
        elif warnings:
            status = f"{YELLOW}[{warnings} warn]{RESET}"
        else:
            status = f"{GREEN}[OK]{RESET}"
        print(f"  {BOLD}{store:<45}{RESET}  {rows:>5} filas  {status}")
        for issue in [i for i in issues if i["severity"] == "Critico"][:3]:
            print(f"      {RED}x{RESET} [{issue['column']}] {issue['message'][:90]}")
    print()

# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Reporte de Calidad — Precios Suples</title>
<style>
  :root {{
    --bg: #0f1117; --surface: #1a1d27; --surface2: #23273a; --border: #2e3250;
    --text: #e2e8f0; --muted: #7c87a6;
    --red: #f56565; --red-bg: rgba(245,101,101,.12);
    --yellow: #ecc94b; --yellow-bg: rgba(236,201,75,.10);
    --green: #68d391; --green-bg: rgba(104,211,145,.10);
    --blue: #63b3ed; --blue-bg: rgba(99,179,237,.10);
    --radius: 10px; --font: 'Segoe UI', system-ui, sans-serif;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: var(--font); font-size: 14px; }}

  header {{
    background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 20px 32px; display: flex; align-items: center; gap: 16px;
  }}
  header h1 {{ font-size: 20px; font-weight: 700; }}
  header .ts {{ color: var(--muted); font-size: 12px; margin-top: 2px; }}
  .container {{ max-width: 1400px; margin: 0 auto; padding: 28px 32px; }}

  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px,1fr)); gap: 16px; margin-bottom: 28px; }}
  .card {{ background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 20px 24px; }}
  .card .label {{ font-size: 11px; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); margin-bottom: 8px; }}
  .card .value {{ font-size: 36px; font-weight: 700; line-height: 1; }}
  .card.red    {{ border-color: var(--red);    background: var(--red-bg);    }} .card.red    .value {{ color: var(--red);    }}
  .card.yellow {{ border-color: var(--yellow); background: var(--yellow-bg); }} .card.yellow .value {{ color: var(--yellow); }}
  .card.green  {{ border-color: var(--green);  background: var(--green-bg);  }} .card.green  .value {{ color: var(--green);  }}
  .card.blue   {{ border-color: var(--blue);   background: var(--blue-bg);   }} .card.blue   .value {{ color: var(--blue);   }}

  .filters {{ display: flex; gap: 10px; margin-bottom: 18px; flex-wrap: wrap; align-items: center; }}
  .filters span {{ color: var(--muted); font-size: 12px; margin-right: 4px; }}
  .btn {{
    padding: 7px 16px; border-radius: 6px; border: 1px solid var(--border);
    background: var(--surface2); color: var(--text); cursor: pointer; font-size: 13px; transition: all .15s;
  }}
  .btn:hover {{ border-color: var(--blue); color: var(--blue); }}
  .btn.active {{ background: var(--blue-bg); border-color: var(--blue); color: var(--blue); font-weight: 600; }}
  .btn.active.red    {{ background: var(--red-bg);    border-color: var(--red);    color: var(--red);    }}
  .btn.active.yellow {{ background: var(--yellow-bg); border-color: var(--yellow); color: var(--yellow); }}

  .store-section {{ margin-bottom: 32px; }}
  .store-header {{
    display: flex; align-items: center; gap: 12px; background: var(--surface);
    border: 1px solid var(--border); border-radius: var(--radius) var(--radius) 0 0;
    padding: 12px 20px; border-bottom: none;
  }}
  .store-header h2 {{ font-size: 15px; font-weight: 600; }}
  .badge {{ font-size: 11px; padding: 2px 9px; border-radius: 20px; font-weight: 600; }}
  .badge.crit {{ background: var(--red-bg);    color: var(--red);    border: 1px solid var(--red);    }}
  .badge.warn {{ background: var(--yellow-bg); color: var(--yellow); border: 1px solid var(--yellow); }}
  .badge.ok   {{ background: var(--green-bg);  color: var(--green);  border: 1px solid var(--green);  }}

  .table-wrap {{ border: 1px solid var(--border); border-radius: 0 0 var(--radius) var(--radius); overflow-x: auto; }}
  table {{ width: 100%; border-collapse: collapse; }}
  thead th {{
    background: var(--surface2); padding: 10px 14px; text-align: left;
    font-size: 11px; text-transform: uppercase; letter-spacing: .06em;
    color: var(--muted); border-bottom: 1px solid var(--border); white-space: nowrap;
  }}
  tbody tr {{ border-bottom: 1px solid var(--border); transition: background .1s; }}
  tbody tr:last-child {{ border-bottom: none; }}
  tbody tr:hover {{ background: var(--surface2); }}
  tbody td {{ padding: 9px 14px; vertical-align: middle; }}

  .sev {{ display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; white-space: nowrap; }}
  .sev.critico {{ background: var(--red-bg);    color: var(--red);    border: 1px solid var(--red);    }}
  .sev.warning {{ background: var(--yellow-bg); color: var(--yellow); border: 1px solid var(--yellow); }}

  .col-tag {{ background: var(--surface2); color: var(--blue); padding: 1px 8px; border-radius: 4px; font-family: monospace; font-size: 12px; }}
  .msg {{ color: var(--text); max-width: 500px; }}
  .row-num {{ color: var(--muted); font-family: monospace; font-size: 12px; }}
  .pname {{ font-weight: 500; max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .no-issues {{ padding: 20px; color: var(--green); background: var(--green-bg); border: 1px solid var(--border); border-radius: 0 0 var(--radius) var(--radius); text-align: center; font-size: 13px; }}
  footer {{ text-align: center; padding: 24px; color: var(--muted); font-size: 12px; }}
</style>
</head>
<body>
<header>
  <div>
    <h1>Reporte de Calidad de Datos</h1>
    <div class="ts">Precios Suples &mdash; Generado el {timestamp}</div>
  </div>
</header>
<div class="container">
  <div class="cards">
    <div class="card blue">  <div class="label">Total registros</div>   <div class="value">{total_rows}</div>     </div>
    <div class="card red">   <div class="label">Errores criticos</div>  <div class="value">{total_criticos}</div> </div>
    <div class="card yellow"><div class="label">Warnings</div>          <div class="value">{total_warnings}</div> </div>
    <div class="card green"> <div class="label">Registros OK</div>      <div class="value">{total_ok}</div>       </div>
    <div class="card blue">  <div class="label">Archivos</div>          <div class="value">{total_files}</div>    </div>
  </div>

  <div class="filters">
    <span>Filtrar:</span>
    <button class="btn active" id="btn-all"     onclick="filterTable('all')">Todos</button>
    <button class="btn red"    id="btn-critico" onclick="filterTable('Critico')">Solo Criticos</button>
    <button class="btn yellow" id="btn-warning" onclick="filterTable('Warning')">Solo Warnings</button>
  </div>

  {store_sections}
</div>
<footer>check_data_quality.py &mdash; Precios Suples ETL Pipeline</footer>
<script>
  function filterTable(sev) {{
    document.querySelectorAll('tbody tr[data-sev]').forEach(function(r) {{
      r.style.display = (sev === 'all' || r.dataset.sev === sev) ? '' : 'none';
    }});
    ['btn-all','btn-critico','btn-warning'].forEach(function(id) {{
      document.getElementById(id).classList.remove('active');
    }});
    document.getElementById({{ all:'btn-all', Critico:'btn-critico', Warning:'btn-warning' }}[sev]).classList.add('active');
  }}
</script>
</body>
</html>
"""

STORE_SECTION_TPL = """\
<div class="store-section">
  <div class="store-header">
    <h2>{store}</h2>
    <span class="badge {badge_class}">{badge_label}</span>
    <span style="color:var(--muted);font-size:12px;">{row_count} filas en CSV</span>
  </div>
  {table_or_ok}
</div>
"""

TABLE_TPL = """\
<div class="table-wrap"><table>
  <thead><tr>
    <th>#&nbsp;Fila</th><th>Producto</th><th>Columna</th><th>Mensaje</th><th>Severidad</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table></div>
"""


def _esc(text):
    return str(text).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")


def generate_html_report(all_results, output_path):
    total_rows     = sum(len(df) for df, _, _ in all_results if df is not None)
    total_criticos = sum(1 for _, iss, _ in all_results for i in iss if i["severity"] == "Critico")
    total_warnings = sum(1 for _, iss, _ in all_results for i in iss if i["severity"] == "Warning")
    total_ok       = max(total_rows - (total_criticos + total_warnings), 0)

    store_sections = ""
    for df, issues, store in all_results:
        row_count = len(df) if df is not None else 0
        criticos  = [i for i in issues if i["severity"] == "Critico"]
        warnings  = [i for i in issues if i["severity"] == "Warning"]

        if criticos:
            badge_class, badge_label = "crit", f"{len(criticos)} critico(s)"
        elif warnings:
            badge_class, badge_label = "warn", f"{len(warnings)} warning(s)"
        else:
            badge_class, badge_label = "ok", "OK"

        if issues:
            trs = "".join(
                f'<tr data-sev="{_esc(i["severity"])}">'
                f'<td class="row-num">{_esc(i["row"])}</td>'
                f'<td class="pname" title="{_esc(i["product_name"])}">{_esc(i["product_name"])}</td>'
                f'<td><span class="col-tag">{_esc(i["column"])}</span></td>'
                f'<td class="msg">{_esc(i["message"])}</td>'
                f'<td><span class="sev {"critico" if i["severity"]=="Critico" else "warning"}">'
                f'{_esc(i["severity"])}</span></td>'
                f'</tr>'
                for i in issues
            )
            table_or_ok = TABLE_TPL.format(rows=trs)
        else:
            table_or_ok = '<div class="no-issues">Sin problemas detectados</div>'

        store_sections += STORE_SECTION_TPL.format(
            store=_esc(store), badge_class=badge_class, badge_label=badge_label,
            row_count=row_count, table_or_ok=table_or_ok,
        )

    html = HTML_TEMPLATE.format(
        timestamp=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        total_rows=total_rows, total_criticos=total_criticos,
        total_warnings=total_warnings, total_ok=total_ok,
        total_files=len(all_results), store_sections=store_sections,
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="QA post-scraping para CSVs de raw_data/")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dir",  default="raw_data", help="Carpeta con CSVs (default: raw_data)")
    group.add_argument("--file", help="CSV individual a analizar")
    args = parser.parse_args()

    root = os.path.dirname(os.path.abspath(__file__))

    if args.file:
        csv_files = [os.path.join(root, args.file) if not os.path.isabs(args.file) else args.file]
    else:
        target_dir = os.path.join(root, args.dir)
        csv_files = sorted(glob.glob(os.path.join(target_dir, "*.csv")))

    if not csv_files:
        print(f"No se encontraron CSVs en '{args.dir}'. Ejecuta run_scrapers.bat primero.")
        sys.exit(0)

    all_results = [analyze_file(fp) for fp in csv_files]

    print_console_report(all_results)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    html_path = os.path.join(root, "logs", "reporte_calidad", f"reporte_calidad_{ts}.html")
    out = generate_html_report(all_results, html_path)
    print(f"  Reporte HTML guardado en: {out}\n")


if __name__ == "__main__":
    main()
