"""
check_dom_health.py
===================
Verifica que los selectores CSS de cada scraper siguen funcionando
contra las páginas web en vivo. Detecta cambios de estructura DOM
antes de que contaminen los datos.

Uso:
    python check_dom_health.py                   # Todas las tiendas
    python check_dom_health.py --store KoteSport # Solo una tienda
    python check_dom_health.py --visible         # Con browser visible (debug)
    python check_dom_health.py --timeout 20      # Timeout en segundos por tienda (default: 18)

Salidas:
    - Resumen en consola con estado por tienda
    - logs/reporte_calidad/dom_health_YYYY-MM-DD_HH-MM-SS.html

Cuándo ejecutar:
    - Después de un run con anomalías (pocos productos)
    - Antes de deployar cambios de selectores
    - Periódicamente (semanal) para detección proactiva
"""

import os
import sys
import argparse
from datetime import datetime

from playwright.sync_api import sync_playwright

# Config de scrapers (mismo directorio raíz tiene scrapers_v2/)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scrapers_v2"))
from health_check_config import STORES

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT_S = 18   # segundos por tienda
WAIT_AFTER_LOAD_MS = 3000

BOLD   = "\033[1m"
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
RESET  = "\033[0m"

# ---------------------------------------------------------------------------
# Playwright check: una tienda
# ---------------------------------------------------------------------------

def check_store(page, store: dict, timeout_ms: int) -> dict:
    """
    Navega a test_url y verifica cada selector.
    Retorna un dict con los resultados por selector.
    """
    name        = store["name"]
    url         = store["test_url"]
    selectors   = store["selectors"]
    min_prods   = store.get("min_products", 3)

    result = {
        "name":         name,
        "url":          url,
        "page_loaded":  False,
        "load_error":   None,
        "selector_results": {},   # selector_key -> {"css": ..., "count": int, "status": OK/BROKEN}
        "products_found": 0,
        "min_products": min_prods,
        "status":       "CRITICO",   # OK / WARNING / CRITICO
    }

    # --- Navegación ---
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(WAIT_AFTER_LOAD_MS)
        result["page_loaded"] = True
    except Exception as e:
        result["load_error"] = str(e)[:200]
        return result

    # --- Verificar selectores ---
    broken_keys = []
    for key, css in selectors.items():
        try:
            # CSS puede tener múltiples alternativas separadas por coma
            # Playwright eval cuenta todos los que coincidan
            count = page.locator(css).count()
        except Exception as e:
            count = -1  # selector inválido / error inesperado

        status = "OK" if count > 0 else "BROKEN"
        if status == "BROKEN":
            broken_keys.append(key)

        result["selector_results"][key] = {
            "css":    css,
            "count":  count,
            "status": status,
        }

    # Productos encontrados = count del selector product_card
    if "product_card" in result["selector_results"]:
        result["products_found"] = result["selector_results"]["product_card"]["count"]

    # --- Estado global ---
    has_broken   = len(broken_keys) > 0
    below_min    = result["products_found"] < min_prods and result["products_found"] >= 0
    card_broken  = "product_card" in broken_keys

    if card_broken or (has_broken and below_min):
        result["status"] = "CRITICO"
    elif has_broken or below_min:
        result["status"] = "WARNING"
    else:
        result["status"] = "OK"

    return result

# ---------------------------------------------------------------------------
# Consola
# ---------------------------------------------------------------------------

def print_console_report(results: list):
    total    = len(results)
    ok       = sum(1 for r in results if r["status"] == "OK")
    warnings = sum(1 for r in results if r["status"] == "WARNING")
    criticos = sum(1 for r in results if r["status"] == "CRITICO")

    print(f"\n{BOLD}{CYAN}{'='*62}")
    print("  DOM HEALTH CHECK — PRECIOS SUPLES")
    print(f"{'='*62}{RESET}")
    print(f"  Tiendas verificadas : {total}")
    print(f"  OK                  : {BOLD}{GREEN}{ok}{RESET}")
    print(f"  Warnings            : {BOLD}{YELLOW}{warnings}{RESET}")
    print(f"  Criticos            : {BOLD}{RED}{criticos}{RESET}")
    print(f"{CYAN}{'='*62}{RESET}\n")

    for r in results:
        stt = r["status"]
        if stt == "OK":
            badge = f"{GREEN}[OK]{RESET}"
        elif stt == "WARNING":
            badge = f"{YELLOW}[WARNING]{RESET}"
        else:
            badge = f"{RED}[CRITICO]{RESET}"

        prods = r["products_found"]
        print(f"  {BOLD}{r['name']:<28}{RESET}  {prods:>3} productos  {badge}")

        if r["load_error"]:
            print(f"      {RED}x Página no cargó: {r['load_error'][:80]}{RESET}")
            continue

        for key, info in r["selector_results"].items():
            if info["status"] == "BROKEN":
                print(f"      {RED}x [{key}] 0 elementos — selector roto: {info['css'][:70]}{RESET}")

        if r["products_found"] < r["min_products"] and r["page_loaded"]:
            print(f"      {YELLOW}! Productos ({r['products_found']}) < mínimo esperado ({r['min_products']}){RESET}")
    print()

# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

_HTML = """\
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DOM Health Check — Precios Suples</title>
<style>
  :root {{
    --bg:#0f1117;--surface:#1a1d27;--surface2:#23273a;--border:#2e3250;
    --text:#e2e8f0;--muted:#7c87a6;
    --red:#f56565;--red-bg:rgba(245,101,101,.12);
    --yellow:#ecc94b;--yellow-bg:rgba(236,201,75,.10);
    --green:#68d391;--green-bg:rgba(104,211,145,.10);
    --blue:#63b3ed;--blue-bg:rgba(99,179,237,.10);
    --radius:10px;--font:'Segoe UI',system-ui,sans-serif;
  }}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:var(--font);font-size:14px}}
  header{{background:var(--surface);border-bottom:1px solid var(--border);padding:20px 32px}}
  header h1{{font-size:20px;font-weight:700}}
  header .ts{{color:var(--muted);font-size:12px;margin-top:2px}}
  .container{{max-width:1200px;margin:0 auto;padding:28px 32px}}
  .cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:16px;margin-bottom:28px}}
  .card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px 24px}}
  .card .label{{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:8px}}
  .card .value{{font-size:36px;font-weight:700}}
  .card.red   {{border-color:var(--red);   background:var(--red-bg);   }} .card.red    .value{{color:var(--red)}}
  .card.yellow{{border-color:var(--yellow);background:var(--yellow-bg);}} .card.yellow .value{{color:var(--yellow)}}
  .card.green {{border-color:var(--green); background:var(--green-bg); }} .card.green  .value{{color:var(--green)}}
  .card.blue  {{border-color:var(--blue);  background:var(--blue-bg);  }} .card.blue   .value{{color:var(--blue)}}
  .filters{{display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap;align-items:center}}
  .filters span{{color:var(--muted);font-size:12px}}
  .btn{{padding:7px 16px;border-radius:6px;border:1px solid var(--border);background:var(--surface2);color:var(--text);cursor:pointer;font-size:13px;transition:all .15s}}
  .btn:hover{{border-color:var(--blue);color:var(--blue)}}
  .btn.active{{background:var(--blue-bg);border-color:var(--blue);color:var(--blue);font-weight:600}}
  .btn.active.red{{background:var(--red-bg);border-color:var(--red);color:var(--red)}}
  .btn.active.yellow{{background:var(--yellow-bg);border-color:var(--yellow);color:var(--yellow)}}
  .store-block{{margin-bottom:24px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}}
  .store-header{{display:flex;align-items:center;gap:12px;background:var(--surface);padding:12px 20px;border-bottom:1px solid var(--border)}}
  .store-header h2{{font-size:15px;font-weight:600;flex:1}}
  .store-url{{color:var(--muted);font-size:11px;font-family:monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:380px}}
  .badge{{font-size:11px;padding:2px 10px;border-radius:20px;font-weight:700;white-space:nowrap}}
  .badge.ok    {{background:var(--green-bg);color:var(--green);border:1px solid var(--green)}}
  .badge.warn  {{background:var(--yellow-bg);color:var(--yellow);border:1px solid var(--yellow)}}
  .badge.crit  {{background:var(--red-bg);color:var(--red);border:1px solid var(--red)}}
  .prod-count{{font-size:12px;color:var(--muted);white-space:nowrap}}
  .sel-table{{width:100%;border-collapse:collapse}}
  .sel-table th{{background:var(--surface2);padding:8px 16px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);border-bottom:1px solid var(--border)}}
  .sel-table td{{padding:9px 16px;border-bottom:1px solid var(--border);vertical-align:middle}}
  .sel-table tr:last-child td{{border-bottom:none}}
  .sel-table tr:hover td{{background:var(--surface2)}}
  .sel-key{{font-family:monospace;font-size:12px;background:var(--surface2);padding:1px 8px;border-radius:4px;color:var(--blue)}}
  .sel-css{{font-family:monospace;font-size:11px;color:var(--muted);max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
  .sel-count{{font-family:monospace;font-size:13px;font-weight:600}}
  .ok-count{{color:var(--green)}}  .broken-count{{color:var(--red)}}
  .status-pill{{display:inline-block;padding:2px 10px;border-radius:20px;font-size:11px;font-weight:700}}
  .status-ok    {{background:var(--green-bg);color:var(--green);border:1px solid var(--green)}}
  .status-broken{{background:var(--red-bg);color:var(--red);border:1px solid var(--red)}}
  .err-box{{padding:14px 20px;background:var(--red-bg);color:var(--red);font-size:13px;font-family:monospace}}
  .note-box{{padding:10px 20px;background:var(--surface2);color:var(--muted);font-size:12px}}
  footer{{text-align:center;padding:24px;color:var(--muted);font-size:12px}}
</style>
</head>
<body>
<header>
  <div>
    <h1>DOM Health Check — Scrapers</h1>
    <div class="ts">Precios Suples &mdash; Generado el {timestamp}</div>
  </div>
</header>
<div class="container">
  <div class="cards">
    <div class="card blue">  <div class="label">Tiendas</div>     <div class="value">{total}</div>    </div>
    <div class="card green"> <div class="label">OK</div>          <div class="value">{n_ok}</div>     </div>
    <div class="card yellow"><div class="label">Warnings</div>    <div class="value">{n_warn}</div>   </div>
    <div class="card red">   <div class="label">Criticos</div>    <div class="value">{n_crit}</div>   </div>
  </div>

  <div class="filters">
    <span>Filtrar:</span>
    <button class="btn active" id="fa" onclick="filter('all')">Todas</button>
    <button class="btn red"    id="fc" onclick="filter('crit')">Solo Criticos</button>
    <button class="btn yellow" id="fw" onclick="filter('warn')">Solo Warnings</button>
  </div>

  {store_blocks}
</div>
<footer>check_dom_health.py &mdash; Precios Suples ETL Pipeline</footer>
<script>
function filter(f){{
  document.querySelectorAll('.store-block[data-status]').forEach(b=>{{
    b.style.display=(f==='all'||b.dataset.status===f)?'':'none';
  }});
  ['fa','fc','fw'].forEach(id=>document.getElementById(id).classList.remove('active'));
  document.getElementById({{all:'fa',crit:'fc',warn:'fw'}}[f]).classList.add('active');
}}
</script>
</body>
</html>
"""

_STORE_BLOCK = """\
<div class="store-block" data-status="{data_status}">
  <div class="store-header">
    <h2>{name}</h2>
    <span class="badge {badge_cls}">{badge_txt}</span>
    <span class="prod-count">{prod_info}</span>
    <span class="store-url" title="{url}">{url}</span>
  </div>
  {body}
</div>
"""

def _esc(t):
    return str(t).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")


def generate_html_report(results: list, output_path: str):
    n_ok   = sum(1 for r in results if r["status"] == "OK")
    n_warn = sum(1 for r in results if r["status"] == "WARNING")
    n_crit = sum(1 for r in results if r["status"] == "CRITICO")

    blocks = ""
    for r in results:
        stt = r["status"]
        badge_cls = {"OK": "ok", "WARNING": "warn", "CRITICO": "crit"}[stt]
        badge_txt = stt
        data_status = {"OK": "all", "WARNING": "warn", "CRITICO": "crit"}[stt]
        prod_info = f"{r['products_found']} productos (mín. {r['min_products']})"

        if r["load_error"]:
            body = f'<div class="err-box">Error cargando página: {_esc(r["load_error"])}</div>'
        else:
            rows = ""
            for key, info in r["selector_results"].items():
                cnt = info["count"]
                cnt_cls = "ok-count" if cnt > 0 else "broken-count"
                pill_cls = "status-ok" if info["status"] == "OK" else "status-broken"
                rows += (
                    f'<tr>'
                    f'<td><span class="sel-key">{_esc(key)}</span></td>'
                    f'<td><span class="sel-css" title="{_esc(info["css"])}">{_esc(info["css"])}</span></td>'
                    f'<td><span class="sel-count {cnt_cls}">{cnt if cnt >= 0 else "error"}</span></td>'
                    f'<td><span class="status-pill {pill_cls}">{_esc(info["status"])}</span></td>'
                    f'</tr>'
                )
            body = (
                '<table class="sel-table">'
                '<thead><tr><th>Selector</th><th>CSS</th><th>Elementos</th><th>Estado</th></tr></thead>'
                f'<tbody>{rows}</tbody></table>'
            )
            note = r.get("note") or ""
            if note:
                body += f'<div class="note-box">Nota: {_esc(note)}</div>'

        blocks += _STORE_BLOCK.format(
            data_status=data_status, name=_esc(r["name"]),
            badge_cls=badge_cls, badge_txt=badge_txt,
            prod_info=_esc(prod_info), url=_esc(r["url"]),
            body=body,
        )

    html = _HTML.format(
        timestamp=datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        total=len(results), n_ok=n_ok, n_warn=n_warn, n_crit=n_crit,
        store_blocks=blocks,
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Verifica selectores CSS de scrapers contra páginas en vivo")
    parser.add_argument("--store",   help="Verificar solo una tienda (nombre exacto o parcial)")
    parser.add_argument("--visible", action="store_true", help="Abrir browser visible (debug)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S,
                        help=f"Timeout en segundos por tienda (default: {DEFAULT_TIMEOUT_S})")
    args = parser.parse_args()

    stores_to_check = STORES
    if args.store:
        q = args.store.lower()
        stores_to_check = [s for s in STORES if q in s["name"].lower()]
        if not stores_to_check:
            print(f"{RED}No se encontró tienda con nombre '{args.store}'.{RESET}")
            print(f"Tiendas disponibles: {', '.join(s['name'] for s in STORES)}")
            sys.exit(1)

    timeout_ms = args.timeout * 1000
    results = []

    print(f"\n{BOLD}{CYAN}Iniciando DOM Health Check para {len(stores_to_check)} tienda(s)...{RESET}\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=not args.visible,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="es-CL",
            timezone_id="America/Santiago",
            viewport={"width": 1366, "height": 768},
        )
        context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        )
        page = context.new_page()

        for store in stores_to_check:
            print(f"  Verificando {BOLD}{store['name']}{RESET}...", end=" ", flush=True)
            r = check_store(page, store, timeout_ms)
            # Adjuntar nota si existe en config
            if "note" in store:
                r["note"] = store["note"]
            results.append(r)

            stt = r["status"]
            color = GREEN if stt == "OK" else (YELLOW if stt == "WARNING" else RED)
            prods = r["products_found"]
            broken = [k for k, v in r["selector_results"].items() if v["status"] == "BROKEN"]
            detail = f"{prods} productos"
            if broken:
                detail += f" | rotos: {', '.join(broken)}"
            if r["load_error"]:
                detail = f"ERROR: {r['load_error'][:60]}"
            print(f"{color}[{stt}]{RESET} {detail}")

        browser.close()

    print_console_report(results)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "logs", "reporte_calidad", f"dom_health_{ts}.html"
    )
    out = generate_html_report(results, out_path)
    print(f"  Reporte HTML guardado en: {out}\n")


if __name__ == "__main__":
    main()
