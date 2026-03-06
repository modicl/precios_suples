# Orquestador para ChileSuplementos Part1, Part2 y Part3
#
# Estrategia de ejecucion:
#   Fase 1 (paralelo): Part1 + Part2 corren simultaneamente.
#   Fase 2 (secuencial): Part3 corre DESPUES de que ambas terminen.
#
# Esto es necesario porque Part3 contiene la categoria "Ofertas", que usa
# SharedSeenUrls para deduplicar contra todos los productos scrapeados por
# Part1 y Part2. Si Part3 corriera en paralelo, veria el JSON compartido
# incompleto y podria importar duplicados.
#
# Uso:
#   python ChileSuplementosScraperRunner.py
#   python ChileSuplementosScraperRunner.py --headless

import subprocess
import sys
import os
import time
from datetime import datetime
from rich import print
from rich.console import Console
from rich.panel import Panel

console = Console()

SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRAPERS_DIR, ".."))

PART1_SCRIPT = os.path.join(SCRAPERS_DIR, "ChileSuplementosScraperPart1.py")
PART2_SCRIPT = os.path.join(SCRAPERS_DIR, "ChileSuplementosScraperPart2.py")
PART3_SCRIPT = os.path.join(SCRAPERS_DIR, "ChileSuplementosScraperPart3.py")

# Mismo nombre que usa SharedSeenUrls en BaseScraper
SHARED_JSON = os.path.join(PROJECT_ROOT, "raw_data", "chilesuplementos_ofertas.json")
SHARED_LOCK = os.path.join(PROJECT_ROOT, "raw_data", "chilesuplementos_ofertas.lock")


def _cleanup_shared_files():
    """Elimina el JSON y el .lock del registro compartido."""
    for path in (SHARED_JSON, SHARED_LOCK):
        if os.path.exists(path):
            try:
                os.remove(path)
                print(f"[dim]Eliminado: {os.path.basename(path)}[/dim]")
            except OSError as e:
                print(f"[yellow]No se pudo eliminar {path}: {e}[/yellow]")


def _launch(script: str, headless: bool) -> subprocess.Popen:
    """Lanza un scraper como subproceso y retorna el handle."""
    cmd = [sys.executable, script]
    if headless:
        cmd.append("--headless")
    name = os.path.basename(script)
    print(f"[cyan]Lanzando {name}...[/cyan]")
    return subprocess.Popen(
        cmd,
        cwd=SCRAPERS_DIR,
        stdout=None,
        stderr=None,
    )


def main():
    headless = "--headless" in sys.argv

    console.print(Panel(
        f"[bold green]ChileSuplementosScraperRunner[/bold green]\n"
        f"Modo headless: {headless}\n"
        f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Estrategia: Part1+Part2 en paralelo → Part3 secuencial",
        title="Orquestador",
        expand=False,
    ))

    print("[dim]Limpiando archivos compartidos residuales (si los hay)...[/dim]")
    _cleanup_shared_files()

    start = time.monotonic()

    # ── Fase 1: Part1 + Part2 en paralelo ────────────────────────────────────
    print("\n[bold yellow]Fase 1:[/bold yellow] Lanzando Part1 y Part2 en paralelo...")
    proc1 = _launch(PART1_SCRIPT, headless)
    proc2 = _launch(PART2_SCRIPT, headless)
    print(f"[bold]PID Part1={proc1.pid}  PID Part2={proc2.pid}[/bold]")
    print("Esperando a que terminen...\n")

    ret1 = proc1.wait()
    ret2 = proc2.wait()

    elapsed_phase1 = time.monotonic() - start
    m1, s1 = divmod(int(elapsed_phase1), 60)
    print(f"\n[bold]Fase 1 completada en {m1}m {s1}s.[/bold]  "
          f"Part1 exit={ret1}  Part2 exit={ret2}")

    # ── Fase 2: Part3 secuencial (Ofertas necesita el JSON completo) ──────────
    print(f"\n[bold yellow]Fase 2:[/bold yellow] Lanzando Part3 (Aminoacidos, Snacks, Ofertas, Packs)...")
    proc3 = _launch(PART3_SCRIPT, headless)
    print(f"[bold]PID Part3={proc3.pid}[/bold]")
    ret3 = proc3.wait()

    elapsed_total = time.monotonic() - start
    mt, st = divmod(int(elapsed_total), 60)

    print()
    console.print(Panel(
        f"Part1 exit code: [{'green' if ret1 == 0 else 'red'}]{ret1}[/]\n"
        f"Part2 exit code: [{'green' if ret2 == 0 else 'red'}]{ret2}[/]\n"
        f"Part3 exit code: [{'green' if ret3 == 0 else 'red'}]{ret3}[/]\n"
        f"Tiempo total:    {mt}m {st}s",
        title="Resultados",
        expand=False,
    ))

    print("\n[dim]Limpiando registro compartido de Ofertas...[/dim]")
    _cleanup_shared_files()
    print("[green]Sesión finalizada.[/green]")

    if ret1 != 0 or ret2 != 0 or ret3 != 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
