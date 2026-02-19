# Orquestador para ChileSuplementos Part1 y Part2
#
# Lanza ambos scrapers en paralelo como subprocesos independientes.
# Cuando los dos terminan (con éxito o con error), limpia el archivo
# JSON compartido que se usa para deduplicar la categoría Ofertas.
#
# Uso:
#   python RunChileSuplementos.py
#   python RunChileSuplementos.py --headless   (modo sin ventana)

import subprocess
import sys
import os
import time
from datetime import datetime
from rich import print
from rich.console import Console
from rich.panel import Panel

console = Console()

# Directorio donde viven los scrapers
SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRAPERS_DIR, ".."))

PART1_SCRIPT = os.path.join(SCRAPERS_DIR, "ChileSuplementosScraperPart1.py")
PART2_SCRIPT = os.path.join(SCRAPERS_DIR, "ChileSuplementosScraperPart2.py")

# Mismo nombre que usa SharedSeenUrls en BaseScraper
SHARED_JSON   = os.path.join(PROJECT_ROOT, "raw_data", "chilesuplementos_ofertas.json")
SHARED_LOCK   = os.path.join(PROJECT_ROOT, "raw_data", "chilesuplementos_ofertas.lock")


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
        cmd.append("--headless")  # los scrapers aceptan este argumento si se implementa
    name = os.path.basename(script)
    print(f"[cyan]Lanzando {name}...[/cyan]")
    return subprocess.Popen(
        cmd,
        cwd=SCRAPERS_DIR,
        # Heredar stdout/stderr para que los logs de rich se vean en la terminal
        stdout=None,
        stderr=None,
    )


def main():
    headless = "--headless" in sys.argv

    console.print(Panel(
        f"[bold green]RunChileSuplementos[/bold green]\n"
        f"Modo headless: {headless}\n"
        f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        title="Orquestador",
        expand=False,
    ))

    # Limpiar estado residual de una sesión anterior que haya abortado
    print("[dim]Limpiando archivos compartidos residuales (si los hay)...[/dim]")
    _cleanup_shared_files()

    start = time.monotonic()

    proc1 = _launch(PART1_SCRIPT, headless)
    proc2 = _launch(PART2_SCRIPT, headless)

    print(f"[bold]Ambos procesos lanzados. PID Part1={proc1.pid}  PID Part2={proc2.pid}[/bold]")
    print("Esperando a que terminen...\n")

    # Esperar a que ambos finalicen
    ret1 = proc1.wait()
    ret2 = proc2.wait()

    elapsed = time.monotonic() - start
    minutes, seconds = divmod(int(elapsed), 60)

    print()
    console.print(Panel(
        f"Part1 exit code: [{'green' if ret1 == 0 else 'red'}]{ret1}[/]\n"
        f"Part2 exit code: [{'green' if ret2 == 0 else 'red'}]{ret2}[/]\n"
        f"Tiempo total:    {minutes}m {seconds}s",
        title="Resultados",
        expand=False,
    ))

    # Limpiar el JSON compartido al finalizar la sesión
    print("\n[dim]Limpiando registro compartido de Ofertas...[/dim]")
    _cleanup_shared_files()
    print("[green]Sesión finalizada.[/green]")

    # Propagar error si alguno de los procesos falló
    if ret1 != 0 or ret2 != 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
