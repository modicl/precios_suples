# Orquestador para SportNutriShop Part1, Part2 y Part3
#
# Las 3 partes no comparten estado, por lo que corren en paralelo completo.
#
# Uso:
#   python SportNutriShopScraperRunner.py
#   python SportNutriShopScraperRunner.py --headless

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

PART1_SCRIPT = os.path.join(SCRAPERS_DIR, "SportNutriShopScraperPart1.py")
PART2_SCRIPT = os.path.join(SCRAPERS_DIR, "SportNutriShopScraperPart2.py")
PART3_SCRIPT = os.path.join(SCRAPERS_DIR, "SportNutriShopScraperPart3.py")


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
        f"[bold green]SportNutriShopScraperRunner[/bold green]\n"
        f"Modo headless: {headless}\n"
        f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Estrategia: Part1 + Part2 + Part3 en paralelo total",
        title="Orquestador",
        expand=False,
    ))

    start = time.monotonic()

    proc1 = _launch(PART1_SCRIPT, headless)
    proc2 = _launch(PART2_SCRIPT, headless)
    proc3 = _launch(PART3_SCRIPT, headless)

    print(f"[bold]Tres procesos lanzados. PID Part1={proc1.pid}  PID Part2={proc2.pid}  PID Part3={proc3.pid}[/bold]")
    print("Esperando a que terminen...\n")

    ret1 = proc1.wait()
    ret2 = proc2.wait()
    ret3 = proc3.wait()

    elapsed = time.monotonic() - start
    minutes, seconds = divmod(int(elapsed), 60)

    print()
    console.print(Panel(
        f"Part1 exit code: [{'green' if ret1 == 0 else 'red'}]{ret1}[/]\n"
        f"Part2 exit code: [{'green' if ret2 == 0 else 'red'}]{ret2}[/]\n"
        f"Part3 exit code: [{'green' if ret3 == 0 else 'red'}]{ret3}[/]\n"
        f"Tiempo total:    {minutes}m {seconds}s",
        title="Resultados",
        expand=False,
    ))

    print("[green]Sesión finalizada.[/green]")

    if ret1 != 0 or ret2 != 0 or ret3 != 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
