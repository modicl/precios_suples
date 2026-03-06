# Orquestador para BYON Part1 y Part2 (paralelo total)
import subprocess, sys, os, time
from datetime import datetime
from rich import print
from rich.console import Console
from rich.panel import Panel

console = Console()
SCRAPERS_DIR = os.path.dirname(os.path.abspath(__file__))

PART1_SCRIPT = os.path.join(SCRAPERS_DIR, "BYONScraperPart1.py")
PART2_SCRIPT = os.path.join(SCRAPERS_DIR, "BYONScraperPart2.py")


def _launch(script, headless):
    cmd = [sys.executable, script]
    if headless:
        cmd.append("--headless")
    print(f"[cyan]Lanzando {os.path.basename(script)}...[/cyan]")
    return subprocess.Popen(cmd, cwd=SCRAPERS_DIR)


def main():
    headless = "--headless" in sys.argv
    console.print(Panel(
        f"[bold green]BYONScraperRunner[/bold green]\n"
        f"Modo headless: {headless}\n"
        f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Estrategia: Part1 + Part2 en paralelo total",
        title="Orquestador", expand=False,
    ))
    start = time.monotonic()
    proc1 = _launch(PART1_SCRIPT, headless)
    proc2 = _launch(PART2_SCRIPT, headless)
    print(f"[bold]PID Part1={proc1.pid}  PID Part2={proc2.pid}[/bold]")
    ret1 = proc1.wait()
    ret2 = proc2.wait()
    elapsed = time.monotonic() - start
    m, s = divmod(int(elapsed), 60)
    console.print(Panel(
        f"Part1 exit: [{'green' if ret1 == 0 else 'red'}]{ret1}[/]\n"
        f"Part2 exit: [{'green' if ret2 == 0 else 'red'}]{ret2}[/]\n"
        f"Tiempo:     {m}m {s}s",
        title="Resultados", expand=False,
    ))
    print("[green]Sesión finalizada.[/green]")
    if ret1 != 0 or ret2 != 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
