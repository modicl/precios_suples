@echo off
setlocal

set "VENV_PYTHON=%~dp0venv\Scripts\python.exe"
set "SCRIPT=%~dp0scrapers_v2\RunAll.py"

:menu
cls
echo.
echo  ===================================================
echo   Run Scrapers - Precios Suples
echo  ===================================================
echo.
echo   1. Todos en paralelo   (rapido, ~3-5 GB RAM)
echo   2. Paralelo limitado   (N scrapers simultaneos)
echo   3. Secuencial          (lento, ~200-400 MB RAM)
echo.
echo   0. Salir
echo.
set /p "opcion=  Selecciona una opcion [0-3]: "

if "%opcion%"=="1" goto paralelo_total
if "%opcion%"=="2" goto paralelo_limitado
if "%opcion%"=="3" goto secuencial
if "%opcion%"=="0" goto fin
echo.
echo  Opcion invalida. Intenta de nuevo.
timeout /t 1 >nul
goto menu

:paralelo_total
cls
echo.
echo  Iniciando todos los scrapers en paralelo...
echo.
"%VENV_PYTHON%" "%SCRIPT%"
goto resultado

:paralelo_limitado
cls
echo.
set /p "workers=  Cuantos scrapers en paralelo? [ej: 4]: "
echo.
echo  Iniciando %workers% scrapers en paralelo...
echo.
"%VENV_PYTHON%" "%SCRIPT%" --workers %workers%
goto resultado

:secuencial
cls
echo.
echo  Iniciando scrapers en modo secuencial...
echo.
"%VENV_PYTHON%" "%SCRIPT%" --seq
goto resultado

:resultado
echo.
if %errorlevel%==0 (
    echo  Todos los scrapers finalizaron exitosamente.
) else (
    echo  Uno o mas scrapers fallaron. Revisa la carpeta logs\ para mas detalles.
)
echo.
pause
goto menu

:fin
endlocal
