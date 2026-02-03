@echo off
echo.
echo =========================================================
echo === Lanzando Proceso Completo de Precios Suplementos ===
echo =========================================================
echo.

:: Definir ruta al interprete de Python en el entorno virtual
set PYTHON_EXE=venv\Scripts\python.exe

if not exist "%PYTHON_EXE%" (
    echo [ERROR] No se encontro el entorno virtual en "%PYTHON_EXE%"
    echo Asegurate de haber creado el entorno virtual y que la carpeta 'venv' este en la raiz del proyecto.
    pause
    exit /b
)

echo Usando Python en: %PYTHON_EXE%
echo Ejecutando script run_full_process.py...
echo.

"%PYTHON_EXE%" run_full_process.py
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] El proceso termino con codigo de error %ERRORLEVEL%.
) else (
    echo.
    echo [EXITO] El proceso completo correctamente.
)

echo.
echo Presiona cualquier tecla para cerrar esta ventana...
pause
