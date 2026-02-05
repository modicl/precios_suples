@echo off
title Procesamiento de Datos
cd ..
echo Starting Post-Processing...
"venv\Scripts\python.exe" "run_post_processing.py"
echo.
echo Refreshing Materialized Views...
"venv\Scripts\python.exe" "data_processing\refresh_materialized_views.py"
echo.
echo Process finished.
pause
