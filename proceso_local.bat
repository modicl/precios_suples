@echo off
echo ===================================================
echo INICIANDO PROCESO COMPLETO V2 (Scraping + IA + Norm + DB + Clean + Views)
echo ===================================================

echo.
echo 1. LANZANDO SCRAPERS V2 (Modo Offline - Rapido)
echo ---------------------------------------------------

for %%f in (scrapers_v2\*.py) do (
    if "%%~nxf" neq "BaseScraper.py" (
        echo Lanzando %%~nxf...
        start "Scraper: %%~nxf" python "%%f"
    )
)

echo.
echo ---------------------------------------------------
echo [ESPERA] Espere a que TODAS las ventanas de scrapers se cierren.
echo ---------------------------------------------------
pause

echo.
echo 2. PASO 1: Limpieza Determinista de Nombres
echo ---------------------------------------------------
python local_processing_testing/step1_clean_names.py

echo.
echo 3. PASO 2: Normalizacion de Nombres (Fuzzy Matching)
echo ---------------------------------------------------
python local_processing_testing/step2_normalization.py

echo.
echo 4. PASO 3: Insercion en Base de Datos Local
echo ---------------------------------------------------
python local_processing_testing/step3_db_insertion.py

echo.
echo 5. PASO 4: Deduplicacion de Productos (Link Fusion)
echo ---------------------------------------------------
python local_processing_testing/step4_deduplication.py

echo.
echo 6. PASO 5: Refresco de Vistas Materializadas
echo ---------------------------------------------------
python local_processing_testing/step5_refresh_views.py

echo.
echo ===================================================
echo PROCESO FINALIZADO EXITOSAMENTE
echo ===================================================
pause
