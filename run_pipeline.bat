@echo off
setlocal enabledelayedexpansion

set "ROOT=%~dp0"
set "PYTHON=%ROOT%venv\Scripts\python.exe"
set "STEP1=%ROOT%local_processing_testing\step1_clean_names.py"
set "STEP2=%ROOT%local_processing_testing\step2_normalization.py"
set "STEP3=%ROOT%local_processing_testing\step3_db_insertion.py"
set "STEP4=%ROOT%local_processing_testing\step4_deduplication.py"
set "STEP5=%ROOT%local_processing_testing\step5_refresh_views.py"
set "STEP6=%ROOT%local_processing_testing\step6_generate_descriptions.py"
set "LOGS_DIR=%ROOT%logs"

:: Crear carpeta logs si no existe
if not exist "%LOGS_DIR%" mkdir "%LOGS_DIR%"

:: Nombre del log con timestamp
for /f "tokens=1-6 delims=/: " %%a in ("%date% %time%") do (
    set "YYYY=%%c"
    set "MM=%%a"
    set "DD=%%b"
    set "HH=%%d"
    set "MIN=%%e"
    set "SEC=%%f"
)
set "LOGFILE=%LOGS_DIR%\pipeline_%YYYY%-%MM%-%DD%_%HH%-%MIN%-%SEC:.=%.log"

cls
echo.
echo  ===================================================
echo   Pipeline de Procesamiento - Precios Suples
echo  ===================================================
echo   Log: %LOGFILE%
echo  ===================================================
echo.
  echo  Los pasos se ejecutaran secuencialmente:
echo    Paso 1 - Limpieza de nombres
echo    Paso 2 - Normalizacion / clustering
echo    Paso 3 - Insercion en base de datos
echo    Paso 4 - Deduplicacion
echo    Paso 5 - Refresh de vistas materializadas
echo    Paso 6 - Generacion de descripciones LLM
echo.
pause

:: Iniciar log
echo Pipeline iniciado: %date% %time% > "%LOGFILE%"
echo. >> "%LOGFILE%"

:: -----------------------------------------------------------
:: PASO 1
:: -----------------------------------------------------------
echo.
echo  [1/6] Limpieza de nombres...
echo  [1/6] Limpieza de nombres... >> "%LOGFILE%"
echo  -------------------------------------------------- >> "%LOGFILE%"

"%PYTHON%" "%STEP1%" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%LOGFILE%' -Append"

if !errorlevel! neq 0 (
    echo.
    echo  [ERROR] Paso 1 fallo. Abortando pipeline.
    echo  [ERROR] Paso 1 fallo. >> "%LOGFILE%"
    goto fin_error
)

:: -----------------------------------------------------------
:: PASO 2
:: -----------------------------------------------------------
echo.
echo  [2/6] Normalizacion / clustering...
echo. >> "%LOGFILE%"
echo  [2/6] Normalizacion / clustering... >> "%LOGFILE%"
echo  -------------------------------------------------- >> "%LOGFILE%"

"%PYTHON%" "%STEP2%" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%LOGFILE%' -Append"

if !errorlevel! neq 0 (
    echo.
    echo  [ERROR] Paso 2 fallo. Abortando pipeline.
    echo  [ERROR] Paso 2 fallo. >> "%LOGFILE%"
    goto fin_error
)

:: -----------------------------------------------------------
:: PASO 3
:: -----------------------------------------------------------
echo.
echo  [3/6] Insercion en base de datos...
echo. >> "%LOGFILE%"
echo  [3/6] Insercion en base de datos... >> "%LOGFILE%"
echo  -------------------------------------------------- >> "%LOGFILE%"

"%PYTHON%" "%STEP3%" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%LOGFILE%' -Append"

if !errorlevel! neq 0 (
    echo.
    echo  [ERROR] Paso 3 fallo. Abortando pipeline.
    echo  [ERROR] Paso 3 fallo. >> "%LOGFILE%"
    goto fin_error
)

:: -----------------------------------------------------------
:: PASO 4
:: -----------------------------------------------------------
echo.
echo  [4/6] Deduplicacion...
echo. >> "%LOGFILE%"
echo  [4/6] Deduplicacion... >> "%LOGFILE%"
echo  -------------------------------------------------- >> "%LOGFILE%"

"%PYTHON%" "%STEP4%" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%LOGFILE%' -Append"

if !errorlevel! neq 0 (
    echo.
    echo  [ERROR] Paso 4 fallo. Abortando pipeline.
    echo  [ERROR] Paso 4 fallo. >> "%LOGFILE%"
    goto fin_error
)

:: -----------------------------------------------------------
:: PASO 5
:: -----------------------------------------------------------
echo.
echo  [5/6] Refresh de vistas materializadas...
echo. >> "%LOGFILE%"
echo  [5/6] Refresh de vistas materializadas... >> "%LOGFILE%"
echo  -------------------------------------------------- >> "%LOGFILE%"

"%PYTHON%" "%STEP5%" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%LOGFILE%' -Append"

if !errorlevel! neq 0 (
    echo.
    echo  [ERROR] Paso 5 fallo.
    echo  [ERROR] Paso 5 fallo. >> "%LOGFILE%"
    goto fin_error
)

:: -----------------------------------------------------------
:: PASO 6
:: -----------------------------------------------------------
echo.
echo  [6/6] Generacion de descripciones LLM...
echo. >> "%LOGFILE%"
echo  [6/6] Generacion de descripciones LLM... >> "%LOGFILE%"
echo  -------------------------------------------------- >> "%LOGFILE%"

"%PYTHON%" "%STEP6%" 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%LOGFILE%' -Append"

if !errorlevel! neq 0 (
    echo.
    echo  [ERROR] Paso 6 fallo.
    echo  [ERROR] Paso 6 fallo. >> "%LOGFILE%"
    goto fin_error
)

:: -----------------------------------------------------------
:: EXITO
:: -----------------------------------------------------------
echo.
echo  ===================================================
echo   Pipeline completado exitosamente.
echo   Log guardado en:
echo   %LOGFILE%
echo  ===================================================
echo.
echo Pipeline completado exitosamente: %date% %time% >> "%LOGFILE%"
goto fin

:fin_error
echo.
echo  ===================================================
echo   Pipeline interrumpido por error.
echo   Revisa el log en:
echo   %LOGFILE%
echo  ===================================================
echo.
echo Pipeline interrumpido: %date% %time% >> "%LOGFILE%"

:fin
endlocal
pause
