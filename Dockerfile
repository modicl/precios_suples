# =============================================================================
# Dockerfile — ComparaFit Scraper
# Base: imagen oficial de Playwright para Python (incluye Chromium + deps de sistema)
# Target: Google Cloud Run Jobs
# =============================================================================

FROM mcr.microsoft.com/playwright/python:v1.50.0-noble

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

# Copiar solo el manifest primero para aprovechar caché de Docker:
# si requirements.txt no cambia, esta capa no se reconstruye.
COPY requirements.txt .

# Instalar dependencias en el Python del sistema (no se necesita venv en Docker)
RUN pip install --upgrade pip --no-cache-dir && \
    pip install --no-cache-dir -r requirements.txt

# Instalar SOLO Chromium (evita descargar Firefox y WebKit, ahorrando ~600MB).
# La imagen base ya incluye todas las dependencias de sistema necesarias.
RUN playwright install chromium

# Copiar el código fuente del proyecto
COPY scrapers_v2/              ./scrapers_v2/
COPY local_processing_testing/ ./local_processing_testing/
COPY shared/                   ./shared/
COPY diccionario_categorias.csv ./diccionario_categorias.csv
COPY main.py                   ./main.py

# Crear directorios de runtime que los scrapers esperan encontrar
RUN mkdir -p raw_data logs

# Variables de sharding — se inyectan desde Cloud Run Job.
# Valores por defecto cubren todos los scrapers.
ENV START_INDEX=0 \
    END_INDEX=999

# Cloud Run Jobs ejecuta el contenedor hasta que el proceso termina (exit 0 = OK).
ENTRYPOINT ["python", "main.py"]
