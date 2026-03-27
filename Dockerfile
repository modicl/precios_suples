FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

# Logs en tiempo real en Cloud Run (sin buffering de Python)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar dependencias Python en capa separada (aprovecha caché de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente (filtrado por .dockerignore)
COPY . .

# Eliminar .env si fue copiado accidentalmente — los secretos van por env vars de Cloud Run
RUN rm -f .env

# Crear directorios de escritura en tiempo de ejecución
RUN mkdir -p raw_data logs/reporte_total logs/reporte_calidad

CMD ["python", "main.py"]
