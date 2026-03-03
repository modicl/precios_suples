#!/usr/bin/env bash
# =============================================================================
# deploy.sh — ComparaFit: build, push y deploy en Google Cloud Run Jobs
#
# USO:
#   chmod +x deploy.sh
#   ./deploy.sh
#
# REQUISITOS PREVIOS:
#   1. gcloud CLI instalado: https://cloud.google.com/sdk/docs/install
#   2. Autenticado: gcloud auth login && gcloud auth configure-docker us-central1-docker.pkg.dev
#   3. APIs habilitadas (ver sección de setup inicial abajo)
#   4. Secretos creados en Secret Manager (ver sección de secretos abajo)
# =============================================================================

set -euo pipefail   # Abortar en cualquier error

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN — editar según el entorno
# ─────────────────────────────────────────────────────────────────────────────
PROJECT_ID="comparafit"
REGION="us-central1"
REPO_NAME="comparafit"                           # Nombre del repositorio en Artifact Registry
IMAGE_NAME="scraper"
JOB_NAME="comparafit-scraper"
SA_NAME="comparafit-runner"                      # Service Account que ejecuta el Job

# URI completa de la imagen
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}"

# ─────────────────────────────────────────────────────────────────────────────
# SETUP INICIAL (solo la primera vez — comentar si ya está hecho)
# ─────────────────────────────────────────────────────────────────────────────

# 1. Habilitar APIs necesarias
# gcloud services enable \
#     artifactregistry.googleapis.com \
#     run.googleapis.com \
#     secretmanager.googleapis.com \
#     cloudscheduler.googleapis.com \
#     --project="${PROJECT_ID}"

# 2. Crear repositorio Docker en Artifact Registry
# gcloud artifacts repositories create "${REPO_NAME}" \
#     --repository-format=docker \
#     --location="${REGION}" \
#     --description="ComparaFit scraper images" \
#     --project="${PROJECT_ID}"

# 3. Configurar Docker para autenticarse con Artifact Registry
# gcloud auth configure-docker "${REGION}-docker.pkg.dev"

# 4. Crear Service Account con los permisos mínimos necesarios
# gcloud iam service-accounts create "${SA_NAME}" \
#     --display-name="ComparaFit Runner" \
#     --project="${PROJECT_ID}"

# 5. Dar acceso a Secret Manager (para leer DB_HOST_PROD y otros secretos)
# gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
#     --member="serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
#     --role="roles/secretmanager.secretAccessor"

# 6. Dar acceso a Cloud Run para subir logs
# gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
#     --member="serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
#     --role="roles/logging.logWriter"

# ─────────────────────────────────────────────────────────────────────────────
# SECRETOS EN SECRET MANAGER (solo la primera vez — comentar si ya existen)
#
# Los secretos se montan como variables de entorno en el Job.
# NUNCA poner valores sensibles en el Dockerfile ni en gcloud run jobs create.
# ─────────────────────────────────────────────────────────────────────────────

# Crear secreto para la conexión a Neon (connection string completo):
# echo -n "postgresql://user:password@host/dbname?sslmode=require" | \
#     gcloud secrets create DB_HOST_PROD \
#         --data-file=- \
#         --project="${PROJECT_ID}"

# Crear secreto para la API key de OpenAI (usada en step5 y step6):
# echo -n "sk-..." | \
#     gcloud secrets create CHATGPT_MINI4 \
#         --data-file=- \
#         --project="${PROJECT_ID}"

# Crear secretos de AWS S3 (para subir imágenes de productos):
# echo -n "my-bucket-name" | gcloud secrets create AWS_BUCKET_NAME --data-file=- --project="${PROJECT_ID}"
# echo -n "us-east-1"      | gcloud secrets create AWS_REGION --data-file=- --project="${PROJECT_ID}"
# echo -n "AKIAIOSFODNN7E" | gcloud secrets create AWS_ACCESS_KEY_ID --data-file=- --project="${PROJECT_ID}"
# echo -n "wJalrXUtnFEMI"  | gcloud secrets create AWS_SECRET_ACCESS_KEY --data-file=- --project="${PROJECT_ID}"

# Para actualizar un secreto existente (nueva versión):
# echo -n "nuevo_valor" | gcloud secrets versions add NOMBRE_SECRETO --data-file=- --project="${PROJECT_ID}"

# ─────────────────────────────────────────────────────────────────────────────
# BUILD Y PUSH DE LA IMAGEN
# ─────────────────────────────────────────────────────────────────────────────

TAG="${IMAGE_URI}:$(date +%Y%m%d-%H%M%S)"
LATEST="${IMAGE_URI}:latest"

echo ">>> Building image: ${TAG}"
docker build \
    --platform linux/amd64 \
    --tag "${TAG}" \
    --tag "${LATEST}" \
    .

echo ">>> Pushing image to Artifact Registry..."
docker push "${TAG}"
docker push "${LATEST}"

echo ">>> Image pushed: ${TAG}"

# ─────────────────────────────────────────────────────────────────────────────
# CREAR O ACTUALIZAR EL CLOUD RUN JOB
#
# El Job se configura para correr UN shard (todos los scrapers) por defecto.
# Para sharding horizontal, usar el override --tasks y --set-env-vars por tarea
# (ver sección de ejecución con sharding abajo).
# ─────────────────────────────────────────────────────────────────────────────

SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo ">>> Updating Cloud Run Job: ${JOB_NAME}..."
gcloud run jobs update "${JOB_NAME}" \
    --image="${LATEST}" \
    --region="${REGION}" \
    --service-account="${SA_EMAIL}" \
    --memory="16Gi" \
    --cpu="8" \
    --task-timeout="3600" \
    --max-retries="1" \
    --set-secrets="DB_HOST_PROD=DB_HOST_PROD:latest,CHATGPT_MINI4=CHATGPT_MINI4:latest,AWS_BUCKET_NAME=AWS_BUCKET_NAME:latest,AWS_REGION=AWS_REGION:latest,AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID:latest,AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY:latest" \
    --set-env-vars="START_INDEX=0,END_INDEX=19,MAX_SCRAPER_WORKERS=20" \
    --project="${PROJECT_ID}" \
    || \
gcloud run jobs create "${JOB_NAME}" \
    --image="${LATEST}" \
    --region="${REGION}" \
    --service-account="${SA_EMAIL}" \
    --memory="16Gi" \
    --cpu="8" \
    --task-timeout="3600" \
    --max-retries="1" \
    --set-secrets="DB_HOST_PROD=DB_HOST_PROD:latest,CHATGPT_MINI4=CHATGPT_MINI4:latest,AWS_BUCKET_NAME=AWS_BUCKET_NAME:latest,AWS_REGION=AWS_REGION:latest,AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID:latest,AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY:latest" \
    --set-env-vars="START_INDEX=0,END_INDEX=19,MAX_SCRAPER_WORKERS=20" \
    --project="${PROJECT_ID}"

echo ">>> Job deployed: ${JOB_NAME}"

# ─────────────────────────────────────────────────────────────────────────────
# EJECUCIÓN MANUAL (sin esperar scheduler)
# ─────────────────────────────────────────────────────────────────────────────

# Ejecutar con todos los scrapers (1 shard, modo por defecto):
# gcloud run jobs execute "${JOB_NAME}" \
#     --region="${REGION}" \
#     --project="${PROJECT_ID}"

# Ver logs en tiempo real:
# gcloud run jobs executions list --job="${JOB_NAME}" --region="${REGION}" --project="${PROJECT_ID}"
# gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}" \
#     --project="${PROJECT_ID}" --limit=200 --format="value(textPayload)"

# ─────────────────────────────────────────────────────────────────────────────
# CLOUD SCHEDULER — ejecución automática varias veces al día
#
# Requiere: gcloud services enable cloudscheduler.googleapis.com
# ─────────────────────────────────────────────────────────────────────────────

# Crear un trigger que ejecuta el Job a las 8:00, 13:00 y 19:00 (hora de Santiago, UTC-3):
# Las horas en UTC serían 11:00, 16:00 y 22:00.
#
# gcloud scheduler jobs create http "comparafit-scraper-schedule" \
#     --location="${REGION}" \
#     --schedule="0 11,16,22 * * *" \
#     --time-zone="America/Santiago" \
#     --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
#     --http-method=POST \
#     --oauth-service-account-email="${SA_EMAIL}" \
#     --project="${PROJECT_ID}"
#
# Nota: el SA necesita el rol roles/run.invoker para disparar el Job:
# gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
#     --member="serviceAccount:${SA_EMAIL}" \
#     --role="roles/run.invoker"

echo ">>> Deploy completo."
