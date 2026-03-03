# Deploy — ComparaFit Cloud Run Jobs

Pasos a seguir cada vez que se actualiza el código o se hace un nuevo deploy.
Los pasos 1–6 (setup inicial de GCP) ya están completados.

---

## Cada deploy

### 7. Build de la imagen

```bash
docker build \
    --platform linux/amd64 \
    --tag us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest \
    .
```

> `--platform linux/amd64` es obligatorio si tu máquina es ARM (Apple Silicon) o Windows.

### 8. Push a Artifact Registry

```bash
docker push us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest
```

### 9. Crear el Job (primera vez)

```bash
gcloud run jobs create comparafit-scraper \
    --image=us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest \
    --region=us-central1 \
    --service-account=comparafit-runner@comparafit.iam.gserviceaccount.com \
    --memory=16Gi \
    --cpu=8 \
    --task-timeout=3600 \
    --max-retries=1 \
    --set-secrets="DB_HOST_PROD=DB_HOST_PROD:latest,CHATGPT_MINI4=CHATGPT_MINI4:latest,AWS_BUCKET_NAME=AWS_BUCKET_NAME:latest,AWS_REGION=AWS_REGION:latest,AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID:latest,AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY:latest" \
    --set-env-vars="START_INDEX=0,END_INDEX=19,MAX_SCRAPER_WORKERS=20"
```

### 9b. Actualizar el Job (deploys siguientes)

```bash
gcloud run jobs update comparafit-scraper \
    --image=us-central1-docker.pkg.dev/comparafit/comparafit/scraper:latest \
    --region=us-central1
```

---

## Ejecutar y monitorear

### 10. Lanzar el Job manualmente

```bash
gcloud run jobs execute comparafit-scraper --region=us-central1
```

### 11. Ver logs

```bash
# Listar ejecuciones
gcloud run jobs executions list \
    --job=comparafit-scraper \
    --region=us-central1

# Ver logs de una ejecución
gcloud logging read \
    "resource.type=cloud_run_job AND resource.labels.job_name=comparafit-scraper" \
    --limit=200 \
    --format="value(textPayload)"
```

---

## Actualizar un secreto

```bash
echo -n "nuevo_valor" | gcloud secrets versions add NOMBRE_SECRETO --data-file=-
```

El Job toma el nuevo valor en la próxima ejecución sin necesidad de hacer `update`.

---

## Automatizar con Cloud Scheduler

```bash
# Dar permiso al SA para invocar el Job (solo la primera vez)
gcloud projects add-iam-policy-binding comparafit \
    --member="serviceAccount:comparafit-runner@comparafit.iam.gserviceaccount.com" \
    --role="roles/run.invoker"

# Ejecutar a las 8:00, 13:00 y 19:00 hora Santiago
gcloud scheduler jobs create http comparafit-scraper-schedule \
    --location=us-central1 \
    --schedule="0 11,16,22 * * *" \
    --time-zone="America/Santiago" \
    --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/comparafit/jobs/comparafit-scraper:run" \
    --http-method=POST \
    --oauth-service-account-email=comparafit-runner@comparafit.iam.gserviceaccount.com
```
