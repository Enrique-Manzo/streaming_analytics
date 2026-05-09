# Alpha Vantage News Collector — Guía de Despliegue

## Estructura del proyecto

```
news-collector/
├── main.py             # Punto de entrada del Cloud Run Job (sin servidor)
├── contract.py         # Contrato de datos Pydantic: NewsArticle
├── observability.py    # ObservabilityState + envío a Prometheus Pushgateway
├── news_collector.py   # Llamada a la API, filtrado, validación y publicación en Pub/Sub
├── config.py           # Constantes (sobreescribibles mediante variables de entorno o .env)
├── requirements.txt
├── Dockerfile
└── .env                # Credenciales de la API — NO incluir en control de versiones
```

---

## Visión general de la arquitectura

```
Cloud Scheduler (cada 5 min)
        │
        ▼
Cloud Run Job  ──── Alpha Vantage API
        │                (NEWS_SENTIMENT)
        │
        ├──► Topic de Pub/Sub: financial_news
        │
        └──► Prometheus Pushgateway  ◄──── Prometheus (scraping en VM de GCP)
               (VM de GCP, puerto 9091)
```

Este collector es **stateless y de vida corta**. Arranca, realiza su trabajo, envía las métricas y termina. Prometheus hace scraping del Pushgateway de forma independiente.

---

## Inicio rápido (local)

### 1. Configura tus credenciales

Edita el archivo `.env`:

```
ALPHAVANTAGE_API_KEY=tu_key_aquí
GCP_PROJECT_ID=tu-gcp-project-id
PUSHGATEWAY_HOST=ip-pública-de-tu-vm
```

### 2. Instala y ejecuta

```bash
pip install -r requirements.txt
python main.py
```

---

## Despliegue en GCP Cloud Run Jobs

### Requisitos previos
- Proyecto GCP con facturación habilitada
- CLI `gcloud` autenticado
- Repositorio de Artifact Registry creado
- Topic de Pub/Sub `financial_news` creado
- Prometheus Pushgateway en ejecución en tu VM de GCP (puerto 9091 por defecto)

### 1. Define tus variables

```bash
export PROJECT_ID=tu-gcp-project-id
export REGION=us-central1
export REPO=news-collector
export IMAGE=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/news-collector:latest
```

### 2. Habilita las APIs

```bash
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  pubsub.googleapis.com \
  cloudscheduler.googleapis.com \
  --project=$PROJECT_ID
```

### 3. Crea el topic de Pub/Sub (una sola vez)

```bash
gcloud pubsub topics create financial_news --project=$PROJECT_ID
```

### 4. Construye y sube la imagen

```bash
gcloud auth configure-docker $REGION-docker.pkg.dev
docker build -t $IMAGE .
docker push $IMAGE
```

### 5. Crea el Cloud Run Job

```bash
gcloud run jobs create news-collector \
  --image=$IMAGE \
  --region=$REGION \
  --project=$PROJECT_ID \
  --task-timeout=120 \
  --max-retries=2 \
  --memory=256Mi \
  --cpu=1 \
  --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,PUSHGATEWAY_HOST=ip-de-tu-vm,NEWS_LOOKBACK_MINUTES=5"
```

Almacena los valores sensibles en Secret Manager y pásalos mediante `--set-secrets`:

```bash
gcloud run jobs update news-collector \
  --region=$REGION \
  --set-secrets="ALPHAVANTAGE_API_KEY=ALPHAVANTAGE_API_KEY:latest"
```

### 6. Programa la ejecución con Cloud Scheduler (cada 5 minutos)

```bash
gcloud scheduler jobs create http news-collector-trigger \
  --location=$REGION \
  --schedule="*/5 * * * *" \
  --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/news-collector:run" \
  --http-method=POST \
  --oauth-service-account-email=TU_CUENTA_DE_SERVICIO@$PROJECT_ID.iam.gserviceaccount.com
```

### 7. Ver los logs

```bash
gcloud logging read \
  "resource.type=cloud_run_job AND resource.labels.job_name=news-collector" \
  --project=$PROJECT_ID \
  --limit=50 \
  --format="value(textPayload)"
```

---

## Configuración del Pushgateway en la VM de GCP

Si no está ya en ejecución, inicia el Pushgateway en tu VM compartida:

```bash
# Con Docker
docker run -d -p 9091:9091 --name pushgateway prom/pushgateway

# O con un volumen de datos persistente
docker run -d -p 9091:9091 --name pushgateway \
  -v pushgateway-data:/pushgateway \
  prom/pushgateway
```

Añade lo siguiente a tu `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: pushgateway
    honor_labels: true
    static_configs:
      - targets: ['localhost:9091']
```

Asegúrate de que el puerto 9091 sea accesible desde Cloud Run: ábrelo en las reglas del firewall de la VM, o usa un conector de VPC para mantener el tráfico en la red privada.

---

## Contrato de datos

### NewsArticle

| Campo | Tipo | Obligatorio | Descripción |
|-------|------|-------------|-------------|
| `title` | `str` | ✓ | Titular del artículo |
| `url` | `str` | ✓ | URL canónica del artículo |
| `time_published` | `str` | ✓ | Formato Alpha Vantage: `YYYYMMDDTHHmmss` |
| `summary` | `str` | ✓ | Resumen del artículo |
| `source` | `str` | ✓ | Nombre del medio |
| `category_within_source` | `str` | — | Sección o categoría del medio |
| `overall_sentiment_score` | `float` | — | Puntuación continua (ver escala más abajo) |
| `overall_sentiment_label` | `str` | — | Etiqueta categórica |
| `ingest_timestamp` | `float` | — | Epoch Unix en el momento de la ingesta |
| `freshness_seconds` | `float` | — | Antigüedad del artículo en el momento de la ingesta |

**Escala de puntuación de sentimiento:**
`<= -0.35` Bearish · `(-0.35, -0.15]` Somewhat-Bearish · `(-0.15, 0.15)` Neutral · `[0.15, 0.35)` Somewhat-Bullish · `>= 0.35` Bullish

---

## Métricas de observabilidad

Todas las métricas se envían al Pushgateway como gauges al final de cada ejecución.

| Métrica | Descripción |
|---------|-------------|
| `news_collector_articles_fetched` | Artículos brutos en la ventana de lookback |
| `news_collector_articles_valid` | Artículos que superan la validación del contrato |
| `news_collector_articles_invalid` | Artículos que no superan la validación del contrato |
| `news_collector_articles_published` | Artículos enviados correctamente a Pub/Sub |
| `news_collector_articles_dlq` | Artículos enrutados a la DLQ |
| `news_collector_schema_compliance_rate` | `valid / fetched` |
| `news_collector_dlq_rate` | `dlq / fetched` |
| `news_collector_freshness_seconds_mean` | Antigüedad media de los artículos en la ingesta |
| `news_collector_api_latency_seconds` | Duración de la llamada HTTP a Alpha Vantage |
| `news_collector_violation_detection_ms_mean` | Tiempo medio para detectar una violación de contrato |
| `news_collector_run_duration_seconds` | Tiempo total de ejecución del job |
| `news_collector_sentiment_<label>_count` | Recuento de artículos por etiqueta de sentimiento (un gauge por etiqueta) |

---

## Referencia de variables de entorno

| Variable | Valor por defecto | Descripción |
|----------|-------------------|-------------|
| `ALPHAVANTAGE_API_KEY` | *(obligatorio)* | Clave de la API de Alpha Vantage |
| `GCP_PROJECT_ID` | `your-gcp-project-id` | Proyecto GCP para Pub/Sub |
| `NEWS_TOPIC_ID` | `financial_news` | Topic de Pub/Sub para artículos de noticias |
| `NEWS_LOOKBACK_MINUTES` | `5` | Ventana temporal hacia atrás para buscar artículos |
| `PUSHGATEWAY_HOST` | `your-vm-public-ip` | Hostname o IP del Pushgateway |
| `PUSHGATEWAY_PORT` | `9091` | Puerto del Pushgateway |
| `PUSHGATEWAY_JOB_NAME` | `news_collector` | Etiqueta de job en el Pushgateway |
