# Alpha Vantage News Collector — Deployment Guide

## Project structure

```
news-collector/
├── main.py             # Cloud Run Job entry point (no server)
├── contract.py         # NewsArticle Pydantic data contract
├── observability.py    # ObservabilityState + Prometheus Pushgateway push
├── news_collector.py   # API call, filtering, validation, Pub/Sub publish
├── config.py           # Constants (override via env vars or .env)
├── requirements.txt
├── Dockerfile
└── .env                # API credentials — DO NOT commit to version control
```

---

## Architecture overview

```
Cloud Scheduler (every 5 min)
        │
        ▼
Cloud Run Job  ──── Alpha Vantage API
        │                (NEWS_SENTIMENT)
        │
        ├──► Pub/Sub topic: financial_news
        │
        └──► Prometheus Pushgateway  ◄──── Prometheus (scrapes on GCP VM)
               (GCP VM, port 9091)
```

This collector is **stateless and short-lived**. It runs, does its work, pushes metrics, and exits. Prometheus scrapes the Pushgateway independently.

---

## Quick start (local)

### 1. Set your credentials

Edit `.env`:

```
ALPHAVANTAGE_API_KEY=your_key_here
GCP_PROJECT_ID=your-gcp-project-id
PUSHGATEWAY_HOST=your-vm-public-ip
```

### 2. Install and run

```bash
pip install -r requirements.txt
python main.py
```

---

## Deploy to GCP Cloud Run Jobs

### Prerequisites
- GCP project with billing enabled
- `gcloud` CLI authenticated
- Artifact Registry repository created
- Pub/Sub topic `financial_news` created
- Prometheus Pushgateway running on your GCP VM (port 9091 by default)

### 1. Set variables

```bash
export PROJECT_ID=your-gcp-project-id
export REGION=us-central1
export REPO=news-collector
export IMAGE=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/news-collector:latest
```

### 2. Enable APIs

```bash
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  pubsub.googleapis.com \
  cloudscheduler.googleapis.com \
  --project=$PROJECT_ID
```

### 3. Create Pub/Sub topic (once)

```bash
gcloud pubsub topics create financial_news --project=$PROJECT_ID
```

### 4. Build and push

```bash
gcloud auth configure-docker $REGION-docker.pkg.dev
docker build -t $IMAGE .
docker push $IMAGE
```

### 5. Create the Cloud Run Job

```bash
gcloud run jobs create news-collector \
  --image=$IMAGE \
  --region=$REGION \
  --project=$PROJECT_ID \
  --task-timeout=120 \
  --max-retries=2 \
  --memory=256Mi \
  --cpu=1 \
  --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,PUSHGATEWAY_HOST=your-vm-ip,NEWS_LOOKBACK_MINUTES=5"
```

Store sensitive values in Secret Manager and pass them via `--set-secrets`:

```bash
gcloud run jobs update news-collector \
  --region=$REGION \
  --set-secrets="ALPHAVANTAGE_API_KEY=ALPHAVANTAGE_API_KEY:latest"
```

### 6. Schedule with Cloud Scheduler (every 5 minutes)

```bash
gcloud scheduler jobs create http news-collector-trigger \
  --location=$REGION \
  --schedule="*/5 * * * *" \
  --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/news-collector:run" \
  --http-method=POST \
  --oauth-service-account-email=YOUR_SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com
```

### 7. View logs

```bash
gcloud logging read \
  "resource.type=cloud_run_job AND resource.labels.job_name=news-collector" \
  --project=$PROJECT_ID \
  --limit=50 \
  --format="value(textPayload)"
```

---

## Pushgateway setup on the GCP VM

If not already running, start the Pushgateway on your shared VM:

```bash
# Using Docker
docker run -d -p 9091:9091 --name pushgateway prom/pushgateway

# Or with a persistent data volume
docker run -d -p 9091:9091 --name pushgateway \
  -v pushgateway-data:/pushgateway \
  prom/pushgateway
```

Add to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: pushgateway
    honor_labels: true
    static_configs:
      - targets: ['localhost:9091']
```

Make sure port 9091 is reachable from Cloud Run. Either open it in the VM's firewall rules, or use a VPC connector to keep traffic private.

---

## Data contract

### NewsArticle

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `title` | `str` | ✓ | Article headline |
| `url` | `str` | ✓ | Canonical article URL |
| `time_published` | `str` | ✓ | Alpha Vantage format: `YYYYMMDDTHHmmss` |
| `summary` | `str` | ✓ | Article summary |
| `source` | `str` | ✓ | Publisher name |
| `category_within_source` | `str` | — | Publisher section/category |
| `overall_sentiment_score` | `float` | — | Continuous score (see scale below) |
| `overall_sentiment_label` | `str` | — | Categorical label |
| `ingest_timestamp` | `float` | — | Unix epoch at ingest |
| `freshness_seconds` | `float` | — | Age of article at ingest |

**Sentiment score scale:**
`<= -0.35` Bearish · `(-0.35, -0.15]` Somewhat-Bearish · `(-0.15, 0.15)` Neutral · `[0.15, 0.35)` Somewhat_Bullish · `>= 0.35` Bullish

---

## Observability metrics

All metrics are pushed to the Pushgateway as gauges at the end of each run.

| Metric | Description |
|--------|-------------|
| `news_collector_articles_fetched` | Raw articles in the lookback window |
| `news_collector_articles_valid` | Articles passing contract validation |
| `news_collector_articles_invalid` | Articles failing contract validation |
| `news_collector_articles_published` | Articles successfully sent to Pub/Sub |
| `news_collector_articles_dlq` | Articles routed to DLQ |
| `news_collector_schema_compliance_rate` | `valid / fetched` |
| `news_collector_dlq_rate` | `dlq / fetched` |
| `news_collector_freshness_seconds_mean` | Mean article age at ingest |
| `news_collector_api_latency_seconds` | Alpha Vantage HTTP call duration |
| `news_collector_violation_detection_ms_mean` | Mean time to detect a violation |
| `news_collector_run_duration_seconds` | Total job wall-clock time |
| `news_collector_sentiment_<label>_count` | Per-label article counts (one gauge per label) |

---

## Environment variables reference

| Variable | Default | Description |
|----------|---------|-------------|
| `ALPHAVANTAGE_API_KEY` | *(required)* | Alpha Vantage API key |
| `GCP_PROJECT_ID` | `your-gcp-project-id` | GCP project for Pub/Sub |
| `NEWS_TOPIC_ID` | `financial_news` | Pub/Sub topic for news articles |
| `NEWS_LOOKBACK_MINUTES` | `5` | How far back to look for articles |
| `PUSHGATEWAY_HOST` | `your-vm-public-ip` | Pushgateway hostname or IP |
| `PUSHGATEWAY_PORT` | `9091` | Pushgateway port |
| `PUSHGATEWAY_JOB_NAME` | `news_collector` | Job label in Pushgateway |
