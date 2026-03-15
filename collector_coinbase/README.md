# Coinbase Trade Collector — Deployment Guide

## Project structure

```
collector/
├── main.py            # FastAPI app, lifespan, entry point
├── contract.py        # TradeEvent Pydantic data contract
├── observability.py   # ObservabilityState + Prometheus renderer
├── ws_collector.py    # WebSocket loop + message processing
├── config.py          # Constants (override via env vars)
├── requirements.txt
└── Dockerfile
```

---

## Run locally

```bash
pip install -r requirements.txt
python main.py
```

Endpoints:
- `GET http://localhost:8000/metrics`      — Prometheus text format
- `GET http://localhost:8000/metrics/json` — JSON snapshot
- `GET http://localhost:8000/health`       — Health check

---

## Run with Docker locally

```bash
# Build
docker build -t coinbase-collector .

# Run
docker run -p 8000:8000 coinbase-collector

# Override config via env vars
docker run -p 8000:8000 \
  -e METRICS_RESET_INTERVAL_SECONDS=1800 \
  -e DEDUP_MAXLEN=100000 \
  coinbase-collector
```

---

## Deploy to GCP Cloud Run Jobs

### Prerequisites
- GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth login`)
- Artifact Registry repository created

### 1. Set your variables

```bash
export PROJECT_ID=your-gcp-project-id
export REGION=us-central1
export REPO=coinbase-collector          # Artifact Registry repo name
export IMAGE=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/coinbase-collector:latest
```

### 2. Enable required GCP APIs

```bash
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  --project=$PROJECT_ID
```

### 3. Create Artifact Registry repository (once)

```bash
gcloud artifacts repositories create $REPO \
  --repository-format=docker \
  --location=$REGION \
  --project=$PROJECT_ID
```

### 4. Build and push the image

```bash
# Authenticate Docker to Artifact Registry
gcloud auth configure-docker $REGION-docker.pkg.dev

# Build and push
docker build -t $IMAGE .
docker push $IMAGE
```

Or use Cloud Build to build remotely (no local Docker required):

```bash
gcloud builds submit --tag $IMAGE --project=$PROJECT_ID
```

### 5. Create the Cloud Run Job

```bash
gcloud run jobs create coinbase-collector \
  --image=$IMAGE \
  --region=$REGION \
  --project=$PROJECT_ID \
  --task-timeout=0 \
  --max-retries=3 \
  --memory=512Mi \
  --cpu=1 \
  --set-env-vars="METRICS_RESET_INTERVAL_SECONDS=3600,DEDUP_MAXLEN=50000"
```

> `--task-timeout=0` means no timeout — the job runs indefinitely,
> which is what you want for a continuous WebSocket collector.

### 6. Execute the job

```bash
gcloud run jobs execute coinbase-collector \
  --region=$REGION \
  --project=$PROJECT_ID
```

### 7. View logs

```bash
gcloud logging read \
  "resource.type=cloud_run_job AND resource.labels.job_name=coinbase-collector" \
  --project=$PROJECT_ID \
  --limit=50 \
  --format="value(textPayload)"
```

---

## Environment variables reference

| Variable                        | Default                                    | Description                                      |
|---------------------------------|--------------------------------------------|--------------------------------------------------|
| `COINBASE_WS_URL`               | `wss://ws-feed.exchange.coinbase.com`      | Coinbase WebSocket endpoint                      |
| `THROUGHPUT_WINDOW_SECONDS`     | `60`                                       | Rolling window for throughput calculation        |
| `METRICS_RESET_INTERVAL_SECONDS`| `3600`                                     | How often windowed metrics reset (seconds)       |
| `DEDUP_MAXLEN`                  | `50000`                                    | Max trade/sequence IDs held for deduplication    |
| `HOST`                          | `0.0.0.0`                                  | FastAPI bind host                                |
| `PORT`                          | `8000`                                     | FastAPI bind port (Cloud Run injects this)       |
| `WS_RECONNECT_DELAY`            | `5`                                        | Seconds to wait before reconnecting after drop   |

---

## Metrics design notes

**Windowed metrics** (reset every hour by default):
`schema_compliance_rate`, `duplicate_rate`, `dlq_rate`,
`throughput`, `freshness_at_ingestion`, `contract_violation_detection_time`

These give you "how is the collector doing *right now*" semantics,
which is what alerting rules should be based on.

**Lifetime counters** (never reset, for audit):
`total_received`, `total_valid`, `total_invalid`,
`total_duplicates`, `total_dlq`

These monotonically increase for the lifetime of the process.
On Cloud Run Job restart they reset to 0 — if you need persistence
across restarts, push them to Cloud Monitoring or Firestore on shutdown.
