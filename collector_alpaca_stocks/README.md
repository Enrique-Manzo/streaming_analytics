# Alpaca Stock Collector — Deployment Guide

## Project structure

```
alpaca-collector/
├── main.py            # FastAPI app, lifespan, entry point
├── contract.py        # TradeEvent + QuoteEvent Pydantic data contracts
├── observability.py   # ObservabilityState + Prometheus renderer (per-stream)
├── ws_collector.py    # WebSocket loop, auth, routing, message processing
├── config.py          # Constants (override via env vars or .env)
├── requirements.txt
├── Dockerfile
└── .env               # API credentials — DO NOT commit to version control
```

---

## Quick start (local)

### 1. Set your credentials

Edit `.env`:

```
ALPACA_API_KEY=your_alpaca_api_key_here
ALPACA_API_SECRET=your_alpaca_api_secret_here
GCP_PROJECT_ID=your-gcp-project-id
```

### 2. Install and run

```bash
pip install -r requirements.txt
python main.py
```

Endpoints:
| URL | Description |
|-----|-------------|
| `GET http://localhost:8000/metrics` | Prometheus text format (both streams) |
| `GET http://localhost:8000/metrics/json` | Full JSON snapshot |
| `GET http://localhost:8000/metrics/trades` | Trades-only JSON snapshot |
| `GET http://localhost:8000/metrics/quotes` | Quotes-only JSON snapshot |
| `GET http://localhost:8000/health` | Health check |

---

## Run with Docker locally

```bash
# Build
docker build -t alpaca-collector .

# Run (inject credentials at runtime — never bake them into the image)
docker run -p 8000:8000 \
  -e ALPACA_API_KEY=your_key \
  -e ALPACA_API_SECRET=your_secret \
  -e GCP_PROJECT_ID=your-project \
  alpaca-collector
```

---

## Deploy to a GCP Compute Engine VM

### Prerequisites
- GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth login`)
- Artifact Registry repository created
- Two Pub/Sub topics created: `stock_trades` and `stock_quotes`

### 1. Set your variables

```bash
export PROJECT_ID=your-gcp-project-id
export REGION=us-central1
export REPO=alpaca-collector
export IMAGE=$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/alpaca-collector:latest
```

### 2. Enable required GCP APIs

```bash
gcloud services enable \
  compute.googleapis.com \
  artifactregistry.googleapis.com \
  pubsub.googleapis.com \
  secretmanager.googleapis.com \
  --project=$PROJECT_ID
```

### 3. Create Pub/Sub topics (once)

```bash
gcloud pubsub topics create stock_trades --project=$PROJECT_ID
gcloud pubsub topics create stock_quotes --project=$PROJECT_ID
```

### 4. Store credentials in Secret Manager (recommended over env vars)

```bash
echo -n "your_alpaca_key"    | gcloud secrets create ALPACA_API_KEY    --data-file=- --project=$PROJECT_ID
echo -n "your_alpaca_secret" | gcloud secrets create ALPACA_API_SECRET --data-file=- --project=$PROJECT_ID
```

### 5. Build and push the image

```bash
gcloud auth configure-docker $REGION-docker.pkg.dev

docker build -t $IMAGE .
docker push $IMAGE
```

Or use Cloud Build (no local Docker required):

```bash
gcloud builds submit --tag $IMAGE --project=$PROJECT_ID
```

### 6. Create and start the VM

```bash
gcloud compute instances create-with-container alpaca-collector-vm \
  --project=$PROJECT_ID \
  --zone=$REGION-a \
  --machine-type=e2-small \
  --container-image=$IMAGE \
  --container-env="GCP_PROJECT_ID=$PROJECT_ID" \
  --container-env="ALPACA_API_KEY=$(gcloud secrets versions access latest --secret=ALPACA_API_KEY --project=$PROJECT_ID)" \
  --container-env="ALPACA_API_SECRET=$(gcloud secrets versions access latest --secret=ALPACA_API_SECRET --project=$PROJECT_ID)" \
  --scopes=cloud-platform
```

### 7. View logs

```bash
gcloud compute ssh alpaca-collector-vm --zone=$REGION-a -- "docker logs \$(docker ps -q)"
```

---

## Data contracts

### TradeEvent (`T == "t"`)

| Field | Type | Description |
|-------|------|-------------|
| `T` | `str` | Message type, always `"t"` |
| `S` | `str` | Ticker symbol (e.g. `"AAPL"`) |
| `i` | `int` | Trade ID (deduplication key) |
| `x` | `str` | Exchange code |
| `p` | `float` | Trade price |
| `s` | `int` | Trade size (shares) |
| `t` | `str` | ISO-8601 exchange timestamp |
| `c` | `list[str]` | Trade condition codes (optional) |
| `z` | `str` | Tape identifier (optional) |
| `ingest_timestamp` | `float` | Unix epoch when collector received the message |
| `freshness_seconds` | `float` | Exchange-to-ingest lag |

### QuoteEvent (`T == "q"`)

| Field | Type | Description |
|-------|------|-------------|
| `T` | `str` | Message type, always `"q"` |
| `S` | `str` | Ticker symbol |
| `bx` | `str` | Bid exchange code |
| `bp` | `float` | Bid price |
| `bs` | `int` | Bid size (shares) |
| `ax` | `str` | Ask exchange code |
| `ap` | `float` | Ask price |
| `as` | `int` | Ask size (shares) |
| `t` | `str` | ISO-8601 exchange timestamp |
| `c` | `list[str]` | Quote condition codes (optional) |
| `z` | `str` | Tape identifier (optional) |
| `ingest_timestamp` | `float` | Unix epoch when collector received the message |
| `freshness_seconds` | `float` | Exchange-to-ingest lag |
| `spread` | `float` | Derived: `ap - bp` |

---

## Pub/Sub topics

| Topic | Source | Message format |
|-------|--------|----------------|
| `stock_trades` | `TradeEvent.model_dump()` serialised to JSON | One trade per message |
| `stock_quotes` | `QuoteEvent.model_dump(by_alias=True)` serialised to JSON | One quote per message |

---

## Observability metrics

Prometheus metrics are split by stream (`trades` / `quotes`).

**Windowed metrics** (reset every hour by default):

| Metric | Description |
|--------|-------------|
| `alpaca_collector_trades_throughput_msgs_per_sec` | Valid trade messages/sec (rolling 60s window) |
| `alpaca_collector_trades_schema_compliance_rate` | Fraction of trades passing contract |
| `alpaca_collector_trades_duplicate_rate` | Fraction of trades flagged as duplicate |
| `alpaca_collector_trades_dlq_rate` | Fraction of trades routed to DLQ |
| `alpaca_collector_trades_freshness_seconds_mean` | Mean exchange→ingest lag for trades |
| `alpaca_collector_trades_violation_detection_time_ms` | Mean ms to detect a trade contract violation |
| `alpaca_collector_quotes_throughput_msgs_per_sec` | Valid quote messages/sec |
| `alpaca_collector_quotes_schema_compliance_rate` | Fraction of quotes passing contract |
| `alpaca_collector_quotes_duplicate_rate` | Fraction of quotes flagged as duplicate |
| `alpaca_collector_quotes_dlq_rate` | Fraction of quotes routed to DLQ |
| `alpaca_collector_quotes_freshness_seconds_mean` | Mean exchange→ingest lag for quotes |
| `alpaca_collector_quotes_violation_detection_time_ms` | Mean ms to detect a quote contract violation |
| `alpaca_collector_quotes_mean_bid_ask_spread` | Mean bid-ask spread in dollars (quotes only) |

**Lifetime counters** (never reset):

`total_received`, `total_valid`, `total_invalid`, `total_duplicates`, `total_dlq` — one set per stream.

---

## Environment variables reference

| Variable | Default | Description |
|----------|---------|-------------|
| `ALPACA_API_KEY` | *(required)* | Alpaca API key |
| `ALPACA_API_SECRET` | *(required)* | Alpaca API secret |
| `ALPACA_WS_URL` | `wss://stream.data.alpaca.markets/v2/iex` | Alpaca IEX WebSocket URL |
| `GCP_PROJECT_ID` | `your-gcp-project-id` | GCP project for Pub/Sub |
| `TRADES_TOPIC_ID` | `stock_trades` | Pub/Sub topic for trade events |
| `QUOTES_TOPIC_ID` | `stock_quotes` | Pub/Sub topic for quote events |
| `THROUGHPUT_WINDOW_SECONDS` | `60` | Rolling window for throughput calculation |
| `METRICS_RESET_INTERVAL_SECONDS` | `3600` | How often windowed metrics reset (seconds) |
| `DEDUP_MAXLEN` | `50000` | Max message keys held for deduplication |
| `HOST` | `0.0.0.0` | FastAPI bind host |
| `PORT` | `8000` | FastAPI bind port |
| `WS_RECONNECT_DELAY` | `5` | Seconds to wait before reconnecting after a drop |

---

## Symbols tracked

Top 20 S&P 500 stocks by market capitalisation:

`AAPL MSFT NVDA AMZN GOOGL META TSLA BRK.B AVGO JPM LLY UNH V XOM MA COST HD PG JNJ NFLX`
