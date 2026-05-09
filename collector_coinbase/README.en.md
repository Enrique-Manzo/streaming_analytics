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
└── .env               # API credentials — DO NOT commit to version control
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

## Deploy to a GCP Compute Engine VM with PM2

The collector runs directly on a Compute Engine VM. PM2 manages the process and ensures it restarts automatically if it crashes or the VM reboots.

### Prerequisites
- Compute Engine VM with Python 3 and Node.js installed
- PM2 installed globally (`npm install -g pm2`)
- Git installed on the VM
- Credentials configured in the `.env` file inside the project

### 1. Enable required GCP APIs (once)

```bash
gcloud services enable \
  compute.googleapis.com \
  --project=$PROJECT_ID
```

### 2. Clone the repository on the VM

SSH into the VM and clone the project from GitHub:

```bash
gcloud compute ssh <your-vm-name> --zone=<your-zone>

# On the VM:
git clone https://github.com/Enrique-Manzo/streaming_analytics.git
```

### 3. Install dependencies and configure credentials

```bash
pip install -r requirements.txt

# Create the .env file with your credentials
cp .env.example .env   # or create it directly
nano .env
```

### 4. Start the process with PM2

```bash
pm2 start main.py --name coinbase-collector --interpreter python3
```

### 5. Configure PM2 to start on system boot

This ensures the process restarts automatically if the VM reboots:

```bash
pm2 startup
# Run the command that PM2 outputs
pm2 save
```

### 6. Useful PM2 commands

```bash
pm2 status                         # Status of all processes
pm2 logs coinbase-collector        # Stream logs in real time
pm2 restart coinbase-collector     # Restart the process
pm2 stop coinbase-collector        # Stop the process
pm2 delete coinbase-collector      # Remove the process from PM2
```

### 7. Updating to a new version

To deploy changes, pull the latest code and restart the process:

```bash
cd coinbase-collector
git pull origin main
pip install -r requirements.txt   # Only if dependencies changed
pm2 restart coinbase-collector
```

---

## Environment variables reference

| Variable                        | Default                               | Description                                      |
|---------------------------------|---------------------------------------|--------------------------------------------------|
| `COINBASE_WS_URL`               | `wss://ws-feed.exchange.coinbase.com` | Coinbase WebSocket endpoint                      |
| `THROUGHPUT_WINDOW_SECONDS`     | `60`                                  | Rolling window for throughput calculation        |
| `METRICS_RESET_INTERVAL_SECONDS`| `3600`                                | How often windowed metrics reset (seconds)       |
| `DEDUP_MAXLEN`                  | `50000`                               | Max trade/sequence IDs held for deduplication    |
| `HOST`                          | `0.0.0.0`                             | FastAPI bind host                                |
| `PORT`                          | `8000`                                | FastAPI bind port                                |
| `WS_RECONNECT_DELAY`            | `5`                                   | Seconds to wait before reconnecting after drop   |

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
On PM2 process restart they reset to 0 — if you need persistence
across restarts, push them to Cloud Monitoring or Firestore on shutdown.
