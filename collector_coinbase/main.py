# main.py
# ---------------------------------------------------------------------------
# FastAPI application entry point.
# Starts the WebSocket collector and periodic metric reset as background tasks.
# Exposes /metrics (Prometheus), /metrics/json, and /health endpoints.
# ---------------------------------------------------------------------------

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from config import HOST, PORT, METRICS_RESET_INTERVAL_SECONDS
from observability import ObservabilityState, render_prometheus_metrics
from ws_collector import coinbase_ws_collector

# Shared singleton — passed explicitly to avoid hidden global state
obs = ObservabilityState()


# ---------------------------------------------------------------------------
# Periodic metric window reset
# ---------------------------------------------------------------------------

async def metrics_reset_loop(obs: ObservabilityState) -> None:
    """Reset windowed metrics every METRICS_RESET_INTERVAL_SECONDS (default: 1h)."""
    while True:
        await asyncio.sleep(METRICS_RESET_INTERVAL_SECONDS)
        obs.reset_window()


# ---------------------------------------------------------------------------
# Lifespan: replaces deprecated @app.on_event("startup")
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background tasks
    collector_task = asyncio.create_task(coinbase_ws_collector(obs))
    reset_task = asyncio.create_task(metrics_reset_loop(obs))
    print(
        f"[MAIN] Collector started. "
        f"Metrics window resets every {METRICS_RESET_INTERVAL_SECONDS}s."
    )
    yield
    # Graceful shutdown
    collector_task.cancel()
    reset_task.cancel()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Coinbase Trade Collector",
    description=(
        "WebSocket trade collector for the 10 most popular Coinbase pairs. "
        "Exposes Prometheus-compatible observability metrics."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/metrics", response_class=PlainTextResponse, tags=["observability"])
async def metrics():
    """
    Prometheus-compatible metrics endpoint.

    Add to prometheus.yml:
        scrape_configs:
          - job_name: coinbase_collector
            static_configs:
              - targets: ['<host>:8000']
    """
    return render_prometheus_metrics(obs)


@app.get("/metrics/json", tags=["observability"])
async def metrics_json():
    """Human-readable JSON snapshot of all observability metrics."""
    return obs.snapshot()


@app.get("/health", tags=["observability"])
async def health():
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=HOST,
        port=PORT,
        log_level="info",
        reload=False,
    )
