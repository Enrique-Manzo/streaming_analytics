# main.py
# ---------------------------------------------------------------------------
# FastAPI application entry point.
# Starts the Alpaca WebSocket collector and periodic metric reset as
# background tasks. Exposes /metrics (Prometheus), /metrics/json, and
# /health endpoints.
# ---------------------------------------------------------------------------

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from config import HOST, METRICS_RESET_INTERVAL_SECONDS, PORT
from observability import ObservabilityState, render_prometheus_metrics
from ws_collector import alpaca_ws_collector

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
    collector_task = asyncio.create_task(alpaca_ws_collector(obs))
    reset_task = asyncio.create_task(metrics_reset_loop(obs))
    print(
        f"[MAIN] Alpaca collector started. "
        f"Metrics window resets every {METRICS_RESET_INTERVAL_SECONDS}s."
    )
    yield
    collector_task.cancel()
    reset_task.cancel()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Alpaca Stock Collector",
    description=(
        "WebSocket trade and quote collector for the top 20 S&P 500 stocks "
        "via Alpaca IEX feed. Publishes to GCP Pub/Sub and exposes "
        "Prometheus-compatible observability metrics."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/metrics", response_class=PlainTextResponse, tags=["observability"])
async def metrics():
    """
    Prometheus-compatible metrics endpoint.
    """
    return render_prometheus_metrics(obs)


@app.get("/metrics/json", tags=["observability"])
async def metrics_json():
    """Human-readable JSON snapshot of all observability metrics, split by stream."""
    return obs.snapshot()


@app.get("/metrics/trades", tags=["observability"])
async def metrics_trades():
    """JSON metrics snapshot for the trades stream only."""
    return obs.trades.snapshot()


@app.get("/metrics/quotes", tags=["observability"])
async def metrics_quotes():
    """JSON metrics snapshot for the quotes stream only."""
    return obs.quotes.snapshot()


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
