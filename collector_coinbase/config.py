# config.py
# ---------------------------------------------------------------------------
# All environment-level constants for the Coinbase trade collector.
# Override any of these via environment variables in Cloud Run Jobs.
# ---------------------------------------------------------------------------

import os

PRODUCTS: list[str] = [
    "BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD",
    "XRP-USD", "DOGE-USD", "AVAX-USD", "LINK-USD",
    "DOT-USD", "LTC-USD"
]

COINBASE_WS_URL: str = os.getenv(
    "COINBASE_WS_URL",
    "wss://ws-feed.exchange.coinbase.com",
)

# Rolling window for throughput calculation (seconds)
THROUGHPUT_WINDOW_SECONDS: int = int(os.getenv("THROUGHPUT_WINDOW_SECONDS", "60"))

# How often to reset windowed metrics (seconds). Default: 1 hour.
METRICS_RESET_INTERVAL_SECONDS: int = int(os.getenv("METRICS_RESET_INTERVAL_SECONDS", "3600"))

# Max trade/sequence IDs to keep in memory for deduplication
DEDUP_MAXLEN: int = int(os.getenv("DEDUP_MAXLEN", "50000"))

# FastAPI server
HOST: str = os.getenv("HOST", "0.0.0.0")
PORT: int = int(os.getenv("PORT", "8000"))

# WebSocket reconnection delay (seconds)
WS_RECONNECT_DELAY: int = int(os.getenv("WS_RECONNECT_DELAY", "5"))
