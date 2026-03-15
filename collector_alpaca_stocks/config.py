# config.py
# ---------------------------------------------------------------------------
# All environment-level constants for the Alpaca stock collector.
# Override any of these via environment variables or the .env file.
# ---------------------------------------------------------------------------

import os

from dotenv import load_dotenv

load_dotenv()

# ── Top 20 S&P 500 stocks by market cap ──────────────────────────────────────
SYMBOLS: list[str] = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
    "META", "TSLA", "BRK.B", "AVGO", "JPM",
    "LLY", "UNH", "V", "XOM", "MA",
]

# ── Alpaca credentials ────────────────────────────────────────────────────────
ALPACA_API_KEY: str = os.getenv("ALPACA_API_KEY", "")
ALPACA_API_SECRET: str = os.getenv("ALPACA_API_SECRET", "")

# ── WebSocket ─────────────────────────────────────────────────────────────────
ALPACA_WS_URL: str = os.getenv(
    "ALPACA_WS_URL",
    "wss://stream.data.alpaca.markets/v2/iex",
)

# Rolling window for throughput calculation (seconds)
THROUGHPUT_WINDOW_SECONDS: int = int(os.getenv("THROUGHPUT_WINDOW_SECONDS", "60"))

# How often to reset windowed metrics (seconds). Default: 1 hour.
METRICS_RESET_INTERVAL_SECONDS: int = int(os.getenv("METRICS_RESET_INTERVAL_SECONDS", "3600"))

# Max message IDs to keep in memory for deduplication
DEDUP_MAXLEN: int = int(os.getenv("DEDUP_MAXLEN", "50000"))

# FastAPI server
HOST: str = os.getenv("HOST", "0.0.0.0")
PORT: int = int(os.getenv("PORT", ""))

# WebSocket reconnection delay (seconds)
WS_RECONNECT_DELAY: int = int(os.getenv("WS_RECONNECT_DELAY", "5"))

# GCP Pub/Sub
GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "")
TRADES_TOPIC_ID: str = os.getenv("TRADES_TOPIC_ID", "stock_trades")
QUOTES_TOPIC_ID: str = os.getenv("QUOTES_TOPIC_ID", "stock_quotes")
