# config.py
# ---------------------------------------------------------------------------
# All environment-level constants for the Alpha Vantage news collector.
# Override any of these via environment variables or the .env file.
# ---------------------------------------------------------------------------

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Alpha Vantage ─────────────────────────────────────────────────────────────
ALPHAVANTAGE_API_KEY: str = os.getenv("ALPHAVANTAGE_API_KEY", "")
ALPHAVANTAGE_BASE_URL: str = "https://www.alphavantage.co/query"

# How far back to look for news (minutes). Should match the Cloud Run Job
# trigger interval so there are no gaps or significant overlaps.
NEWS_LOOKBACK_MINUTES: int = int(os.getenv("NEWS_LOOKBACK_MINUTES", "5"))

# GCP Pub/Sub
GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "")
NEWS_TOPIC_ID: str = os.getenv("NEWS_TOPIC_ID", "financial_news")

# ── Prometheus Pushgateway ────────────────────────────────────────────────────
# The pushgateway runs on the same GCP VM as the other collectors.
PUSHGATEWAY_HOST: str = os.getenv("PUSHGATEWAY_HOST", "your-vm-public-ip")
PUSHGATEWAY_PORT: int = int(os.getenv("PUSHGATEWAY_PORT", "9091"))
PUSHGATEWAY_JOB_NAME: str = os.getenv("PUSHGATEWAY_JOB_NAME", "news_collector")
