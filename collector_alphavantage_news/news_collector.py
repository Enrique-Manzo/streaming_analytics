# news_collector.py
# ---------------------------------------------------------------------------
# Alpha Vantage NEWS_SENTIMENT fetcher.
#
# Responsibilities:
#   1. Call the Alpha Vantage NEWS_SENTIMENT endpoint with time_from set to
#      NOW - NEWS_LOOKBACK_MINUTES, so the API returns only recent articles
#   2. Validate each article against the NewsArticle data contract
#   3. Enrich with ingest_timestamp and freshness_seconds
#   4. Publish valid articles to the GCP Pub/Sub "financial_news" topic
#   5. Route invalid articles to the in-memory DLQ and record observability
# ---------------------------------------------------------------------------

import json
import time
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import pubsub_v1
from pydantic import ValidationError

from config import (
    ALPHAVANTAGE_API_KEY,
    ALPHAVANTAGE_BASE_URL,
    GCP_PROJECT_ID,
    NEWS_LOOKBACK_MINUTES,
    NEWS_TOPIC_ID,
)
from contract import NewsArticle
from observability import ObservabilityState


# ── Pub/Sub setup ─────────────────────────────────────────────────────────────
publisher = pubsub_v1.PublisherClient()
news_topic_path = publisher.topic_path(GCP_PROJECT_ID, NEWS_TOPIC_ID)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _av_timestamp_to_utc(ts: str) -> datetime:
    """Parse Alpha Vantage compact timestamp (YYYYMMDDTHHmmss) to UTC datetime."""
    return datetime.strptime(ts, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def fetch_news(obs: ObservabilityState) -> list[dict]:
    """
    Call the Alpha Vantage NEWS_SENTIMENT endpoint with time_from set to
    NOW - NEWS_LOOKBACK_MINUTES. The API returns only articles published
    after that timestamp, so no client-side filtering is needed.

    The time_from format required by the API is YYYYMMDDTHHmm (no seconds).

    Records API latency in obs. Returns an empty list on any error.
    """
    if not ALPHAVANTAGE_API_KEY:
        raise RuntimeError(
            "ALPHAVANTAGE_API_KEY must be set (via .env or environment variable)."
        )

    time_from = (
        datetime.now(timezone.utc) - timedelta(minutes=NEWS_LOOKBACK_MINUTES)
    ).strftime("%Y%m%dT%H%M")

    url = (
        f"{ALPHAVANTAGE_BASE_URL}"
        f"?function=NEWS_SENTIMENT"
        f"&time_from={time_from}"
        f"&apikey={ALPHAVANTAGE_API_KEY}"
    )

    print(f"[FETCH] Calling Alpha Vantage NEWS_SENTIMENT (time_from={time_from}) …")
    t_start = time.perf_counter()

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        obs.record_api_latency(time.perf_counter() - t_start)
        print(f"[FETCH] ERROR — HTTP request failed: {exc}")
        return []

    obs.record_api_latency(time.perf_counter() - t_start)

    try:
        data = response.json()
    except ValueError as exc:
        print(f"[FETCH] ERROR — JSON parse failed: {exc}")
        return []

    if "feed" not in data:
        # Alpha Vantage returns {"Information": "..."} when rate-limited
        info = data.get("Information") or data.get("Note") or str(data)
        print(f"[FETCH] WARNING — unexpected response (no 'feed' key): {info}")
        return []

    articles = data["feed"]
    print(f"[FETCH] Received {len(articles)} articles from API.")
    return articles


# ---------------------------------------------------------------------------
# Per-article processing
# ---------------------------------------------------------------------------

def process_article(raw: dict, obs: ObservabilityState) -> None:
    """Validate, enrich, and publish one article."""
    ingest_ts = time.time()
    t_start = time.perf_counter()

    try:
        article = NewsArticle(**raw)
    except ValidationError as exc:
        detection_ms = (time.perf_counter() - t_start) * 1000
        obs.record_invalid(raw, str(exc), detection_ms)
        print(
            f"[DLQ] Contract violation ({detection_ms:.2f} ms): "
            f"{exc.error_count()} error(s) | title={str(raw.get('title', ''))[:60]}"
        )
        return

    # Freshness enrichment
    try:
        published_utc = _av_timestamp_to_utc(article.time_published)
        freshness = ingest_ts - published_utc.timestamp()
    except Exception:
        freshness = 0.0

    article.ingest_timestamp = ingest_ts
    article.freshness_seconds = freshness

    obs.record_valid(freshness, article.overall_sentiment_label)

    print(
        f"[ARTICLE] {article.source:<20s} | "
        f"sentiment={str(article.overall_sentiment_label):<18s} | "
        f"score={str(article.overall_sentiment_score):<8} | "
        f"freshness={freshness/60:.1f} min | "
        f"{article.title[:60]}"
    )

    # Publish to Pub/Sub
    message_bytes = json.dumps(article.model_dump()).encode("utf-8")
    future = publisher.publish(news_topic_path, message_bytes)
    future.result()  # block briefly to surface publish errors immediately
    obs.record_published()


# ---------------------------------------------------------------------------
# Main collection entry point
# ---------------------------------------------------------------------------

def run_collection() -> ObservabilityState:
    """
    Execute one complete collection cycle:
      fetch → validate → publish → return populated obs state.

    Called from main.py. The caller is responsible for pushing obs metrics
    to the Pushgateway after this returns.
    """
    obs = ObservabilityState()

    raw_articles = fetch_news(obs)

    if not raw_articles:
        print("[COLLECT] No articles to process. Exiting cleanly.")
        return obs

    obs.record_fetched(len(raw_articles))

    for raw in raw_articles:
        process_article(raw, obs)

    return obs
