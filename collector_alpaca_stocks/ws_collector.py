# ws_collector.py
# ---------------------------------------------------------------------------
# Alpaca IEX WebSocket connection, reconnection logic, and message processing.
#
# Subscribes to trade ("t") and quote ("q") events for the top 20 S&P 500
# stocks. Each event type is:
#   - Validated against its respective data contract (TradeEvent / QuoteEvent)
#   - Deduplicated
#   - Enriched with ingest_timestamp and freshness_seconds
#   - Published to its dedicated GCP Pub/Sub topic:
#       trades → stock_trades
#       quotes → stock_quotes
# ---------------------------------------------------------------------------

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import websockets
from google.cloud import pubsub_v1
from pydantic import ValidationError

from config import (
    ALPACA_API_KEY,
    ALPACA_API_SECRET,
    ALPACA_WS_URL,
    GCP_PROJECT_ID,
    QUOTES_TOPIC_ID,
    SYMBOLS,
    TRADES_TOPIC_ID,
    WS_RECONNECT_DELAY,
)
from contract import QuoteEvent, TradeEvent
from observability import ObservabilityState

# ── Pub/Sub setup ─────────────────────────────────────────────────────────────
publisher = pubsub_v1.PublisherClient()
trades_topic_path = publisher.topic_path(GCP_PROJECT_ID, TRADES_TOPIC_ID)
quotes_topic_path = publisher.topic_path(GCP_PROJECT_ID, QUOTES_TOPIC_ID)
executor = ThreadPoolExecutor(max_workers=4)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_exchange_ts(ts_str: str) -> float:
    """
    Parse Alpaca's nanosecond-precision ISO-8601 timestamp to a Unix float.
    Python's fromisoformat only handles up to microseconds, so we truncate.
    """
    # "2025-09-19T19:41:06.268341055Z"  →  truncate to microseconds
    if len(ts_str) > 27:
        ts_str = ts_str[:26] + "Z"
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()


# ---------------------------------------------------------------------------
# Message processors
# ---------------------------------------------------------------------------

async def _process_trade(raw: dict, obs: ObservabilityState) -> None:
    """Validate, deduplicate, enrich, and publish a trade event."""
    metrics = obs.trades
    metrics.record_received()

    ingest_ts = time.time()
    t_start = time.perf_counter()

    try:
        trade = TradeEvent(**raw)
    except ValidationError as exc:
        detection_ms = (time.perf_counter() - t_start) * 1000
        metrics.record_invalid(raw, str(exc), detection_ms)
        print(
            f"[TRADE-DLQ] Contract violation ({detection_ms:.2f} ms): "
            f"{exc.error_count()} error(s) | "
            f"symbol={raw.get('S')} trade_id={raw.get('i')}"
        )
        return

    # Deduplicate on trade_id (field "i")
    if metrics.is_duplicate(trade.i):
        print(f"[TRADE-DUP] trade_id={trade.i} symbol={trade.S}")
        return

    # Freshness enrichment
    try:
        freshness = ingest_ts - _parse_exchange_ts(trade.t)
    except Exception:
        freshness = 0.0

    trade.ingest_timestamp = ingest_ts
    trade.freshness_seconds = freshness
    metrics.record_valid(freshness)

    print(
        f"[TRADE] {trade.S:6s} | side=n/a | "
        f"price={trade.p:>10.2f} | size={trade.s:>6d} | "
        f"trade_id={trade.i} | exch={trade.x} | freshness={freshness:.3f}s"
    )

    message_bytes = json.dumps(trade.model_dump()).encode("utf-8")
    await asyncio.get_running_loop().run_in_executor(
        executor,
        lambda: publisher.publish(trades_topic_path, message_bytes),
    )


async def _process_quote(raw: dict, obs: ObservabilityState) -> None:
    """Validate, deduplicate, enrich, and publish a quote event."""
    metrics = obs.quotes
    metrics.record_received()

    ingest_ts = time.time()
    t_start = time.perf_counter()

    try:
        quote = QuoteEvent(**raw)
    except ValidationError as exc:
        detection_ms = (time.perf_counter() - t_start) * 1000
        metrics.record_invalid(raw, str(exc), detection_ms)
        print(
            f"[QUOTE-DLQ] Contract violation ({detection_ms:.2f} ms): "
            f"{exc.error_count()} error(s) | "
            f"symbol={raw.get('S')} ts={raw.get('t')}"
        )
        return

    # Deduplicate on (symbol, exchange_timestamp) composite key
    dedup_key = (quote.S, quote.t)
    if metrics.is_duplicate(dedup_key):
        print(f"[QUOTE-DUP] symbol={quote.S} ts={quote.t}")
        return

    # Freshness enrichment
    try:
        freshness = ingest_ts - _parse_exchange_ts(quote.t)
    except Exception:
        freshness = 0.0

    # Spread calculation
    spread = round(quote.ap - quote.bp, 6) if quote.ap > 0 and quote.bp > 0 else None

    quote.ingest_timestamp = ingest_ts
    quote.freshness_seconds = freshness
    quote.spread = spread
    metrics.record_valid(freshness, spread=spread)

    print(
        f"[QUOTE] {quote.S:6s} | "
        f"bid={quote.bp:>10.2f}x{quote.bs:<5d} | "
        f"ask={quote.ap:>10.2f}x{quote.as_:<5d} | "
        f"spread={spread:.4f} | freshness={freshness:.3f}s"
    )

    message_bytes = json.dumps(quote.model_dump(by_alias=True)).encode("utf-8")
    await asyncio.get_running_loop().run_in_executor(
        executor,
        lambda: publisher.publish(quotes_topic_path, message_bytes),
    )


# ---------------------------------------------------------------------------
# Per-message router
# ---------------------------------------------------------------------------

async def process_message(raw: dict, obs: ObservabilityState) -> None:
    """Route an incoming Alpaca message to the correct processor."""
    msg_type = raw.get("T")

    if msg_type == "t":
        await _process_trade(raw, obs)
    elif msg_type == "q":
        await _process_quote(raw, obs)
    # Silently ignore control messages ("success", "subscription", etc.)


# ---------------------------------------------------------------------------
# WebSocket collector main loop
# ---------------------------------------------------------------------------

async def alpaca_ws_collector(obs: ObservabilityState) -> None:
    """Maintain a persistent Alpaca WebSocket connection with auto-reconnect."""
    if not ALPACA_API_KEY or not ALPACA_API_SECRET:
        raise RuntimeError(
            "ALPACA_API_KEY and ALPACA_API_SECRET must be set "
            "(via .env or environment variables)."
        )

    auth_msg = {
        "action": "auth",
        "key": ALPACA_API_KEY,
        "secret": ALPACA_API_SECRET,
    }
    subscribe_msg = {
        "action": "subscribe",
        "trades": SYMBOLS,
        "quotes": SYMBOLS,
    }

    while True:
        try:
            print(f"[WS] Connecting to {ALPACA_WS_URL} …")
            async with websockets.connect(
                ALPACA_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                # Alpaca sends a welcome message first
                welcome = await ws.recv()
                print(f"[WS] Welcome: {welcome}")

                # Authenticate
                await ws.send(json.dumps(auth_msg))
                auth_response = await ws.recv()
                print(f"[WS] Auth response: {auth_response}")

                # Subscribe
                await ws.send(json.dumps(subscribe_msg))
                sub_response = await ws.recv()
                print(f"[WS] Subscription response: {sub_response}")
                print(f"[WS] Subscribed to {len(SYMBOLS)} symbols: {SYMBOLS}")

                # Main message loop
                async for message in ws:
                    try:
                        # Alpaca sends arrays of messages
                        payload = json.loads(message)
                        if isinstance(payload, list):
                            for item in payload:
                                await process_message(item, obs)
                        else:
                            await process_message(payload, obs)
                        await asyncio.sleep(0)  # yield to FastAPI event loop
                    except json.JSONDecodeError as exc:
                        print(f"[ERROR] JSON decode failed: {exc}")

        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.WebSocketException,
            OSError,
        ) as exc:
            print(
                f"[WS] Connection lost: {exc}. "
                f"Reconnecting in {WS_RECONNECT_DELAY}s …"
            )
            await asyncio.sleep(WS_RECONNECT_DELAY)
