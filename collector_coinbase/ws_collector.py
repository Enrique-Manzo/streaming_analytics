# ws_collector.py
# ---------------------------------------------------------------------------
# Coinbase WebSocket connection, reconnection logic, and message processing.
# Filters for 'match' and 'last_match' events only, validates against the
# data contract, deduplicates, enriches with freshness, and emits events.
# ---------------------------------------------------------------------------

import asyncio
import json
import time
from datetime import datetime

import websockets
from pydantic import ValidationError

from config import COINBASE_WS_URL, PRODUCTS, WS_RECONNECT_DELAY
from contract import TradeEvent
from observability import ObservabilityState

from google.cloud import pubsub_v1
import asyncio
from concurrent.futures import ThreadPoolExecutor

PROJECT_ID = "tfm-uoc-489523"
TOPIC_ID = "crypto_trades"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
executor = ThreadPoolExecutor(max_workers=4)

MATCH_TYPES = {"match", "last_match"}


async def process_message(raw: dict, obs: ObservabilityState) -> None:
    """Validate one raw message against the data contract and update metrics."""
    obs.record_received()

    if raw.get("type") not in MATCH_TYPES:
        return

    ingest_ts = time.time()

    # ── Contract validation ───────────────────────────────────────────────────
    t_start = time.perf_counter()
    try:
        trade = TradeEvent(**raw)
    except ValidationError as exc:
        detection_ms = (time.perf_counter() - t_start) * 1000
        obs.record_invalid(raw, str(exc), detection_ms)
        print(
            f"[DLQ] Contract violation ({detection_ms:.2f} ms): "
            f"{exc.error_count()} error(s) | "
            f"product={raw.get('product_id')} trade_id={raw.get('trade_id')}"
        )
        return

    # ── Duplicate detection ───────────────────────────────────────────────────
    if obs.is_duplicate(trade.trade_id, trade.sequence):
        print(
            f"[DUP] trade_id={trade.trade_id} "
            f"seq={trade.sequence} product={trade.product_id}"
        )
        return

    # ── Freshness enrichment ──────────────────────────────────────────────────
    try:
        exchange_ts = datetime.fromisoformat(
            trade.time.replace("Z", "+00:00")
        ).timestamp()
        freshness = ingest_ts - exchange_ts
    except Exception:
        freshness = 0.0

    trade.ingest_timestamp = ingest_ts
    trade.freshness_seconds = freshness
    #trade.trade_time = trade.time

    obs.record_valid(freshness)

    # ── Emit event ────────────────────────────────────────────────────────────
    print(
        f"[TRADE] {trade.product_id:12s} | side={trade.side:4s} | "
        f"price={float(trade.price):>12.2f} | size={float(trade.size):.6f} | "
        f"trade_id={trade.trade_id} | freshness={freshness:.3f}s"
    )

    message_bytes = json.dumps(trade.model_dump()).encode("utf-8")
    await asyncio.get_running_loop().run_in_executor(
        executor,
        lambda: publisher.publish(topic_path, message_bytes)
    )


async def coinbase_ws_collector(obs: ObservabilityState) -> None:
    """Maintain a persistent WebSocket connection with automatic reconnection."""
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["matches"],
    }

    while True:
        try:
            print(f"[WS] Connecting to {COINBASE_WS_URL} …")
            async with websockets.connect(
                COINBASE_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                await ws.send(json.dumps(subscribe_msg))
                confirmation = await ws.recv()
                print(f"[WS] Server response: {confirmation}")
                print(f"[WS] Subscribed to {len(PRODUCTS)} products: {PRODUCTS}")

                async for message in ws:
                    try:
                        raw = json.loads(message)
                        await process_message(raw, obs)
                        await asyncio.sleep(0)  # yield control to FastAPI event loop
                    except json.JSONDecodeError as exc:
                        print(f"[ERROR] JSON decode failed: {exc}")

        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.WebSocketException,
            OSError,
        ) as exc:
            print(f"[WS] Connection lost: {exc}. Reconnecting in {WS_RECONNECT_DELAY}s …")
            await asyncio.sleep(WS_RECONNECT_DELAY)
