# observability.py
# ---------------------------------------------------------------------------
# In-memory observability state and Prometheus metric renderer.
#
# Tracks separate metric sets for trade events and quote events.
#
# Metrics tracked (per stream: trades / quotes):
#   1. throughput                       – valid msgs/sec (rolling window)
#   2. schema_compliance_rate           – fraction passing contract validation
#   3. duplicate_rate                   – fraction flagged as duplicate
#   4. dlq_rate                         – fraction routed to DLQ
#   5. freshness_at_ingestion           – mean exchange→ingest lag (seconds)
#   6. contract_violation_detection_time – mean ms to detect a violation
#
# Quote-only metrics:
#   7. mean_bid_ask_spread              – mean (ask - bid) over the window
#
# Windowed metrics (rates, means, deques) are reset on a schedule via
# reset_window(). Lifetime counters (total_*) are never reset.
# ---------------------------------------------------------------------------

import threading
import time
from collections import deque

from config import THROUGHPUT_WINDOW_SECONDS, DEDUP_MAXLEN


class StreamMetrics:
    """
    Thread-safe windowed + lifetime counters for a single message stream
    (either trades or quotes). Extra deques hold domain-specific samples.
    """

    def __init__(self, stream_name: str):
        self.stream_name = stream_name
        self._lock = threading.Lock()

        # ── Lifetime counters (never reset) ──────────────────────────────────
        self.total_received: int = 0
        self.total_valid: int = 0
        self.total_invalid: int = 0
        self.total_duplicates: int = 0
        self.total_dlq: int = 0

        # ── Windowed counters (reset hourly) ─────────────────────────────────
        self._window_received: int = 0
        self._window_valid: int = 0
        self._window_invalid: int = 0
        self._window_duplicates: int = 0
        self._window_dlq: int = 0

        # Rolling timestamps of valid messages (for throughput)
        self._valid_timestamps: deque = deque()

        # Bounded sample deques (reset hourly)
        self._freshness_samples: deque = deque(maxlen=1000)
        self._violation_detection_times_ms: deque = deque(maxlen=1000)

        # Quote-only: bid-ask spread samples
        self._spread_samples: deque = deque(maxlen=1000)

        # Deduplication — bounded sliding window (never fully reset)
        # For trades: deduplication on trade_id (field "i")
        # For quotes: deduplication on (symbol, timestamp) composite key
        self._seen_keys: deque = deque(maxlen=DEDUP_MAXLEN)
        self._seen_keys_set: set = set()

        # DLQ (in-memory; replace with Pub/Sub DLQ topic later)
        self.dlq: deque = deque(maxlen=500)

    # ── Window reset ─────────────────────────────────────────────────────────

    def reset_window(self):
        with self._lock:
            self._window_received = 0
            self._window_valid = 0
            self._window_invalid = 0
            self._window_duplicates = 0
            self._window_dlq = 0
            self._valid_timestamps.clear()
            self._freshness_samples.clear()
            self._violation_detection_times_ms.clear()
            self._spread_samples.clear()
            self.dlq.clear()
        print(f"[OBS] {self.stream_name} windowed metrics reset.")

    # ── Ingestion helpers ─────────────────────────────────────────────────────

    def record_received(self):
        with self._lock:
            self.total_received += 1
            self._window_received += 1

    def record_valid(self, freshness_seconds: float, spread: float | None = None):
        now = time.monotonic()
        cutoff = now - THROUGHPUT_WINDOW_SECONDS
        with self._lock:
            self.total_valid += 1
            self._window_valid += 1
            self._valid_timestamps.append(now)
            self._freshness_samples.append(freshness_seconds)
            if spread is not None:
                self._spread_samples.append(spread)
            while self._valid_timestamps and self._valid_timestamps[0] < cutoff:
                self._valid_timestamps.popleft()

    def record_invalid(self, raw: dict, reason: str, detection_ms: float):
        with self._lock:
            self.total_invalid += 1
            self.total_dlq += 1
            self._window_invalid += 1
            self._window_dlq += 1
            self._violation_detection_times_ms.append(detection_ms)
            self.dlq.append({"raw": raw, "reason": reason, "ts": time.time()})

    def is_duplicate(self, key) -> bool:
        """key can be any hashable (int trade_id, or tuple for quotes)."""
        with self._lock:
            dup = key in self._seen_keys_set
            if dup:
                self.total_duplicates += 1
                self._window_duplicates += 1
            else:
                if len(self._seen_keys) == DEDUP_MAXLEN:
                    evicted = self._seen_keys[0]
                    self._seen_keys_set.discard(evicted)
                self._seen_keys.append(key)
                self._seen_keys_set.add(key)
            return dup

    # ── Metric computations (windowed) ────────────────────────────────────────

    def throughput(self) -> float:
        now = time.monotonic()
        cutoff = now - THROUGHPUT_WINDOW_SECONDS
        with self._lock:
            while self._valid_timestamps and self._valid_timestamps[0] < cutoff:
                self._valid_timestamps.popleft()
            count = len(self._valid_timestamps)
        return round(count / THROUGHPUT_WINDOW_SECONDS, 4)

    def schema_compliance_rate(self) -> float:
        with self._lock:
            if self._window_received == 0:
                return 1.0
            return round(self._window_valid / self._window_received, 6)

    def duplicate_rate(self) -> float:
        with self._lock:
            if self._window_received == 0:
                return 0.0
            return round(self._window_duplicates / self._window_received, 6)

    def dlq_rate(self) -> float:
        with self._lock:
            if self._window_received == 0:
                return 0.0
            return round(self._window_dlq / self._window_received, 6)

    def freshness_at_ingestion(self) -> float:
        with self._lock:
            samples = list(self._freshness_samples)
        if not samples:
            return 0.0
        return round(sum(samples) / len(samples), 4)

    def contract_violation_detection_time(self) -> float:
        with self._lock:
            samples = list(self._violation_detection_times_ms)
        if not samples:
            return 0.0
        return round(sum(samples) / len(samples), 4)

    def mean_spread(self) -> float:
        """Mean bid-ask spread over the current window (quotes only)."""
        with self._lock:
            samples = list(self._spread_samples)
        if not samples:
            return 0.0
        return round(sum(samples) / len(samples), 6)

    def snapshot(self) -> dict:
        base = {
            # Windowed rates
            "throughput_msgs_per_sec":              self.throughput(),
            "schema_compliance_rate":               self.schema_compliance_rate(),
            "duplicate_rate":                       self.duplicate_rate(),
            "dlq_rate":                             self.dlq_rate(),
            "freshness_at_ingestion_seconds_mean":  self.freshness_at_ingestion(),
            "contract_violation_detection_time_ms": self.contract_violation_detection_time(),
            # Lifetime counters
            "total_received":   self.total_received,
            "total_valid":      self.total_valid,
            "total_invalid":    self.total_invalid,
            "total_duplicates": self.total_duplicates,
            "total_dlq":        self.total_dlq,
            # Windowed counters
            "window_received":   self._window_received,
            "window_valid":      self._window_valid,
            "window_invalid":    self._window_invalid,
            "window_duplicates": self._window_duplicates,
            "window_dlq":        self._window_dlq,
        }
        if self._spread_samples.maxlen is not None:
            base["mean_bid_ask_spread"] = self.mean_spread()
        return base


# ---------------------------------------------------------------------------
# Top-level observability state — one StreamMetrics per stream type
# ---------------------------------------------------------------------------

class ObservabilityState:
    """Aggregates StreamMetrics for both the trades and quotes streams."""

    def __init__(self):
        self.trades = StreamMetrics("trades")
        self.quotes = StreamMetrics("quotes")

    def reset_window(self):
        self.trades.reset_window()
        self.quotes.reset_window()

    def snapshot(self) -> dict:
        return {
            "trades": self.trades.snapshot(),
            "quotes": self.quotes.snapshot(),
        }


# ---------------------------------------------------------------------------
# Prometheus text exposition
# ---------------------------------------------------------------------------

def _stream_prometheus_lines(prefix: str, snap: dict) -> list[str]:
    lines = []

    def gauge(name, help_text, value):
        lines += [
            f"# HELP {prefix}_{name} {help_text}",
            f"# TYPE {prefix}_{name} gauge",
            f"{prefix}_{name} {value}",
        ]

    def counter(name, help_text, value):
        lines += [
            f"# HELP {prefix}_{name}_total {help_text}",
            f"# TYPE {prefix}_{name}_total counter",
            f"{prefix}_{name}_total {value}",
        ]

    gauge("throughput_msgs_per_sec",
          "Valid messages per second (rolling window)",
          snap["throughput_msgs_per_sec"])
    gauge("schema_compliance_rate",
          "Fraction of messages passing contract (current window)",
          snap["schema_compliance_rate"])
    gauge("duplicate_rate",
          "Fraction of messages identified as duplicates (current window)",
          snap["duplicate_rate"])
    gauge("dlq_rate",
          "Fraction of messages routed to DLQ (current window)",
          snap["dlq_rate"])
    gauge("freshness_seconds_mean",
          "Mean exchange-to-ingest lag in seconds (current window)",
          snap["freshness_at_ingestion_seconds_mean"])
    gauge("violation_detection_time_ms",
          "Mean ms to detect a contract violation (current window)",
          snap["contract_violation_detection_time_ms"])

    if "mean_bid_ask_spread" in snap:
        gauge("mean_bid_ask_spread",
              "Mean bid-ask spread in dollars (current window)",
              snap["mean_bid_ask_spread"])

    counter("total_received",    "Lifetime messages received",                        snap["total_received"])
    counter("total_valid",       "Lifetime messages passing contract validation",      snap["total_valid"])
    counter("total_invalid",     "Lifetime messages failing contract validation",      snap["total_invalid"])
    counter("total_duplicates",  "Lifetime duplicate messages detected",              snap["total_duplicates"])
    counter("total_dlq",         "Lifetime messages routed to DLQ",                   snap["total_dlq"])

    return lines


def render_prometheus_metrics(obs: ObservabilityState) -> str:
    snap = obs.snapshot()
    lines = (
        _stream_prometheus_lines("alpaca_collector_trades", snap["trades"])
        + _stream_prometheus_lines("alpaca_collector_quotes", snap["quotes"])
    )
    return "\n".join(lines) + "\n"
