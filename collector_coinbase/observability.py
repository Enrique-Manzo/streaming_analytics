# observability.py
# ---------------------------------------------------------------------------
# In-memory observability state and Prometheus metric renderer.
#
# Metrics tracked:
#   1. throughput                       – valid msgs/sec (rolling window)
#   2. schema_compliance_rate           – fraction passing contract validation
#   3. duplicate_rate                   – fraction flagged as duplicate
#   4. dlq_rate                         – fraction routed to DLQ
#   5. freshness_at_ingestion           – mean exchange→ingest lag (seconds)
#   6. contract_violation_detection_time – mean ms to detect a violation
#
# Windowed metrics (rates, means, deques) are reset on a schedule via
# reset_window(). Lifetime counters (total_*) are never reset — they
# provide an audit trail across resets.
# ---------------------------------------------------------------------------

import threading
import time
from collections import deque

from config import THROUGHPUT_WINDOW_SECONDS, DEDUP_MAXLEN


class ObservabilityState:
    """Thread-safe, in-memory metrics store with periodic window resets."""

    def __init__(self):
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

        # Deduplication — bounded sliding window (never fully reset)
        self._seen_trade_ids: deque = deque(maxlen=DEDUP_MAXLEN)
        self._seen_sequences: deque = deque(maxlen=DEDUP_MAXLEN)
        self._seen_trade_ids_set: set = set()
        self._seen_sequences_set: set = set()

        # DLQ (in-memory; replace with Kafka DLQ topic later)
        self.dlq: deque = deque(maxlen=500)

        # Required metric dimensions for coverage calculation
        self._required_dimensions: set = {
            "throughput", "schema_compliance_rate", "duplicate_rate",
            "dlq_rate", "freshness_at_ingestion",
            "contract_violation_detection_time", "observability_coverage",
        }

    # ── Window reset ─────────────────────────────────────────────────────────

    def reset_window(self):
        """
        Reset all windowed metrics. Called on a schedule (default: hourly).
        Lifetime counters and deduplication sets are intentionally preserved.
        """
        with self._lock:
            self._window_received = 0
            self._window_valid = 0
            self._window_invalid = 0
            self._window_duplicates = 0
            self._window_dlq = 0
            self._valid_timestamps.clear()
            self._freshness_samples.clear()
            self._violation_detection_times_ms.clear()
            self.dlq.clear()
        print("[OBS] Windowed metrics reset.")

    # ── Ingestion helpers ─────────────────────────────────────────────────────

    def record_received(self):
        with self._lock:
            self.total_received += 1
            self._window_received += 1

    def record_valid(self, freshness_seconds: float):
        now = time.monotonic()
        cutoff = now - THROUGHPUT_WINDOW_SECONDS
        with self._lock:
            self.total_valid += 1
            self._window_valid += 1
            self._valid_timestamps.append(now)
            self._freshness_samples.append(freshness_seconds)
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

    def is_duplicate(self, trade_id: int, sequence: int) -> bool:
        with self._lock:
            dup = (trade_id in self._seen_trade_ids_set) or \
                  (sequence in self._seen_sequences_set)
            if dup:
                self.total_duplicates += 1
                self._window_duplicates += 1
            else:
                # Evict oldest if at capacity (deque handles this automatically)
                if len(self._seen_trade_ids) == DEDUP_MAXLEN:
                    evicted_tid = self._seen_trade_ids[0]
                    self._seen_trade_ids_set.discard(evicted_tid)
                if len(self._seen_sequences) == DEDUP_MAXLEN:
                    evicted_seq = self._seen_sequences[0]
                    self._seen_sequences_set.discard(evicted_seq)
                self._seen_trade_ids.append(trade_id)
                self._seen_trade_ids_set.add(trade_id)
                self._seen_sequences.append(sequence)
                self._seen_sequences_set.add(sequence)
            return dup

    # ── Metric computations (windowed) ────────────────────────────────────────

    def throughput(self) -> float:
        """Valid messages per second over the rolling window."""
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

    def snapshot(self) -> dict:
        return {
            # Windowed rates (reset hourly)
            "throughput_msgs_per_sec":               self.throughput(),
            "schema_compliance_rate":                self.schema_compliance_rate(),
            "duplicate_rate":                        self.duplicate_rate(),
            "dlq_rate":                              self.dlq_rate(),
            "freshness_at_ingestion_seconds_mean":   self.freshness_at_ingestion(),
            "contract_violation_detection_time_ms":  self.contract_violation_detection_time(),
            # Lifetime counters (never reset)
            "total_received":    self.total_received,
            "total_valid":       self.total_valid,
            "total_invalid":     self.total_invalid,
            "total_duplicates":  self.total_duplicates,
            "total_dlq":         self.total_dlq,
            # Windowed counters (reset hourly)
            "window_received":   self._window_received,
            "window_valid":      self._window_valid,
            "window_invalid":    self._window_invalid,
            "window_duplicates": self._window_duplicates,
            "window_dlq":        self._window_dlq,
        }


# ---------------------------------------------------------------------------
# Prometheus text exposition
# ---------------------------------------------------------------------------

def render_prometheus_metrics(obs: ObservabilityState) -> str:
    snap = obs.snapshot()
    lines = [
        "# HELP coinbase_collector_throughput_msgs_per_sec Valid messages per second (rolling window)",
        "# TYPE coinbase_collector_throughput_msgs_per_sec gauge",
        f"coinbase_collector_throughput_msgs_per_sec {snap['throughput_msgs_per_sec']}",

        "# HELP coinbase_collector_schema_compliance_rate Fraction of messages passing contract (current window)",
        "# TYPE coinbase_collector_schema_compliance_rate gauge",
        f"coinbase_collector_schema_compliance_rate {snap['schema_compliance_rate']}",

        "# HELP coinbase_collector_duplicate_rate Fraction of messages identified as duplicates (current window)",
        "# TYPE coinbase_collector_duplicate_rate gauge",
        f"coinbase_collector_duplicate_rate {snap['duplicate_rate']}",

        "# HELP coinbase_collector_dlq_rate Fraction of messages routed to DLQ (current window)",
        "# TYPE coinbase_collector_dlq_rate gauge",
        f"coinbase_collector_dlq_rate {snap['dlq_rate']}",

        "# HELP coinbase_collector_freshness_seconds_mean Mean exchange-to-ingest lag in seconds (current window)",
        "# TYPE coinbase_collector_freshness_seconds_mean gauge",
        f"coinbase_collector_freshness_seconds_mean {snap['freshness_at_ingestion_seconds_mean']}",

        "# HELP coinbase_collector_violation_detection_time_ms Mean ms to detect a contract violation (current window)",
        "# TYPE coinbase_collector_violation_detection_time_ms gauge",
        f"coinbase_collector_violation_detection_time_ms {snap['contract_violation_detection_time_ms']}",

        "# HELP coinbase_collector_total_received_total Lifetime messages received",
        "# TYPE coinbase_collector_total_received_total counter",
        f"coinbase_collector_total_received_total {snap['total_received']}",

        "# HELP coinbase_collector_total_valid_total Lifetime messages passing contract validation",
        "# TYPE coinbase_collector_total_valid_total counter",
        f"coinbase_collector_total_valid_total {snap['total_valid']}",

        "# HELP coinbase_collector_total_invalid_total Lifetime messages failing contract validation",
        "# TYPE coinbase_collector_total_invalid_total counter",
        f"coinbase_collector_total_invalid_total {snap['total_invalid']}",

        "# HELP coinbase_collector_total_duplicates_total Lifetime duplicate messages detected",
        "# TYPE coinbase_collector_total_duplicates_total counter",
        f"coinbase_collector_total_duplicates_total {snap['total_duplicates']}",

        "# HELP coinbase_collector_total_dlq_total Lifetime messages routed to DLQ",
        "# TYPE coinbase_collector_total_dlq_total counter",
        f"coinbase_collector_total_dlq_total {snap['total_dlq']}",
    ]
    return "\n".join(lines) + "\n"
