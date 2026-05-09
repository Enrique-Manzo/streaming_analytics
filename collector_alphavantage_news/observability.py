# observability.py
# ---------------------------------------------------------------------------
# Observability state and Prometheus Pushgateway integration for the
# Alpha Vantage news collector.
#
# Because this collector runs as a short-lived Cloud Run Job (not a
# long-running server), metrics are accumulated in-memory during the run
# and pushed to a Prometheus Pushgateway at the end of execution.
# Prometheus then scrapes the Pushgateway on its normal schedule.
#
# Metrics tracked per job run:
#   1. articles_fetched          – total raw articles returned by the API
#   2. articles_published        – articles that passed validation and were
#                                  pushed to Pub/Sub
#   3. schema_compliance_rate    – fraction passing contract validation
#   4. dlq_rate                  – fraction routed to DLQ (validation failures)
#   5. freshness_seconds_mean    – mean age of published articles at ingest
#   6. api_latency_seconds       – time taken for the Alpha Vantage HTTP call
#   7. violation_detection_ms    – mean ms to detect a contract violation
#   8. run_duration_seconds      – total wall-clock time for the job run
#
# Sentiment distribution (gauge per label):
#   9. sentiment_<label>_count   – count of articles per sentiment label
# ---------------------------------------------------------------------------

import time
import urllib.request
import urllib.error
from collections import defaultdict

from config import PUSHGATEWAY_HOST, PUSHGATEWAY_PORT, PUSHGATEWAY_JOB_NAME


class ObservabilityState:
    """
    Accumulates metrics for a single Cloud Run Job execution.
    All state is in-memory; call push_to_gateway() at the end of the run.
    """

    def __init__(self):
        self._run_start: float = time.time()

        # ── Counters ──────────────────────────────────────────────────────────
        self.articles_fetched: int = 0
        self.articles_valid: int = 0
        self.articles_invalid: int = 0
        self.articles_published: int = 0
        self.articles_dlq: int = 0

        # ── Timing samples ────────────────────────────────────────────────────
        self.api_latency_seconds: float = 0.0          # set once after HTTP call
        self._freshness_samples: list[float] = []
        self._violation_detection_ms_samples: list[float] = []

        # ── Sentiment distribution ─────────────────────────────────────────────
        self._sentiment_counts: dict[str, int] = defaultdict(int)

        # ── DLQ (in-memory) ───────────────────────────────────────────────────
        self.dlq: list[dict] = []

    # ── Recording helpers ─────────────────────────────────────────────────────

    def record_fetched(self, count: int):
        """Call once after the API response is received."""
        self.articles_fetched = count

    def record_api_latency(self, seconds: float):
        self.api_latency_seconds = round(seconds, 4)

    def record_valid(self, freshness_seconds: float, sentiment_label: str | None):
        self.articles_valid += 1
        self._freshness_samples.append(freshness_seconds)
        if sentiment_label:
            self._sentiment_counts[sentiment_label] += 1

    def record_invalid(self, raw: dict, reason: str, detection_ms: float):
        self.articles_invalid += 1
        self.articles_dlq += 1
        self._violation_detection_ms_samples.append(detection_ms)
        self.dlq.append({"raw": raw, "reason": reason, "ts": time.time()})

    def record_published(self):
        self.articles_published += 1

    # ── Computed metrics ──────────────────────────────────────────────────────

    def schema_compliance_rate(self) -> float:
        if self.articles_fetched == 0:
            return 1.0
        return round(self.articles_valid / self.articles_fetched, 6)

    def dlq_rate(self) -> float:
        if self.articles_fetched == 0:
            return 0.0
        return round(self.articles_dlq / self.articles_fetched, 6)

    def freshness_seconds_mean(self) -> float:
        if not self._freshness_samples:
            return 0.0
        return round(sum(self._freshness_samples) / len(self._freshness_samples), 4)

    def violation_detection_ms_mean(self) -> float:
        if not self._violation_detection_ms_samples:
            return 0.0
        s = self._violation_detection_ms_samples
        return round(sum(s) / len(s), 4)

    def run_duration_seconds(self) -> float:
        return round(time.time() - self._run_start, 4)

    def snapshot(self) -> dict:
        return {
            "articles_fetched":            self.articles_fetched,
            "articles_valid":              self.articles_valid,
            "articles_invalid":            self.articles_invalid,
            "articles_published":          self.articles_published,
            "articles_dlq":                self.articles_dlq,
            "schema_compliance_rate":      self.schema_compliance_rate(),
            "dlq_rate":                    self.dlq_rate(),
            "freshness_seconds_mean":      self.freshness_seconds_mean(),
            "api_latency_seconds":         self.api_latency_seconds,
            "violation_detection_ms_mean": self.violation_detection_ms_mean(),
            "run_duration_seconds":        self.run_duration_seconds(),
            "sentiment_distribution":      dict(self._sentiment_counts),
        }

    # ── Prometheus exposition ─────────────────────────────────────────────────

    def _render_prometheus(self) -> str:
        snap = self.snapshot()

        def gauge(name, help_text, value) -> list[str]:
            return [
                f"# HELP {name} {help_text}",
                f"# TYPE {name} gauge",
                f"{name} {value}",
            ]

        lines: list[str] = []

        lines += gauge("news_collector_articles_fetched",
                       "Total articles returned by the Alpha Vantage API in this run",
                       snap["articles_fetched"])
        lines += gauge("news_collector_articles_valid",
                       "Articles passing contract validation in this run",
                       snap["articles_valid"])
        lines += gauge("news_collector_articles_invalid",
                       "Articles failing contract validation in this run",
                       snap["articles_invalid"])
        lines += gauge("news_collector_articles_published",
                       "Articles successfully published to Pub/Sub in this run",
                       snap["articles_published"])
        lines += gauge("news_collector_articles_dlq",
                       "Articles routed to DLQ in this run",
                       snap["articles_dlq"])
        lines += gauge("news_collector_schema_compliance_rate",
                       "Fraction of fetched articles passing contract validation",
                       snap["schema_compliance_rate"])
        lines += gauge("news_collector_dlq_rate",
                       "Fraction of fetched articles routed to DLQ",
                       snap["dlq_rate"])
        lines += gauge("news_collector_freshness_seconds_mean",
                       "Mean article age at ingest (seconds since time_published)",
                       snap["freshness_seconds_mean"])
        lines += gauge("news_collector_api_latency_seconds",
                       "Wall-clock seconds for the Alpha Vantage HTTP call",
                       snap["api_latency_seconds"])
        lines += gauge("news_collector_violation_detection_ms_mean",
                       "Mean milliseconds to detect a contract violation",
                       snap["violation_detection_ms_mean"])
        lines += gauge("news_collector_run_duration_seconds",
                       "Total wall-clock duration of this job run (seconds)",
                       snap["run_duration_seconds"])

        # Sentiment distribution — one gauge per label
        for label, count in snap["sentiment_distribution"].items():
            safe_label = label.lower().replace("-", "_").replace(" ", "_")
            lines += gauge(
                f"news_collector_sentiment_{safe_label}_count",
                f"Number of articles with sentiment label '{label}' in this run",
                count,
            )

        return "\n".join(lines) + "\n"

    # ── Push to Pushgateway ───────────────────────────────────────────────────

    def push_to_gateway(self) -> None:
        """
        Push all metrics to the Prometheus Pushgateway via the text exposition
        format over plain HTTP (no external prometheus_client dependency needed).

        The Pushgateway URL format is:
            http://<host>:<port>/metrics/job/<job_name>
        """
        url = (
            f"http://{PUSHGATEWAY_HOST}:{PUSHGATEWAY_PORT}"
            f"/metrics/job/{PUSHGATEWAY_JOB_NAME}"
        )
        payload = self._render_prometheus().encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload,
            method="PUT",
            headers={"Content-Type": "text/plain; version=0.0.4"},
        )

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(
                    f"[OBS] Metrics pushed to Pushgateway at {url} "
                    f"(HTTP {resp.status})"
                )
        except urllib.error.URLError as exc:
            # Non-fatal: log and continue so the job doesn't fail on metrics
            print(f"[OBS] WARNING — failed to push metrics to {url}: {exc}")

    def log_summary(self) -> None:
        """Print a human-readable run summary to stdout (captured by Cloud Logging)."""
        snap = self.snapshot()
        print("\n" + "=" * 60)
        print("[OBS] News Collector — Run Summary")
        print("=" * 60)
        for k, v in snap.items():
            if k != "sentiment_distribution":
                print(f"  {k:<35s} {v}")
        print("  Sentiment distribution:")
        for label, count in snap["sentiment_distribution"].items():
            print(f"    {label:<30s} {count}")
        print("=" * 60 + "\n")
