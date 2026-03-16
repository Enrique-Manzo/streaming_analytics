# news_pipeline.py
# ---------------------------------------------------------------------------
# Dataflow streaming pipeline: Pub/Sub → Parse → Explode → Deduplicate → BigQuery
#
# What it does:
#   1. Reads news batch messages from a Pub/Sub subscription
#      (each message is a JSON *array* of news objects)
#   2. Parses the array and explodes it into individual news records
#   3. Normalises types:
#        - time_published  "20260315T184157" → BigQuery TIMESTAMP string
#        - topics[].relevance_score string → float
#   4. Deduplicates within a 12-hour session window keyed on (url, title)
#      using Beam's stateful DoFn + a timer-based expiry
#   5. Routes invalid messages to a dead letter Pub/Sub topic
#   6. Writes valid, deduplicated records to BigQuery via streaming inserts
#
# Deduplication note:
#   Beam stateful DoFns require a keyed PCollection and a fixed or session
#   window.  We use a 12-hour session window so that the state store holds
#   seen keys for at least 12 h before being garbage-collected by Dataflow.
#   The dedup key is sha256(url + title) to keep key size small.
#
# Run:
#   python news_pipeline.py
# ---------------------------------------------------------------------------

import hashlib
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.userstate import (
    BagStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.coders import VarIntCoder

# ---------------------------------------------------------------------------
# Configuration  —  update these before deploying
# ---------------------------------------------------------------------------

PROJECT_ID   = "tfm-uoc-489523"
SUBSCRIPTION = "projects/tfm-uoc-489523/subscriptions/financial_news-sub"
BQ_TABLE     = "tfm-uoc-489523:financial_data.financial_news"
DLQ_TOPIC    = None   # e.g. "projects/tfm-uoc-489523/topics/news-dlq"
BUCKET       = "dataflow-staging-us-central1-476924094843"

# Deduplication window — 12 hours in seconds
DEDUP_WINDOW_SECONDS = 12 * 60 * 60   # 43 200 s

# ---------------------------------------------------------------------------
# BigQuery schema
# ---------------------------------------------------------------------------

NEWS_SCHEMA = {
    "fields": [
        {"name": "title",                  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "url",                    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "time_published",         "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "summary",                "type": "STRING",    "mode": "NULLABLE"},
        {"name": "source",                 "type": "STRING",    "mode": "NULLABLE"},
        {"name": "category_within_source", "type": "STRING",    "mode": "NULLABLE"},
        {
            "name": "topics",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "topic",           "type": "STRING", "mode": "NULLABLE"},
                {"name": "relevance_score", "type": "FLOAT",  "mode": "NULLABLE"},
            ],
        },
        {"name": "overall_sentiment_score", "type": "FLOAT",  "mode": "NULLABLE"},
        {"name": "overall_sentiment_label", "type": "STRING", "mode": "NULLABLE"},
    ]
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dedup_key(record: dict) -> str:
    """Stable sha256 fingerprint of url + title used as the dedup key."""
    raw = (record.get("url", "") + "|" + record.get("title", "")).encode()
    return hashlib.sha256(raw).hexdigest()


def _parse_time_published(raw: str) -> str:
    """
    Convert Alpha Vantage-style timestamp "20260315T184157"
    to BigQuery RFC-3339 TIMESTAMP string "2026-03-15T18:41:57 UTC".
    """
    dt = datetime.strptime(raw, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S UTC")


# ---------------------------------------------------------------------------
# DoFns
# ---------------------------------------------------------------------------

class ParseAndExplode(beam.DoFn):
    """
    Deserialise a Pub/Sub message that contains a JSON *array* of news objects
    and emit one record per item.  Invalid messages go to the DLQ.
    """
    DLQ = "dlq"

    def process(self, element: bytes, *args, **kwargs):
        try:
            batch = json.loads(element.decode("utf-8"))
            if not isinstance(batch, list):
                # Tolerate a single object wrapped in a dict
                batch = [batch]
            for item in batch:
                yield item
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logging.error("[PARSE ERROR] %s | raw=%s", exc, element[:200])
            yield beam.pvalue.TaggedOutput(
                self.DLQ,
                {"error": str(exc), "raw": element.decode("utf-8", errors="replace")},
            )


class NormaliseNews(beam.DoFn):
    """
    Select the required fields, normalise types, and emit the cleaned record.
    Malformed records are tagged as dead letters.
    """
    DLQ = "dlq"

    def process(self, record: dict, *args, **kwargs):
        try:
            # ── time_published ────────────────────────────────────────────
            time_published = _parse_time_published(record["time_published"])

            # ── topics: cast relevance_score to float ─────────────────────
            raw_topics = record.get("topics") or []
            topics = [
                {
                    "topic":           t.get("topic", ""),
                    "relevance_score": float(t.get("relevance_score", 0.0)),
                }
                for t in raw_topics
            ]

            normalised = {
                "title":                  record.get("title"),
                "url":                    record.get("url"),
                "time_published":         time_published,
                "summary":                record.get("summary"),
                "source":                 record.get("source"),
                "category_within_source": record.get("category_within_source"),
                "topics":                 topics,
                "overall_sentiment_score": float(record["overall_sentiment_score"])
                                           if record.get("overall_sentiment_score") is not None
                                           else None,
                "overall_sentiment_label": record.get("overall_sentiment_label"),
            }

            yield normalised

        except (KeyError, ValueError, TypeError) as exc:
            logging.error("[NORMALISE ERROR] %s | record=%s", exc, record)
            yield beam.pvalue.TaggedOutput(
                self.DLQ,
                {"error": str(exc), "raw": json.dumps(record)},
            )


class DeduplicateNews(beam.DoFn):
    """
    Stateful DoFn that suppresses duplicate news items within a 12-hour window.

    State model
    -----------
    - SEEN_STATE  : a BagState<int> holding a single sentinel value (1) when
                    the key has already been processed.
    - EXPIRY_TIMER: a processing-time timer that clears the state after
                    DEDUP_WINDOW_SECONDS so the key can be accepted again if
                    the same article is re-published after the window.

    Input
    -----
    Keyed PCollection: (dedup_key: str, record: dict)
    The pipeline keys each record by sha256(url|title) before this DoFn.

    Output
    ------
    main  – first-seen records only
    """

    SEEN_STATE  = BagStateSpec("seen", VarIntCoder())
    EXPIRY_TIMER = TimerSpec("expiry", TimeDomain.REAL_TIME)

    def process(
        self,
        element,
        seen=beam.DoFn.StateParam(SEEN_STATE),
        expiry_timer=beam.DoFn.TimerParam(EXPIRY_TIMER),
        *args,
        **kwargs,
    ):
        _, record = element

        # Check if we have already seen this key
        already_seen = any(True for _ in seen.read())

        if not already_seen:
            # Mark as seen and set an expiry timer
            seen.add(1)
            import time as _time
            expiry_timer.set(_time.time() + DEDUP_WINDOW_SECONDS)
            yield record
        else:
            logging.info(
                "[DEDUP] Suppressed duplicate: url=%s", record.get("url", "?")
            )

    @on_timer(EXPIRY_TIMER)
    def expiry(self, seen=beam.DoFn.StateParam(SEEN_STATE)):
        """Clear the seen-state so the key is accepted again after the window."""
        seen.clear()


class FormatDlqMessage(beam.DoFn):
    """Serialise dead letter records back to bytes for the Pub/Sub DLQ topic."""

    def process(self, record: dict, *args, **kwargs):
        yield json.dumps(record).encode("utf-8")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def build_pipeline(pipeline: beam.Pipeline) -> None:

    # ── 1. Read raw bytes from Pub/Sub ────────────────────────────────────
    raw = pipeline | "ReadFromPubSub" >> ReadFromPubSub(subscription=SUBSCRIPTION)

    # ── 2. Parse JSON array → individual records ──────────────────────────
    exploded, parse_dlq = (
        raw
        | "ParseAndExplode" >> beam.ParDo(ParseAndExplode()).with_outputs(
            ParseAndExplode.DLQ, main="exploded"
        )
    )

    # ── 3. Normalise types & select fields ────────────────────────────────
    normalised, normalise_dlq = (
        exploded
        | "NormaliseNews" >> beam.ParDo(NormaliseNews()).with_outputs(
            NormaliseNews.DLQ, main="normalised"
        )
    )

    # ── 4. Deduplicate within 12-hour session window ──────────────────────
    #
    # Stateful DoFns in Beam require:
    #   (a) a keyed PCollection  →  (key, value) pairs
    #   (b) a windowing strategy that bounds state lifetime
    #
    # We apply a SessionWindow with a 12-hour gap.  Because news articles
    # are produced in bursts, a session window naturally expires state for
    # articles that haven't been seen for 12 h, which is the intended
    # deduplication window.
    deduped = (
        normalised
        | "KeyByDedupHash" >> beam.Map(lambda r: (_dedup_key(r), r))
        | "SessionWindow"  >> beam.WindowInto(
            beam.window.Sessions(gap_size=DEDUP_WINDOW_SECONDS)
        )
        | "DeduplicateNews" >> beam.ParDo(DeduplicateNews())
    )

    # ── 5. Write to BigQuery ──────────────────────────────────────────────
    (
        deduped
        | "WriteToBigQuery" >> WriteToBigQuery(
            table=BQ_TABLE,
            schema=NEWS_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )

    # ── 6. Dead letter routing ────────────────────────────────────────────
    if DLQ_TOPIC:
        dlq_messages = (
            (parse_dlq, normalise_dlq)
            | "FlattenDlq"        >> beam.Flatten()
            | "FormatDlqMessages" >> beam.ParDo(FormatDlqMessage())
        )
        dlq_messages | "WriteDlqToPubSub" >> beam.io.WriteToPubSub(topic=DLQ_TOPIC)
    else:
        logging.warning(
            "[DLQ] No DLQ_TOPIC configured — dead letter messages will be logged only."
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        runner="DataflowRunner",
        region="europe-west1",                               # changed
        temp_location=f"gs://{BUCKET}/tmp",
        staging_location=f"gs://{BUCKET}/staging",
        job_name="financial-news-pipeline",
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        build_pipeline(pipeline)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run()
