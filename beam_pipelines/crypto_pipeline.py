# crypto_pipeline.py
# ---------------------------------------------------------------------------
# Dataflow streaming pipeline: Pub/Sub → Normalise → Derive Metrics → BigQuery
#
# What it does:
#   1. Reads trade messages from a Pub/Sub subscription
#   2. Parses and normalises types (strings → float, ISO string → timestamp)
#   3. Computes three derived metrics:
#        - trade_value     = price * size  (notional USD value)
#        - is_large_trade  = trade_value > LARGE_TRADE_THRESHOLD
#        - latency_ms      = (ingest_timestamp - exchange_timestamp) * 1000
#   4. Routes invalid messages to a dead letter Pub/Sub topic
#   5. Writes valid, enriched records to BigQuery via streaming inserts
#
# Run:
# Run:
#   python crypto_pipeline.py
# ---------------------------------------------------------------------------

import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ID   = "tfm-uoc-489523"
SUBSCRIPTION = "projects/tfm-uoc-489523/subscriptions/crypto_trades-sub"
BQ_TABLE     = "tfm-uoc-489523:crypto_trades.trades"
DLQ_TOPIC    = None  # Set to "projects/tfm-uoc-489523/topics/trades-dlq" when ready
BUCKET       = "dataflow-staging-us-central1-476924094843"

# Trades with notional value above this are flagged as large
LARGE_TRADE_THRESHOLD_USD = 10_000.0

# BigQuery table schema — mirrors the table created in GCP
TRADES_SCHEMA = {
    "fields": [
        {"name": "type",                 "type": "STRING",    "mode": "REQUIRED"},
        {"name": "trade_id",             "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "sequence",             "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "product_id",           "type": "STRING",    "mode": "REQUIRED"},
        {"name": "price",                "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "size",                 "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "side",                 "type": "STRING",    "mode": "REQUIRED"},
        {"name": "time",                 "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "maker_order_id",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "taker_order_id",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "ingest_timestamp",     "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "freshness_seconds",    "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "trade_value",          "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "is_large_trade",       "type": "BOOLEAN",   "mode": "REQUIRED"},
        {"name": "latency_ms",           "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "pipeline_ingest_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# ---------------------------------------------------------------------------
# DoFns
# ---------------------------------------------------------------------------

class ParseMessage(beam.DoFn):
    """
    Deserialise raw Pub/Sub bytes into a Python dict.
    Invalid JSON is tagged as a dead letter.
    """
    DLQ = "dlq"

    def process(self, element: bytes, *args, **kwargs):
        try:
            record = json.loads(element.decode("utf-8"))
            yield record
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logging.error("[PARSE ERROR] %s | raw=%s", exc, element[:200])
            yield beam.pvalue.TaggedOutput(
                self.DLQ,
                {"error": str(exc), "raw": element.decode("utf-8", errors="replace")},
            )


class NormaliseAndEnrich(beam.DoFn):
    """
    1. Normalise types: price/size string → float, time string → TIMESTAMP string.
    2. Compute derived metrics: trade_value, is_large_trade, latency_ms.
    3. Tag records that fail normalisation as dead letters.
    """
    DLQ = "dlq"

    def process(self, record: dict, *args, **kwargs):
        try:
            price = float(record["price"])
            size  = float(record["size"])

            # Parse exchange timestamp
            trade_time_dt = datetime.fromisoformat(
                record["time"].replace("Z", "+00:00")
            )
            # BigQuery TIMESTAMP expects RFC 3339 / ISO 8601 with UTC offset
            trade_time_str = trade_time_dt.strftime("%Y-%m-%dT%H:%M:%S.%f UTC")

            ingest_ts = float(record["ingest_timestamp"])

            # ── Derived metrics ───────────────────────────────────────────
            trade_value    = round(price * size, 8)
            is_large_trade = trade_value > LARGE_TRADE_THRESHOLD_USD
            latency_ms     = round(
                (ingest_ts - trade_time_dt.timestamp()) * 1000, 3
            )

            enriched = {
                "type":              record["type"],
                "trade_id":          int(record["trade_id"]),
                "sequence":          int(record["sequence"]),
                "product_id":        record["product_id"],
                "price":             price,
                "size":              size,
                "side":              record["side"],
                "time":              trade_time_str,
                "maker_order_id":    record.get("maker_order_id"),
                "taker_order_id":    record.get("taker_order_id"),
                "ingest_timestamp":  ingest_ts,
                "freshness_seconds": record.get("freshness_seconds"),
                "trade_value":       trade_value,
                "is_large_trade":    is_large_trade,
                "latency_ms":        latency_ms,
                "pipeline_ingest_time": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%f UTC"
                ),
            }

            yield enriched

        except (KeyError, ValueError, TypeError) as exc:
            logging.error("[NORMALISE ERROR] %s | record=%s", exc, record)
            yield beam.pvalue.TaggedOutput(
                self.DLQ,
                {"error": str(exc), "raw": json.dumps(record)},
            )


class FormatDlqMessage(beam.DoFn):
    """Serialise dead letter records back to bytes for Pub/Sub DLQ topic."""

    def process(self, record: dict, *args, **kwargs):
        yield json.dumps(record).encode("utf-8")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def build_pipeline(pipeline: beam.Pipeline) -> None:

    raw = (
        pipeline
        | "ReadFromPubSub" >> ReadFromPubSub(subscription=SUBSCRIPTION)
    )

    parsed, parse_dlq = (
        raw
        | "ParseMessages" >> beam.ParDo(ParseMessage()).with_outputs(
            ParseMessage.DLQ, main="parsed"
        )
    )

    enriched, enrich_dlq = (
        parsed
        | "NormaliseAndEnrich" >> beam.ParDo(NormaliseAndEnrich()).with_outputs(
            NormaliseAndEnrich.DLQ, main="enriched"
        )
    )

    (
        enriched
        | "WriteToBigQuery" >> WriteToBigQuery(
            table=BQ_TABLE,
            schema=TRADES_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )

    if DLQ_TOPIC:
        dlq_messages = (
            (parse_dlq, enrich_dlq)
            | "FlattenDlq"        >> beam.Flatten()
            | "FormatDlqMessages" >> beam.ParDo(FormatDlqMessage())
        )
        (
            dlq_messages
            | "WriteDlqToPubSub" >> beam.io.WriteToPubSub(topic=DLQ_TOPIC)
        )
    else:
        logging.warning(
            "[DLQ] No DLQ_TOPIC configured. Dead letter messages will be logged only."
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        runner="DataflowRunner",
        region="europe-west1",
        temp_location=f"gs://{BUCKET}/tmp",
        staging_location=f"gs://{BUCKET}/staging",
        job_name="coinbase-trades-pipeline",
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