# quotes_pipeline.py
# ---------------------------------------------------------------------------
# Dataflow streaming pipeline: Pub/Sub → Parse → Normalise → BigQuery
#
# What it does:
#   1. Reads quote messages from a Pub/Sub subscription
#   2. Parses and normalises types (strings → float, ISO string → timestamp)
#   3. Renames raw Alpaca field names to source-agnostic canonical names
#   4. Computes one derived metric:
#        - spread = ask_price - bid_price
#   5. Routes invalid messages to a dead letter Pub/Sub topic
#   6. Writes valid, enriched records to BigQuery via streaming inserts
#
# Field mapping (Alpaca → canonical):
#   T  → type
#   S  → symbol
#   bx → bid_exchange
#   bp → bid_price
#   bs → bid_size
#   ax → ask_exchange
#   ap → ask_price
#   as → ask_size
#   c  → conditions
#   z  → tape
#   t  → exchange_timestamp
#
# Run:
#   python quotes_pipeline.py
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
SUBSCRIPTION = "projects/tfm-uoc-489523/subscriptions/stock_quotes-sub"
BQ_TABLE     = "tfm-uoc-489523:stock_data.quotes"
DLQ_TOPIC    = "projects/tfm-uoc-489523/topics/quotes-dlq"
BUCKET       = "dataflow-staging-us-central1-476924094843"

# ---------------------------------------------------------------------------
# BigQuery table schema
# ---------------------------------------------------------------------------

QUOTES_SCHEMA = {
    "fields": [
        {"name": "type",                 "type": "STRING",    "mode": "REQUIRED"},
        {"name": "symbol",               "type": "STRING",    "mode": "REQUIRED"},
        {"name": "bid_exchange",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "bid_price",            "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "bid_size",             "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "ask_exchange",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "ask_price",            "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "ask_size",             "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "conditions",           "type": "STRING",    "mode": "REPEATED"},
        {"name": "tape",                 "type": "STRING",    "mode": "NULLABLE"},
        {"name": "exchange_timestamp",   "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "ingest_timestamp",     "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "freshness_seconds",    "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "spread",               "type": "FLOAT",     "mode": "NULLABLE"},
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
    1. Normalise types: bp/ap → float, bs/as → int, t → TIMESTAMP string.
    2. Rename raw Alpaca keys to source-agnostic canonical names.
    3. Compute derived metric: spread = ask_price - bid_price.
    4. Tag records that fail normalisation as dead letters.
    """
    DLQ = "dlq"

    def process(self, record: dict, *args, **kwargs):
        try:
            bid_price = float(record["bp"])
            ask_price = float(record["ap"])
            bid_size  = int(record["bs"])
            ask_size  = int(record["as"])

            # Parse exchange timestamp
            exchange_time_dt = datetime.fromisoformat(
                record["t"].replace("Z", "+00:00")
            )
            exchange_time_str = exchange_time_dt.strftime("%Y-%m-%dT%H:%M:%S.%f UTC")

            ingest_ts = float(record["ingest_timestamp"]) if record.get("ingest_timestamp") else None

            # ── Derived metrics ───────────────────────────────────────────
            spread = round(ask_price - bid_price, 6) if ask_price > 0 and bid_price > 0 else None

            enriched = {
                "type":                  record["T"],
                "symbol":                record["S"],
                "bid_exchange":          record["bx"],
                "bid_price":             bid_price,
                "bid_size":              bid_size,
                "ask_exchange":          record["ax"],
                "ask_price":             ask_price,
                "ask_size":              ask_size,
                "conditions":            record.get("c") or [],
                "tape":                  record.get("z"),
                "exchange_timestamp":    exchange_time_str,
                "ingest_timestamp":      ingest_ts,
                "freshness_seconds":     record.get("freshness_seconds"),
                "spread":                spread,
                "pipeline_ingest_time":  datetime.now(timezone.utc).strftime(
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
            schema=QUOTES_SCHEMA,
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
        machine_type="e2-standard-2",
        temp_location=f"gs://{BUCKET}/tmp",
        staging_location=f"gs://{BUCKET}/staging",
        job_name="stock-quotes-pipeline",
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