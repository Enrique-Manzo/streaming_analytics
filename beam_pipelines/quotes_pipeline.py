# quotes_pipeline.py
# ---------------------------------------------------------------------------
# Dataflow streaming pipeline: Pub/Sub → Parse → Normalise → BigQuery
#
# What it does:
#   1. Reads quote messages from a Pub/Sub subscription
#   2. Parses and normalises types (strings → float, ISO string → timestamp)
#   3. Computes one derived metric:
#        - spread = ask_price - bid_price
#   4. Routes invalid messages to a dead letter Pub/Sub topic
#   5. Writes valid, enriched records to BigQuery via streaming inserts
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
DLQ_TOPIC    = None  # Set to "projects/tfm-uoc-489523/topics/quotes-dlq" when ready
BUCKET       = "dataflow-staging-us-central1-476924094843"

# ---------------------------------------------------------------------------
# BigQuery table schema
# ---------------------------------------------------------------------------

QUOTES_SCHEMA = {
    "fields": [
        {"name": "T",                    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "S",                    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "bx",                   "type": "STRING",    "mode": "REQUIRED"},
        {"name": "bp",                   "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "bs",                   "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "ax",                   "type": "STRING",    "mode": "REQUIRED"},
        {"name": "ap",                   "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "as",                   "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "c",                    "type": "STRING",    "mode": "REPEATED"},
        {"name": "z",                    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "t",                    "type": "TIMESTAMP", "mode": "REQUIRED"},
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
    1. Normalise types: bp/ap/bs/as → float/int, t string → TIMESTAMP string.
    2. Compute derived metric: spread = ap - bp.
    3. Tag records that fail normalisation as dead letters.
    """
    DLQ = "dlq"

    def process(self, record: dict, *args, **kwargs):
        try:
            bp = float(record["bp"])
            ap = float(record["ap"])
            bs = int(record["bs"])
            as_ = int(record["as"])

            # Parse exchange timestamp
            quote_time_dt = datetime.fromisoformat(
                record["t"].replace("Z", "+00:00")
            )
            quote_time_str = quote_time_dt.strftime("%Y-%m-%dT%H:%M:%S.%f UTC")

            ingest_ts = float(record["ingest_timestamp"]) if record.get("ingest_timestamp") else None

            # ── Derived metrics ───────────────────────────────────────────
            spread = round(ap - bp, 6) if ap > 0 and bp > 0 else None

            enriched = {
                "T":                     record["T"],
                "S":                     record["S"],
                "bx":                    record["bx"],
                "bp":                    bp,
                "bs":                    bs,
                "ax":                    record["ax"],
                "ap":                    ap,
                "as":                    as_,
                "c":                     record.get("c") or [],
                "z":                     record.get("z"),
                "t":                     quote_time_str,
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
        temp_location=f"gs://{BUCKET}/tmp",
        staging_location=f"gs://{BUCKET}/staging",
        job_name="alpaca-quotes-pipeline",
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
