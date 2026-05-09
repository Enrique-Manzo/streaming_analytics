# aapl_trades_pipeline.py
# ---------------------------------------------------------------------------
# Dataflow streaming pipeline (AAPL-only):
#   Pub/Sub → Parse → Normalise → BigQuery (trades)
#                  ↘ 1-min OHLCV bars → 60-bar rolling window → RF inference
#                                                             → BigQuery (vol predictions)
#
# What it does:
#   1.  Reads AAPL trade messages from a dedicated Pub/Sub subscription
#   2.  Parses and normalises types (strings → float, ISO string → timestamp)
#   3.  Renames raw Alpaca field names to source-agnostic canonical names
#   4.  Computes three derived trade metrics:
#          - trade_value     = price * size  (notional USD value)
#          - is_large_trade  = trade_value > LARGE_TRADE_THRESHOLD
#          - latency_ms      = (ingest_timestamp - exchange_timestamp) * 1000
#   5.  Routes invalid messages to a dead-letter Pub/Sub topic
#   6.  Writes valid, enriched trade records to BigQuery (trades table)
#   7.  In parallel, aggregates enriched trades into 1-minute OHLCV bars
#       using FixedWindows(60)
#   8.  A stateful DoFn maintains a rolling buffer of the last 60 bars and,
#       once the buffer is full, loads a Random Forest classifier from GCS
#       (loaded once per worker via setup()) and emits a volatility prediction
#   9.  Volatility predictions are written to a separate BigQuery table
#
# Volatility prediction output schema (vol_predictions table):
#   symbol            - always "AAPL"
#   window_end        - timestamp of the last bar in the 60-bar context window
#   prediction        - 0 (vol decreasing) or 1 (vol increasing)
#   probability       - P(vol up), float [0, 1]
#   label             - human-readable: "vol_up" or "vol_down"
#   realized_vol      - std dev of log returns over the 60-bar window
#   trade_count       - total number of raw trades that fed the 60 bars
#   mean_trade_value  - average notional USD value of trades in the window
#   price_range_pct   - mean (high-low)/close across the 60 bars (range %)
#   pipeline_time     - wall-clock time the prediction was emitted
#
# Field mapping (Alpaca → canonical):
#   T → type            S → symbol       i → trade_id
#   x → exchange        p → price        s → size
#   c → conditions      z → tape         t → exchange_timestamp
#
# Run:
#   python aapl_trades_pipeline.py
# ---------------------------------------------------------------------------

import io
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
import numpy as np
import pandas as pd
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
import joblib
from google.cloud import bigquery, storage

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ID    = "tfm-uoc-489523"
SUBSCRIPTION  = "projects/tfm-uoc-489523/subscriptions/aapl_stock_trades-sub"
BQ_TABLE      = "tfm-uoc-489523:stock_data.trades"
VOL_BQ_TABLE  = "tfm-uoc-489523:stock_data.aapl_vol_predictions"
DLQ_TOPIC     = "projects/tfm-uoc-489523/topics/aapl-trades-dlq"
BUCKET        = "dataflow-staging-us-central1-476924094843"
MODEL_BLOB    = "models/vol_classifier_aapl.joblib"

# Trades with notional value above this are flagged as large
LARGE_TRADE_THRESHOLD_USD = 10_000.0

# Number of 1-minute bars required before inference fires
ROLLING_WINDOW_BARS = 60

# ---------------------------------------------------------------------------
# BigQuery schemas
# ---------------------------------------------------------------------------

TRADES_SCHEMA = {
    "fields": [
        {"name": "type",                 "type": "STRING",    "mode": "REQUIRED"},
        {"name": "symbol",               "type": "STRING",    "mode": "REQUIRED"},
        {"name": "trade_id",             "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "exchange",             "type": "STRING",    "mode": "REQUIRED"},
        {"name": "price",                "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "size",                 "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "conditions",           "type": "STRING",    "mode": "REPEATED"},
        {"name": "tape",                 "type": "STRING",    "mode": "NULLABLE"},
        {"name": "exchange_timestamp",   "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "ingest_timestamp",     "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "freshness_seconds",    "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "trade_value",          "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "is_large_trade",       "type": "BOOLEAN",   "mode": "REQUIRED"},
        {"name": "latency_ms",           "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "pipeline_ingest_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

VOL_PRED_SCHEMA = {
    "fields": [
        # Core identification
        {"name": "symbol",           "type": "STRING",    "mode": "REQUIRED"},
        {"name": "window_end",       "type": "TIMESTAMP", "mode": "REQUIRED"},
        # Prediction outputs
        {"name": "prediction",       "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "probability",      "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "label",            "type": "STRING",    "mode": "REQUIRED"},
        # Context / feature transparency
        {"name": "realized_vol",     "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "trade_count",      "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "mean_trade_value", "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "price_range_pct",  "type": "FLOAT",     "mode": "NULLABLE"},
        # Audit
        {"name": "pipeline_time",    "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

# ---------------------------------------------------------------------------
# DoFns — trade processing (identical logic to the multi-symbol pipeline)
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
    1. Normalise types: p → float, s → int, t → TIMESTAMP string.
    2. Rename raw Alpaca keys to source-agnostic canonical names.
    3. Compute derived metrics: trade_value, is_large_trade, latency_ms.
    4. Tag records that fail normalisation as dead letters.
    """
    DLQ = "dlq"

    def process(self, record: dict, *args, **kwargs):
        try:
            price = float(record["p"])
            size  = int(record["s"])

            exchange_time_dt = datetime.fromisoformat(
                record["t"].replace("Z", "+00:00")
            )
            exchange_time_str = exchange_time_dt.strftime("%Y-%m-%dT%H:%M:%S.%f UTC")

            ingest_ts = float(record["ingest_timestamp"]) if record.get("ingest_timestamp") else None

            trade_value    = round(price * size, 8)
            is_large_trade = trade_value > LARGE_TRADE_THRESHOLD_USD
            latency_ms     = round(
                (ingest_ts - exchange_time_dt.timestamp()) * 1000, 3
            ) if ingest_ts else 0.0

            enriched = {
                "type":                  record["T"],
                "symbol":                record["S"],
                "trade_id":              int(record["i"]),
                "exchange":              record["x"],
                "price":                 price,
                "size":                  size,
                "conditions":            record.get("c") or [],
                "tape":                  record.get("z"),
                "exchange_timestamp":    exchange_time_str,
                "ingest_timestamp":      ingest_ts,
                "freshness_seconds":     record.get("freshness_seconds"),
                "trade_value":           trade_value,
                "is_large_trade":        is_large_trade,
                "latency_ms":            latency_ms,
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
    """Serialise dead-letter records back to bytes for the Pub/Sub DLQ topic."""

    def process(self, record: dict, *args, **kwargs):
        yield json.dumps(record).encode("utf-8")


# ---------------------------------------------------------------------------
# DoFns — volatility prediction branch
# ---------------------------------------------------------------------------

class BuildOHLCVBar(beam.DoFn):
    """
    Receives a (key, iterable-of-trades) element produced by GroupByKey inside
    a FixedWindows(60) window and emits a single 1-minute OHLCV bar dict.

    The key is always "AAPL" (set in build_pipeline).  We keep it so the
    downstream stateful DoFn can use it as the per-key grouping handle.

    Extra fields carried forward:
      - trade_count   : number of raw trades in this bar
      - mean_trade_value : mean notional USD value of trades
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        key, trades = element
        trades = list(trades)
        if not trades:
            return

        prices  = [t["price"]        for t in trades]
        sizes   = [t["size"]         for t in trades]
        tvalues = [t["trade_value"]   for t in trades]

        total_vol = sum(sizes)
        vw = (
            sum(p * s for p, s in zip(prices, sizes)) / total_vol
            if total_vol else prices[-1]
        )

        bar = {
            "open":             prices[0],
            "high":             max(prices),
            "low":              min(prices),
            "close":            prices[-1],
            "volume":           float(total_vol),
            "vw":               round(vw, 6),
            "n":                float(len(trades)),
            "trade_count":      len(trades),
            "mean_trade_value": round(sum(tvalues) / len(tvalues), 4),
            # ISO string so it survives Beam's FastPrimitivesCoder
            "bar_time": window.end.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S UTC"),
        }
        yield key, bar


class VolatilityPredictor(beam.DoFn):
    """
    Stateless DoFn — no BagState.

    On every new bar emitted by BuildOHLCVBar:
      1. Query BigQuery for the 60 most recent 1-minute OHLCV bars for AAPL,
         aggregated on the fly from the trades table.  The query looks back up
         to 7 days so bars from the previous session are included automatically.
      2. If fewer than ROLLING_WINDOW_BARS rows come back: log and skip.
      3. Otherwise run RF inference and emit a prediction dict.

    BigQuery client and GCS model are initialised once per worker in setup().
    """

    # ── Worker lifecycle ──────────────────────────────────────────────────

    def setup(self):
        """Initialise BQ client and load RF model from GCS once per worker."""
        logging.info("[VOL] Loading model from gs://%s/%s", BUCKET, MODEL_BLOB)
        gcs_client = storage.Client()
        buf = io.BytesIO()
        gcs_client.bucket(BUCKET).blob(MODEL_BLOB).download_to_file(buf)
        buf.seek(0)
        self.model = joblib.load(buf)
        logging.info("[VOL] Model loaded successfully.")

        self.bq_client = bigquery.Client(project=PROJECT_ID)
        logging.info("[VOL] BigQuery client initialised.")

    # ── Per-element processing ────────────────────────────────────────────

    # Query template: aggregates raw trades into 1-min OHLCV bars.
    # Looks back 7 days so overnight / weekend gaps are covered.
    # The current bar's end timestamp is passed as a query parameter so we
    # never include trades that haven't closed yet.
    _BQ_QUERY = """
        SELECT
            TIMESTAMP_TRUNC(exchange_timestamp, MINUTE)                          AS bar_time,
            ARRAY_AGG(price ORDER BY exchange_timestamp)[OFFSET(0)]              AS open,
            MAX(price)                                                            AS high,
            MIN(price)                                                            AS low,
            ARRAY_AGG(price ORDER BY exchange_timestamp DESC)[OFFSET(0)]         AS close,
            CAST(SUM(size) AS FLOAT64)                                           AS volume,
            SAFE_DIVIDE(SUM(price * size), SUM(size))                            AS vw,
            CAST(COUNT(*) AS FLOAT64)                                            AS n,
            COUNT(*)                                                              AS trade_count,
            AVG(trade_value)                                                      AS mean_trade_value
        FROM `tfm-uoc-489523.stock_data.trades`
        WHERE symbol = 'AAPL'
          AND exchange_timestamp <= @bar_end
          AND exchange_timestamp >= TIMESTAMP_SUB(@bar_end, INTERVAL 7 DAY)
        GROUP BY bar_time
        ORDER BY bar_time DESC
        LIMIT @n_bars
    """

    def _fetch_bars(self, bar_end: str) -> pd.DataFrame:
        bar_end_ts = datetime.strptime(bar_end, "%Y-%m-%dT%H:%M:%S UTC").replace(
            tzinfo=timezone.utc
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("bar_end", "TIMESTAMP", bar_end_ts),
                bigquery.ScalarQueryParameter("n_bars", "INT64", ROLLING_WINDOW_BARS),
            ]
        )
        rows = self.bq_client.query(self._BQ_QUERY, job_config=job_config).result()
        df = pd.DataFrame([dict(row) for row in rows])
        if df.empty:
            return df
        df["bar_time"] = pd.to_datetime(df["bar_time"], utc=True)
        return df.sort_values("bar_time").reset_index(drop=True)

    def process(self, element):
        _key, bar = element

        df = self._fetch_bars(bar["bar_time"])

        if len(df) < ROLLING_WINDOW_BARS:
            logging.info(
                "[VOL] Warming up: %d / %d bars available in BQ.",
                len(df), ROLLING_WINDOW_BARS,
            )
            return

        prediction = self._run_inference(df, bar["bar_time"])
        yield prediction

    # ── Inference ─────────────────────────────────────────────────────────

    def _run_inference(self, window_df: pd.DataFrame, window_end: str) -> dict:
        """
        Compute the feature vector, call the RF, return the prediction dict.
        Matches compute_features_from_window() from the training notebook.
        """
        log_ret     = np.log(window_df["close"] / window_df["close"].shift(1)).dropna()
        price_range = (window_df["high"] - window_df["low"]) / window_df["close"]
        vwap_spread = np.abs(window_df["close"] - window_df["vw"]) / window_df["vw"]

        realized_vol = float(log_ret.std())

        features = pd.Series({
            "realized_vol": realized_vol,
            # vol_lag_1/2/3 are NaN — the RF handles them via imputation
            # (replace with a Bigtable side-input if you add lagged vol later)
            "vol_lag_1":    np.nan,
            "vol_lag_2":    np.nan,
            "vol_lag_3":    np.nan,
            "mean_return":  float(log_ret.mean()),
            "abs_return":   float(log_ret.abs().mean()),
            "volume_mean":  float(window_df["volume"].mean()),
            "volume_std":   float(window_df["volume"].std()),
            "n_mean":       float(window_df["n"].mean()),
            "range_mean":   float(price_range.mean()),
            "vwap_mean":    float(vwap_spread.mean()),
        })

        X    = features.values.reshape(1, -1)
        pred = int(self.model.predict(X)[0])
        prob = round(float(self.model.predict_proba(X)[0][1]), 4)

        return {
            "symbol":           "AAPL",
            "window_end":       window_end,
            "prediction":       pred,
            "probability":      prob,
            "label":            "vol_up" if pred == 1 else "vol_down",
            # Context columns: useful for monitoring / feature drift detection
            "realized_vol":     round(realized_vol, 6),
            "trade_count":      int(window_df["trade_count"].sum()),
            "mean_trade_value": round(float(window_df["mean_trade_value"].mean()), 4),
            "price_range_pct":  round(float(price_range.mean()), 6),
            "pipeline_time":    datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f UTC"
            ),
        }


# ---------------------------------------------------------------------------
# Pipeline graph
# ---------------------------------------------------------------------------

def build_pipeline(pipeline: beam.Pipeline) -> None:

    # ── 1. Ingest ──────────────────────────────────────────────────────────
    raw = pipeline | "ReadFromPubSub" >> ReadFromPubSub(subscription=SUBSCRIPTION)

    # ── 2. Parse ───────────────────────────────────────────────────────────
    parsed, parse_dlq = (
        raw
        | "ParseMessages" >> beam.ParDo(ParseMessage()).with_outputs(
            ParseMessage.DLQ, main="parsed"
        )
    )

    # ── 3. Normalise & enrich ──────────────────────────────────────────────
    enriched, enrich_dlq = (
        parsed
        | "NormaliseAndEnrich" >> beam.ParDo(NormaliseAndEnrich()).with_outputs(
            NormaliseAndEnrich.DLQ, main="enriched"
        )
    )

    # ── 4. Write enriched trades to BigQuery ───────────────────────────────
    (
        enriched
        | "WriteTradesТоBigQuery" >> WriteToBigQuery(
            table=BQ_TABLE,
            schema=TRADES_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )

    # ── 5. Dead-letter handling ────────────────────────────────────────────
    if DLQ_TOPIC:
        dlq_messages = (
            (parse_dlq, enrich_dlq)
            | "FlattenDlq"        >> beam.Flatten()
            | "FormatDlqMessages" >> beam.ParDo(FormatDlqMessage())
        )
        dlq_messages | "WriteDlqToPubSub" >> beam.io.WriteToPubSub(topic=DLQ_TOPIC)
    else:
        logging.warning("[DLQ] No DLQ_TOPIC configured — dead letters will be logged only.")

    # ── 6. Volatility prediction branch ────────────────────────────────────
    #
    #  enriched trades
    #      │
    #      ├─ KeyByConstant("AAPL")         ← no per-symbol routing needed
    #      │
    #      ├─ FixedWindows(60 s)            ← 1-minute tumbling windows
    #      │
    #      ├─ GroupByKey                    ← collect all trades in the window
    #      │
    #      ├─ BuildOHLCVBar                 ← one bar per window
    #      │
    #      ├─ VolatilityPredictor           ← queries BQ for last 60 bars
    #      │                                   + RF inference (stateless)
    #      └─ WriteVolPredictionsToBigQuery

    keyed_trades = (
        enriched
        | "KeyByConstant" >> beam.Map(lambda r: ("AAPL", r))
    )

    ohlcv_bars = (
        keyed_trades
        | "WindowIntoMinutes" >> beam.WindowInto(FixedWindows(60))
        | "GroupByKey"        >> beam.GroupByKey()
        | "BuildOHLCVBars"    >> beam.ParDo(BuildOHLCVBar())
    )

    vol_predictions = (
        ohlcv_bars
        | "PredictVolatility" >> beam.ParDo(VolatilityPredictor())
    )

    (
        vol_predictions
        | "WriteVolPredictionsToBigQuery" >> WriteToBigQuery(
            table=VOL_BQ_TABLE,
            schema=VOL_PRED_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        runner="DataflowRunner",
        region="europe-west3",
        machine_type="e2-standard-2",
        temp_location=f"gs://{BUCKET}/tmp",
        staging_location=f"gs://{BUCKET}/staging",
        job_name="aapl-trades-vol-pipeline-v2",
        requirements_file="requirements.txt",   # must include scikit-learn, joblib, pandas, numpy, google-cloud-storage
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
