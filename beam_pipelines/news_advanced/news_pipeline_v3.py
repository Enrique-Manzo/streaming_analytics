# news_pipeline.py
# ---------------------------------------------------------------------------
# Dataflow streaming pipeline: Pub/Sub → Parse → Explode → Normalise →
#                              FinBERT Sentiment → Deduplicate → BigQuery
#
# What it does:
#   1. Reads news batch messages from a Pub/Sub subscription
#      (each message is a JSON *array* of news objects)
#   2. Parses the array and explodes it into individual news records
#   3. Normalises types:
#        - time_published  "20260315T184157" → BigQuery TIMESTAMP string
#        - topics[].relevance_score string → float
#   4. Runs FinBERT inference to overwrite overall_sentiment_score /
#      overall_sentiment_label with the model's own predictions.
#      The model is loaded once per worker (lazy singleton) from GCS.
#   5. Deduplicates within a 12-hour session window keyed on (url, title)
#      using Beam's stateful DoFn + a timer-based expiry
#   6. Routes invalid messages to a dead letter Pub/Sub topic
#   7. Writes valid, deduplicated records to BigQuery via streaming inserts
#
# FinBERT inference note:
#   Each Dataflow worker is an independent Python process.  The model is
#   loaded exactly once per worker via the DoFn.setup() lifecycle hook,
#   which Beam calls before the first element is delivered to that worker.
#   This is the idiomatic Beam pattern and avoids the fragility of
#   module-level singletons.
#
#   Artefact layout in GCS (all under gs://<BUCKET>/models/):
#     models/
#       finbert_backbone/        ← ProsusAI/finbert saved via save_pretrained()
#         config.json
#         model.safetensors      (or tf_model.h5)
#         tokenizer.json
#         tokenizer_config.json
#         vocab.txt
#         special_tokens_map.json
#       model_weights.weights.h5 ← your fine-tuned FinBERTRegressor weights
#
#   The backbone is downloaded from GCS using the google-cloud-storage SDK
#   (no gsutil subprocess, no HuggingFace Hub network call on the worker).
#   The tokenizer lives in the same folder as the backbone, so only one
#   GCS directory download is needed.
#
#   The backbone is loaded with from_pt=True so the safetensors/PyTorch
#   checkpoint is converted to TF tensors on the fly — the resulting object
#   is a fully native TF/Keras model, identical to loading a TF checkpoint.
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
import os
import tempfile
import time as _time
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
DLQ_TOPIC    = "projects/tfm-uoc-489523/topics/news-dlq"
BUCKET       = "dataflow-staging-us-central1-476924094843"

# GCS blob paths — all artefacts live under gs://<BUCKET>/models/
BACKBONE_GCS_PREFIX = "models/finbert/model"   # directory prefix (no trailing slash)
WEIGHTS_GCS_BLOB    = "models/news_classifier/model/model_weights.weights.h5"
TOKENIZER_GCS_PREFIX = "models/news_classifier/tokenizer"

# FinBERT hyper-parameters — must match training
MAX_LEN         = 256
DROPOUT_RATE    = 0.2
INFERENCE_BATCH = 32   # articles per tf.data batch during predict()

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


def _download_gcs_directory(bucket_obj, gcs_prefix: str, local_dir: str) -> None:
    """
    Download every blob under gcs_prefix into local_dir, preserving filenames.
    Uses the google-cloud-storage SDK — no gsutil subprocess required.
    """
    blobs = list(bucket_obj.client.list_blobs(bucket_obj, prefix=gcs_prefix + "/"))
    if not blobs:
        raise RuntimeError(
            f"[GCS] No blobs found under gs://{bucket_obj.name}/{gcs_prefix}/ — "
            "check that the backbone was uploaded correctly."
        )
    for blob in blobs:
        filename = os.path.basename(blob.name)
        if not filename:          # skip directory placeholder blobs
            continue
        dest = os.path.join(local_dir, filename)
        blob.download_to_filename(dest)
        logging.info("[GCS] Downloaded %s → %s", blob.name, dest)


# ---------------------------------------------------------------------------
# FinBERT architecture  (must mirror the training definition exactly)
# ---------------------------------------------------------------------------

def _build_finbert_regressor(backbone_local_dir: str, dropout_rate: float = DROPOUT_RATE):
    """
    Rebuild the FinBERTRegressor architecture and load the backbone weights
    from a local directory previously downloaded from GCS.

    Parameters
    ----------
    backbone_local_dir : str
        Path to the local directory containing config.json + model.safetensors
        (or tf_model.h5) saved via save_pretrained().  When the directory
        contains safetensors (PyTorch format), from_pt=True converts them to
        TF tensors on the fly — no PyTorch dependency needed at runtime.
    dropout_rate : float
        Must match the value used during fine-tuning.
    """
    import tensorflow as tf
    from transformers import TFAutoModel

    class FinBERTRegressor(tf.keras.Model):
        def __init__(self, backbone, dropout_rate: float = 0.2, **kwargs):
            super().__init__(**kwargs)
            self.backbone  = backbone
            self.dropout   = tf.keras.layers.Dropout(dropout_rate)
            self.regressor = tf.keras.layers.Dense(1, activation="tanh")

        def call(self, inputs, training=False):
            outputs   = self.backbone(inputs, training=training)
            cls_token = outputs.last_hidden_state[:, 0, :]  # (batch, hidden) — [CLS]
            dropped   = self.dropout(cls_token, training=training)
            return self.regressor(dropped)                  # (batch, 1)

    # Load backbone from local directory — from_pt=True handles safetensors.
    logging.info("[FINBERT] Loading backbone from %s …", backbone_local_dir)
    backbone = TFAutoModel.from_pretrained(backbone_local_dir, from_pt=True)
    model    = FinBERTRegressor(backbone, dropout_rate=dropout_rate)

    # Dummy forward pass to initialise all sub-layer weights before
    # load_weights() is called in setup().
    dummy = {
        "input_ids":      tf.zeros((1, MAX_LEN), dtype=tf.int32),
        "attention_mask": tf.zeros((1, MAX_LEN), dtype=tf.int32),
    }
    _ = model(dummy, training=False)
    logging.info("[FINBERT] Backbone loaded and model initialised.")
    return model


# ---------------------------------------------------------------------------
# FinBERT inference helpers
# ---------------------------------------------------------------------------

def _score_to_label(s: float) -> str:
    """Map a regression score in (-1, 1) to a human-readable sentiment label."""
    if s <= -0.35:
        return "Bearish"
    if s <= -0.15:
        return "Somewhat-Bearish"
    if s <   0.15:
        return "Neutral"
    if s <   0.35:
        return "Somewhat-Bullish"
    return "Bullish"


def _predict_sentiment(texts: list, model, tokenizer) -> tuple:
    """
    Run FinBERT inference on a list of raw text strings.

    Parameters
    ----------
    texts     : list of str — one entry per article (title + " " + summary)
    model     : loaded Keras FinBERTRegressor
    tokenizer : matching HuggingFace tokenizer

    Returns
    -------
    scores : list[float]  — values in (-1, 1), one per article
    labels : list[str]    — bucket label for each score
    """
    import tensorflow as tf

    enc = tokenizer(
        list(texts),
        max_length=MAX_LEN,
        padding="max_length",
        truncation=True,
        return_tensors="tf",
    )

    dataset = tf.data.Dataset.from_tensor_slices({
        "input_ids":      enc["input_ids"],
        "attention_mask": enc["attention_mask"],
    }).batch(INFERENCE_BATCH)

    raw    = model.predict(dataset, verbose=0)
    scores = raw.flatten().tolist()
    labels = [_score_to_label(s) for s in scores]
    return scores, labels


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

    Note: overall_sentiment_score / overall_sentiment_label are kept here so
    the schema is always complete, but they will be overwritten by
    FinBERTInference in the next step.
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
                # Placeholders — FinBERTInference will overwrite these
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


class FinBERTInference(beam.DoFn):
    """
    Overwrite overall_sentiment_score and overall_sentiment_label with
    predictions from the fine-tuned FinBERT regression model.

    Design
    ------
    - setup() is Beam's official per-worker lifecycle hook.  It fires once
      before the first element reaches this worker, downloads the model
      artefacts from GCS via the google-cloud-storage SDK, and stores them
      as instance attributes.  All subsequent elements in that worker reuse
      self.model / self.tokenizer without any extra GCS traffic.
    - The backbone directory (finbert_backbone/) and the tokenizer files are
      stored together in GCS under the same prefix.  A single directory
      download covers both — no separate tokenizer path needed.
    - The backbone is loaded with from_pt=True so that a safetensors
      checkpoint is transparently converted to TF tensors on load.
    - Input text is title + " " + summary, matching the pre-processing used
      during fine-tuning.
    - Fail-open: if inference raises for any reason the record is passed
      through with its upstream sentiment values intact and the error is
      logged, so the pipeline never stalls on a single bad article.
    """

    def setup(self):
        """Download artefacts from GCS and load model — runs once per worker."""
        from google.cloud import storage
        from transformers import AutoTokenizer

        # ── Local cache paths ─────────────────────────────────────────────
        local_dir = os.path.join(tempfile.gettempdir(), "finbert_worker_cache")
        backbone_local = os.path.join(local_dir, "finbert_backbone")
        weights_local = os.path.join(local_dir, "model_weights.weights.h5")
        tokenizer_local = os.path.join(local_dir, "tokenizer")
        os.makedirs(backbone_local, exist_ok=True)
        os.makedirs(tokenizer_local, exist_ok=True)

        # ── GCS client ────────────────────────────────────────────────────
        gcs_client = storage.Client()
        bucket     = gcs_client.bucket(BUCKET)

        # ── Download backbone (skip if already cached) ────────
        # config.json is always present in a valid save_pretrained() output;
        # its absence means the local directory is empty or incomplete.
        if not os.path.exists(os.path.join(backbone_local, "config.json")):
            logging.info(
                "[FINBERT] Downloading backbone from gs://%s/%s …",
                BUCKET, BACKBONE_GCS_PREFIX,
            )
            _download_gcs_directory(bucket, BACKBONE_GCS_PREFIX, backbone_local)
            logging.info("[FINBERT] Backbone downloaded → %s", backbone_local)
        else:
            logging.info("[FINBERT] Backbone already cached at %s", backbone_local)

        # ── Download fine-tuned weights (skip if already cached) ──────────
        if not os.path.exists(weights_local):
            logging.info(
                "[FINBERT] Downloading fine-tuned weights from gs://%s/%s …",
                BUCKET, WEIGHTS_GCS_BLOB,
            )
            bucket.blob(WEIGHTS_GCS_BLOB).download_to_filename(weights_local)
            logging.info("[FINBERT] Weights downloaded → %s", weights_local)
        else:
            logging.info("[FINBERT] Weights already cached at %s", weights_local)

        # ── Download tokenizer (skip if already cached) ───────────────────
        if not os.path.exists(os.path.join(tokenizer_local, "tokenizer_config.json")):
            logging.info(
                "[FINBERT] Downloading tokenizer from gs://%s/%s …",
                BUCKET, TOKENIZER_GCS_PREFIX,
            )
            _download_gcs_directory(bucket, TOKENIZER_GCS_PREFIX, tokenizer_local)
            logging.info("[FINBERT] Tokenizer downloaded → %s", tokenizer_local)
        else:
            logging.info("[FINBERT] Tokenizer already cached at %s", tokenizer_local)

        # ── Build model and overlay fine-tuned weights ────────────────────
        self.model = _build_finbert_regressor(backbone_local)
        self.model.load_weights(weights_local)
        logging.info("[FINBERT] Fine-tuned weights loaded.")

        # ── Load tokenizer from the same local backbone directory ─────────
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_local)
        logging.info("[FINBERT] Tokenizer loaded.  Worker ready.")

    def process(self, record: dict, *args, **kwargs):
        try:
            title   = record.get("title")   or ""
            summary = record.get("summary") or ""
            text    = f"{title} {summary}".strip()

            scores, labels = _predict_sentiment([text], self.model, self.tokenizer)

            record = dict(record)   # shallow copy — avoid mutating upstream
            record["overall_sentiment_score"] = float(scores[0])
            record["overall_sentiment_label"] = labels[0]

        except Exception as exc:
            # Fail-open: log and keep whatever upstream provided
            logging.error(
                "[FINBERT ERROR] Inference failed for url=%s | %s",
                record.get("url", "?"), exc,
            )

        yield record


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

    SEEN_STATE   = BagStateSpec("seen", VarIntCoder())
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

        already_seen = any(True for _ in seen.read())

        if not already_seen:
            seen.add(1)
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

    # ── 4. FinBERT sentiment inference ────────────────────────────────────
    scored = (
        normalised
        | "FinBERTInference" >> beam.ParDo(FinBERTInference())
    )

    # ── 5. Deduplicate within 12-hour session window ──────────────────────
    deduped = (
        scored
        | "KeyByDedupHash" >> beam.Map(lambda r: (_dedup_key(r), r))
        | "SessionWindow"  >> beam.WindowInto(
            beam.window.Sessions(gap_size=DEDUP_WINDOW_SECONDS)
        )
        | "DeduplicateNews" >> beam.ParDo(DeduplicateNews())
    )

    # ── 6. Write to BigQuery ──────────────────────────────────────────────
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

    # ── 7. Dead letter routing ────────────────────────────────────────────
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
        region="europe-west3",
        temp_location=f"gs://{BUCKET}/tmp",
        staging_location=f"gs://{BUCKET}/staging",
        job_name="financial-news-pipeline",
        requirements_file="requirements.txt",
        machine_type="n1-standard-4",   # 15 GB RAM — required for FinBERT load
        disk_size_gb=50,                 # backbone + weights need local disk space
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
