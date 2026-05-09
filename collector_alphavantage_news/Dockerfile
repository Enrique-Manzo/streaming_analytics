# Dockerfile
# ---------------------------------------------------------------------------
# Cloud Run Job image for the Alpha Vantage news collector.
# This is a short-lived batch container — no server port is exposed.
#
# Build:  docker build -t news-collector .
# Run:    docker run --env-file .env news-collector
# ---------------------------------------------------------------------------

# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: lean runtime image ───────────────────────────────────────────────
FROM python:3.12-slim AS runtime

RUN useradd --no-create-home --shell /bin/false collector

WORKDIR /app

COPY --from=builder /install /usr/local

# Copy application source owned by the runtime user — no .env (inject credentials at runtime)
COPY --chown=collector:collector config.py contract.py observability.py news_collector.py main.py ./

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER collector

CMD ["python", "main.py"]