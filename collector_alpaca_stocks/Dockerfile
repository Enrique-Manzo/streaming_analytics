# Dockerfile
# ---------------------------------------------------------------------------
# GCP Cloud Run / VM compatible image for the Alpaca stock collector.
#
# Build:  docker build -t alpaca-collector .
# Run:    docker run -p 8000:8000 --env-file .env alpaca-collector
# ---------------------------------------------------------------------------

# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: lean runtime image ───────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Non-root user for security best practice
RUN useradd --no-create-home --shell /bin/false collector

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY config.py contract.py observability.py ws_collector.py main.py ./

# .env is intentionally NOT copied — inject credentials at runtime via
# environment variables or a secrets manager (e.g. GCP Secret Manager).

ENV PORT=8000 \
    HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER collector

EXPOSE 8000

CMD ["sh", "-c", "python main.py"]
