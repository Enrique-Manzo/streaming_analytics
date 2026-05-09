# Dockerfile
# ---------------------------------------------------------------------------
# Cloud Run Jobs compatible image for the Coinbase trade collector.
#
# Build:  docker build -t coinbase-collector .
# Run:    docker run -p 8000:8000 coinbase-collector
# ---------------------------------------------------------------------------

# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install dependencies into an isolated directory so we can copy only them
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

# Cloud Run injects PORT via environment variable; default to 8000
ENV PORT=8000 \
    HOST=0.0.0.0 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER collector

EXPOSE 8080

# Cloud Run Jobs: no --reload, single worker, honours $PORT at runtime
CMD ["sh", "-c", "python main.py"]
