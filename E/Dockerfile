# ---------------------------------------------------------
# Stage 1: Builder
# ---------------------------------------------------------
FROM python:3.10-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY requirements.txt .

RUN uv pip install \
    --index-strategy unsafe-best-match \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    --target /opt/venv \
    --no-cache \
    -r requirements.txt

# ---------------------------------------------------------
# Stage 2: Runtime
# ---------------------------------------------------------
FROM python:3.10-slim

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

ENV PYTHONPATH="/opt/venv:/app" \
    PYTHONUNBUFFERED=1

COPY . .

RUN useradd -m -u 1000 appuser \
    && mkdir -p /app/logs \
    && chown -R appuser /app

USER appuser

CMD ["python", "Extract.py"]