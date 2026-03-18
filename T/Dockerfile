# =========================
# STAGE 1 — BUILDER
# =========================
FROM spark:3.5.0-python3 AS builder

USER root
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ make libssl-dev libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install --no-cache-dir \
    --target=/app/deps \
    -r requirements.txt

COPY Transform.py .

# =========================
# STAGE 2 — RUNTIME
# =========================
FROM spark:3.5.0-python3 AS runtime

USER root
WORKDIR /app

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    SPARK_HOME=/opt/spark \
    PYTHONPATH=/app/deps \
    PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

# ✅ Minimal runtime deps only
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Copy only app code and deps from builder
COPY --from=builder /app/deps /app/deps
COPY --from=builder /app/Transform.py .

# Mount point for volume-managed jars
RUN mkdir -p /app/T/jars

COPY entrypoint.sh /app/entrypoint.sh
RUN sed -i 's/\r//' /app/entrypoint.sh \
    && chmod +x /app/entrypoint.sh \
    && chown -R spark:spark /app

USER spark

VOLUME ["/app/T/jars"]

CMD ["/app/entrypoint.sh"]