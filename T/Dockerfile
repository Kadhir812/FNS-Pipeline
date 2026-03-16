# =========================================================
# STAGE 1: BUILDER
# =========================================================
FROM spark:python3 AS builder

USER root
WORKDIR /app

# Install system build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for cache
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code and JARs
COPY Transform.py .
COPY jars/ ./jars/

# Clean up python cache
RUN find /usr/local/lib/python3.*/site-packages -type d -name "__pycache__" -exec rm -rf {} + || true

# =========================================================
# STAGE 2: RUNTIME
# =========================================================
FROM spark:python3 AS runtime

USER root
WORKDIR /app

# Set Environment Variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# 1. Copy installed Python packages from Builder
COPY --from=builder /usr/local/lib/python3.*/site-packages /usr/local/lib/python3.*/site-packages

# 2. Copy application code and JARs
COPY --from=builder /app/Transform.py .
COPY --from=builder /app/jars ./jars

# 3. Fix permissions for spark user
RUN chown -R spark:spark /app

# DO NOT set USER or CMD yet - let base image entrypoint handle it
# The spark:python3 image has an entrypoint that sets up PATH and SPARK_HOME

# Switch to spark user (optional, but recommended for security)
USER spark

# Pass the script as an ARGUMENT to the entrypoint
# The entrypoint will invoke spark-submit with these args
CMD ["Transform.py"]