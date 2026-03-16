# Dockerfile for T (Transform) service using PySpark and custom JARs
FROM spark:python3

USER root
WORKDIR /app

# Copy all code and JARs
COPY . /app

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Set environment variables for PySpark (optional, but explicit)
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# Mount the models directory from host at runtime (do not copy in Dockerfile)
# Example: -v d:/big data pipeline/T/models:/app/models

# Entrypoint: run Transform.py with spark-submit and all required JARs
CMD ["spark-submit", "--jars", "/app/jars/elasticsearch-spark-30_2.12-8.10.2.jar,/app/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar,/app/jars/slf4j-api-1.7.32.jar,/app/jars/kafka-clients-3.0.0.jar,/app/jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar,/app/jars/commons-pool2-2.11.1.jar", "Transform.py"]
