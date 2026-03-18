#!/bin/bash
set -e

export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export PATH="${SPARK_HOME}/bin:${PATH}"

JAR_DIR="/app/T/jars"

echo "=== DEBUG ==="
echo "SPARK_HOME:   $SPARK_HOME"
echo "spark-submit: $(which spark-submit || echo NOT FOUND)"
echo "Python:       $(python3 --version 2>&1)"
echo "JAR dir contents:"
ls -la "$JAR_DIR" 2>/dev/null || echo "  JAR dir not found or empty"

JARS=$(find "$JAR_DIR" -type f -name "*.jar" 2>/dev/null | sort | paste -sd "," -)

if [ -z "$JARS" ]; then
    echo "WARNING: No jars found in $JAR_DIR — submitting without --jars"
    echo "         Kafka/ES connectors will be unavailable"
else
    echo "JARS resolved:"
    echo "$JARS" | tr ',' '\n' | sed 's/^/  - /'
fi
echo "============="

CMD=(
    spark-submit
    --master "local[*]"
    --conf "spark.driver.extraClassPath=${JARS}"
    --conf "spark.executor.extraClassPath=${JARS}"
)

if [ -n "$JARS" ]; then
    CMD+=(--jars "$JARS")
fi

CMD+=("/app/Transform.py")

echo "Executing: ${CMD[*]}"
exec "${CMD[@]}"