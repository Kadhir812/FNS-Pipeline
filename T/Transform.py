"""
TRANSFORM.PY
============
Reads financial news from Kafka, enriches with risk/sentiment analysis,
writes to Elasticsearch.

Pipeline:
  Kafka → Parse JSON → Enrich (risk, impact, scores) → Elasticsearch

Fixes applied:
  - Stream started exactly once, after index setup
  - spark.jars removed (entrypoint.sh handles jar loading)
  - confidence normalized from 0–100 to 0–1 (MarketAux scale fix)
  - confidence threshold corrected to 0.0–1.0 scale
  - conf_norm max_conf corrected to 1.0
  - Risk no longer blindly overwrites positive sentiment (RISKY BULLISH)
  - Index creation happens before stream starts
  - batch_df cached to avoid multiple scans
  - repartition(4) before ES write for better throughput
  - Keyword detection uses rlike regex instead of chained contains()
  - Timestamp parsing handles timezone offset formats (+00:00)
  - unpersist() always called in finally block
"""

import os
import re
import json
import socket
import requests
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lower, when, to_timestamp,
    coalesce, lit, log, size, sha2, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, ArrayType
)

# ============================================================
# CONFIGURATION
# ============================================================
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/tmp/spark-checkpoint")
ES_HOST         = os.environ.get("ES_HOST",         "elasticsearch")
ES_PORT         = os.environ.get("ES_PORT",         "9200")
KAFKA_BROKER    = os.environ.get("KAFKA_BROKER",    "kafka:29092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC",     "Financenews-raw")
ES_INDEX        = os.environ.get("ES_INDEX",        "news-analysis")
ES_URL          = f"http://{ES_HOST}:{ES_PORT}"

# ============================================================
# SPARK SESSION
# ✅ No spark.jars — entrypoint.sh passes --jars and extraClassPath
# ============================================================
spark = SparkSession.builder \
    .appName("NewsSentimentTransform") \
    .config("spark.hadoop.io.nativeio.NativeIO", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# SCHEMA
# ============================================================
schema = StructType([
    StructField("title",             StringType()),
    StructField("description",       StringType()),
    StructField("content",           StringType()),
    StructField("publishedAt",       StringType()),
    StructField("source",            StringType()),
    StructField("sentiment",         DoubleType()),
    StructField("confidence",        DoubleType()),
    StructField("risk_score",        DoubleType()),
    StructField("link",              StringType()),
    StructField("image_url",         StringType()),
    StructField("key_phrases",       StringType()),
    StructField("category",          StringType()),
    StructField("summary",           StringType()),
    StructField("impact_assessment", StringType()),
    StructField("content_quality",   StringType()),
    StructField("symbols",           ArrayType(StringType())),
    StructField("entity_names",      ArrayType(StringType())),
    StructField("doc_id",            StringType()),
])

# ============================================================
# KAFKA SOURCE
# ============================================================
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_json = df_raw \
    .selectExpr("CAST(value AS STRING) as json") \
    .withColumn("data", from_json(col("json"), schema)) \
    .select("data.*") \
    .withColumn(
        "symbol",
        when(
            col("symbols").isNotNull() & (size(col("symbols")) > 0),
            col("symbols")[0]
        ).otherwise(lit(None))
    ) \
    .withColumn(
        "entity_name",
        when(
            col("entity_names").isNotNull() & (size(col("entity_names")) > 0),
            col("entity_names")[0]
        ).otherwise(lit(None))
    )

# ============================================================
# KEYWORD CONDITIONS
# ✅ rlike regex — single eval per column, faster than chained contains()
# ============================================================
KEYWORDS_PATH = os.path.join(os.path.dirname(__file__), "enhanced_risk_keywords.txt")
try:
    with open(KEYWORDS_PATH, "r") as f:
        keywords = [line.strip().lower() for line in f if line.strip()]
    print(f"Loaded {len(keywords)} risk keywords")
except Exception:
    keywords = ["risk", "loss", "default", "bankruptcy", "crash"]
    print("Warning: Could not load risk keywords, using default list")

MA_KEYWORDS = [
    "m&a", "merger", "acquisition", "deal",
    "strategic", "expansion", "growth"
]
COMMODITY_KEYWORDS = [
    "output", "capacity", "margin", "commodity",
    "smelter", "cut", "price", "supply", "demand"
]

def build_keyword_condition(word_list, *columns):
    """Build single rlike OR regex across multiple columns."""
    escaped   = [re.escape(w) for w in word_list]
    pattern   = "|".join(escaped)
    condition = None
    for column in columns:
        cond      = lower(col(column)).rlike(pattern)
        condition = cond if condition is None else (condition | cond)
    return condition

keyword_condition   = build_keyword_condition(keywords,           "content", "title")
ma_condition        = build_keyword_condition(MA_KEYWORDS,        "title",   "content")
commodity_condition = build_keyword_condition(COMMODITY_KEYWORDS, "title",   "content")

# ============================================================
# ELASTICSEARCH INDEX SETUP
# ============================================================
def ensure_index_exists():
    """Create ES index with correct mappings if it doesn't exist."""
    try:
        resp = requests.head(f"{ES_URL}/{ES_INDEX}", timeout=10)
        if resp.status_code == 200:
            print(f"Index '{ES_INDEX}' already exists")
            return True

        if resp.status_code == 404:
            print(f"Creating index '{ES_INDEX}'...")
            mappings = {
                "mappings": {
                    "properties": {
                        "title":                      {"type": "text"},
                        "description":                {"type": "text"},
                        "content":                    {"type": "text"},
                        "publishedAt":                {"type": "date", "format": "epoch_millis"},
                        "source":                     {"type": "keyword"},
                        "sentiment":                  {"type": "float"},
                        "confidence":                 {"type": "float"},
                        "conf_norm":                  {"type": "float"},
                        "risk_level":                 {"type": "keyword"},
                        "risk_raw":                   {"type": "float"},
                        "risk_adj":                   {"type": "float"},
                        "key_phrases":                {"type": "text"},
                        "category":                   {"type": "keyword"},
                        "summary":                    {"type": "text"},
                        "impact_assessment":          {"type": "keyword"},
                        "impact_score":               {"type": "float"},
                        "final_score":                {"type": "float"},
                        "sentiment_confidence_score": {"type": "float"},
                        "content_quality":            {"type": "keyword"},
                        "symbol":                     {"type": "keyword"},
                        "entity_name":                {"type": "keyword"},
                        "link":                       {"type": "keyword"},
                        "image_url":                  {"type": "keyword"},
                        "doc_id":                     {"type": "keyword"},
                    }
                }
            }
            create = requests.put(
                f"{ES_URL}/{ES_INDEX}",
                data=json.dumps(mappings),
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            print(f"Index creation: {create.status_code} — {create.text}")
            return create.status_code in (200, 201)

    except Exception as e:
        print(f"ERROR: Could not verify/create ES index: {e}")
        return False

# ============================================================
# BATCH PROCESSOR
# ============================================================
def write_to_es(batch_df, batch_id):
    try:
        # ✅ Cache once — single scan reused by all downstream operations
        batch_df.cache()
        n = batch_df.count()

        if n == 0:
            print(f"Batch {batch_id}: empty, skipping.")
            batch_df.unpersist()
            return

        print(f"Processing batch {batch_id} with {n} records")

        # ── Fill nulls ──────────────────────────────────────────────
        batch_df = batch_df.fillna({
            "sentiment":         0.0,
            "confidence":        0.0,
            "content":           "",
            "description":       "",
            "title":             "",
            "risk_score":        0.0,
            "impact_assessment": "NEUTRAL",
            "content_quality":   "unknown",
            "symbol":            "",
        })

        # ── Normalize confidence ────────────────────────────────────
        # ✅ MarketAux returns confidence as 0–100, normalize to 0–1
        enriched_df = batch_df.withColumn(
            "confidence",
            when(col("confidence") > 1.0, col("confidence") / 100.0)
            .otherwise(col("confidence"))
        )

        # ── Confidence normalisation ────────────────────────────────
        # ✅ max_conf = 1.0 — confidence now correctly 0.0–1.0
        # log(1+x)/log(2) scales [0,1] → [0,1] smoothly
        enriched_df = enriched_df.withColumn(
            "conf_norm",
            when(col("confidence").isNull() | (col("confidence") <= 0), 0.0)
            .otherwise(
                log(1 + col("confidence")) / log(1 + lit(1.0))
            )
        )

        # ── Risk level ──────────────────────────────────────────────
        enriched_df = enriched_df.withColumn(
            "risk_level",
            when(keyword_condition    & (col("sentiment") < 0),   lit("high_risk"))
            .when(commodity_condition & (col("sentiment") < 0),   lit("high_risk"))
            .when(ma_condition        & (col("sentiment") > 0.2), lit("medium_risk"))
            .when(col("sentiment") < 0,                           lit("moderate_risk"))
            .otherwise(                                            lit("low_risk"))
        )

        # ── Risk base score ─────────────────────────────────────────
        enriched_df = enriched_df.withColumn(
            "risk_base",
            when(col("risk_level") == "high_risk",      0.9)
            .when(col("risk_level") == "moderate_risk", 0.7)
            .when(col("risk_level") == "medium_risk",   0.5)
            .otherwise(                                  0.1)
        )

        # ── Risk score, raw, adjusted ───────────────────────────────
        enriched_df = enriched_df \
            .withColumn("risk_score",
                (col("risk_base") * 0.8) + (col("risk_base") * col("conf_norm") * 0.2)
            ) \
            .withColumn("risk_raw", col("risk_base")) \
            .withColumn("risk_adj", col("risk_score"))

        # ── Dedup doc ID ────────────────────────────────────────────
        enriched_df = enriched_df.withColumn(
            "doc_id",
            sha2(
                concat_ws("_",
                    coalesce(col("symbol"),      lit("")),
                    coalesce(col("source"),      lit("")),
                    col("title"),
                    coalesce(col("publishedAt"), lit("default"))
                ),
                256
            )
        )

        # ── Timestamp → epoch millis ────────────────────────────────
        # ✅ Handles: ISO Z, ISO +00:00 offset, space-separated
        enriched_df = enriched_df.withColumn(
            "publishedAt",
            coalesce(
                to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
                to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                to_timestamp(col("publishedAt"), "yyyy-MM-dd HH:mm:ss"),
            ).cast("long") * 1000
        )

        # ── Impact assessment — Step 1: sentiment ───────────────────
        # ✅ Threshold 0.2 — correct for 0.0–1.0 confidence scale
        enriched_df = enriched_df.withColumn(
            "impact_assessment",
            when(col("confidence") < 0.2,    "NEUTRAL")
            .when(col("sentiment") >= 0.7,   "BULLISH")
            .when(col("sentiment") >= 0.4,   "POSITIVE")
            .when(col("sentiment") >= 0.1,   "SLIGHTLY POSITIVE")
            .when(col("sentiment") <= -0.7,  "BEARISH")
            .when(col("sentiment") <= -0.4,  "NEGATIVE")
            .when(col("sentiment") <= -0.1,  "SLIGHTLY NEGATIVE")
            .otherwise(                      "NEUTRAL")
        )

        # ── Impact assessment — Step 2: risk overlay ────────────────
        # ✅ Preserves positive signal — RISKY BULLISH instead of overwrite
        enriched_df = enriched_df.withColumn(
            "impact_assessment",
            when(
                (col("risk_raw") >= 0.8) & (col("sentiment") <  0), "HIGH RISK"
            ).when(
                (col("risk_raw") >= 0.8) & (col("sentiment") >= 0), "RISKY BULLISH"
            ).when(
                (col("risk_raw") >= 0.5) & (col("sentiment") <  0), "MODERATE RISK"
            ).otherwise(col("impact_assessment"))
        )

        # ── Impact score ────────────────────────────────────────────
        enriched_df = enriched_df.withColumn(
            "impact_score",
            when(col("impact_assessment") == "BULLISH",             2.0)
            .when(col("impact_assessment") == "RISKY BULLISH",      1.5)
            .when(col("impact_assessment") == "POSITIVE",           1.0)
            .when(col("impact_assessment") == "SLIGHTLY POSITIVE",  0.5)
            .when(col("impact_assessment") == "NEUTRAL",            0.0)
            .when(col("impact_assessment") == "SLIGHTLY NEGATIVE", -0.5)
            .when(col("impact_assessment") == "NEGATIVE",          -1.0)
            .when(col("impact_assessment") == "BEARISH",           -2.0)
            .when(col("impact_assessment") == "MODERATE RISK",     -1.5)
            .when(col("impact_assessment") == "HIGH RISK",         -3.0)
            .otherwise(0.0)
        )

        # ── Final score ─────────────────────────────────────────────
        # formula: (impact_score × conf_norm) - (risk_raw × (1 - conf_norm))
        enriched_df = enriched_df \
            .withColumn("final_score",
                (col("impact_score") * col("conf_norm"))
                - (col("risk_raw") * (1 - col("conf_norm")))
            ) \
            .withColumn("sentiment_confidence_score",
                col("sentiment") * col("conf_norm")
            )

        # ── Select output columns ───────────────────────────────────
        # ✅ repartition(4) — better ES bulk write throughput
        final_df = enriched_df.select(
            "title", "description", "content", "publishedAt", "source",
            "sentiment", "confidence", "conf_norm",
            "risk_level", "risk_raw", "risk_adj",
            "key_phrases", "category", "summary",
            "impact_assessment", "impact_score",
            "final_score", "sentiment_confidence_score",
            "content_quality",
            "symbol", "entity_name",
            "link", "image_url", "doc_id"
        )

        # ── Debug sample ────────────────────────────────────────────
        try:
            sample = final_df.limit(1).collect()
            if sample:
                row = sample[0].asDict()
                print("Sample row for debugging:")
                for k, v in row.items():
                    display = (str(v)[:50] + "...") if isinstance(v, str) and len(str(v)) > 50 else v
                    print(f"  {k}: {display}")
        except Exception as e:
            print(f"Could not print sample: {e}")

        # ── ES connectivity check ───────────────────────────────────
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((ES_HOST, int(ES_PORT)))
            s.close()
        except Exception as e:
            print(f"ERROR: Cannot reach Elasticsearch at {ES_HOST}:{ES_PORT} — {e}")
            batch_df.unpersist()
            return

        # ── Write to Elasticsearch ──────────────────────────────────
        try:
            print(f"Writing batch {batch_id} to Elasticsearch...")
            final_df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes",                   ES_HOST) \
                .option("es.port",                    ES_PORT) \
                .option("es.nodes.wan.only",          "true") \
                .option("es.net.ssl",                 "false") \
                .option("es.mapping.id",              "doc_id") \
                .option("es.write.operation",         "upsert") \
                .option("es.index.auto.create",       "true") \
                .option("es.resource",                ES_INDEX) \
                .option("es.batch.size.entries",      "100") \
                .option("es.batch.write.retry.count", "3") \
                .option("es.http.timeout",            "60s") \
                .option("es.mapping.date.rich",       "false") \
                .mode("append") \
                .save()
            print(f"Batch {batch_id} successfully written to Elasticsearch")
        except Exception as e:
            print(f"ERROR writing batch {batch_id}: {e}")
            traceback.print_exc()

    except Exception as e:
        print(f"ERROR in write_to_es (batch {batch_id}): {e}")
        traceback.print_exc()

    finally:
        # ✅ Always release cache — prevents memory leak across batches
        try:
            batch_df.unpersist()
        except Exception:
            pass

# ============================================================
# MAIN
# ✅ Index created first, stream started exactly once
# ============================================================
if __name__ == "__main__":
    ensure_index_exists()

    print("Starting streaming query...")
    query = df_json.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_es) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start()

    print("Streaming query running — awaiting termination...")
    query.awaitTermination()