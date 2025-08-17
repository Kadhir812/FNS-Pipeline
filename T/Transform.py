from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, when, to_timestamp, coalesce, lit
from pyspark.sql.types import StructType, StringType, StructField, DoubleType
from pyspark.sql.functions import sha2, concat_ws
import re
import os

# === Spark Session ===
JARS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "jars"))
os.environ['HADOOP_OPTS'] = '-Djava.library.path='

spark = SparkSession.builder \
    .appName("NewsSentimentTransform") \
    .config("spark.hadoop.io.nativeio.NativeIO", "false") \
    .config(
        "spark.jars",
        f"{JARS_DIR}/elasticsearch-spark-30_2.12-8.10.2.jar,"
        f"{JARS_DIR}/spark-sql-kafka-0-10_2.12-3.3.0.jar,"
        f"{JARS_DIR}/slf4j-api-1.7.32.jar,"
        f"{JARS_DIR}/kafka-clients-3.0.0.jar,"
        f"{JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.3.0.jar,"
        f"{JARS_DIR}/commons-pool2-2.11.1.jar"
    ) \
    .getOrCreate()

# === Schema ===
schema = StructType([
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("content", StringType()),
    StructField("publishedAt", StringType()),
    StructField("source", StringType()),
    StructField("sentiment", DoubleType()),   # changed
    StructField("confidence", DoubleType()),  # changed
    StructField("risk_score", DoubleType()),  # keep consistent
    StructField("link", StringType()),
    StructField("image_url", StringType()),
    StructField("key_phrases", StringType()),      # <-- enabled
    StructField("category", StringType()),         # <-- enabled
    StructField("summary", StringType()),          # <-- enabled
    StructField("impact_assessment", StringType()),# <-- enabled
    StructField("doc_id", StringType())
])

# === Read Kafka Stream ===
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "Financenews-raw") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .withColumn("data", from_json(col("json"), schema)) \
    .select("data.*")

# === Risk Keywords ===
# Read keywords from file
KEYWORDS_PATH = os.path.join(os.path.dirname(__file__), "risk_keywords.txt")
with open(KEYWORDS_PATH, 'r') as f:
    keywords = [line.strip().lower() for line in f if line.strip()]

# Build condition for any keyword match in content or title
keyword_condition = None
for word in keywords:
    cond = (lower(col("content")).contains(word)) | (lower(col("title")).contains(word))
    keyword_condition = cond if keyword_condition is None else (keyword_condition | cond)

# === Write to Elasticsearch ===
def write_to_es(batch_df, batch_id):
    print(f"Batch {batch_id} received, count: {batch_df.count()}")
    if batch_df.count() == 0:
        print("Batch is empty, skipping.")
        return

    batch_df = batch_df.fillna({'sentiment': '0', 'confidence': '100'})

    enriched_df = batch_df.withColumn(
        "risk_score",
        when(
            (col("sentiment").cast("float") < -0.2) | keyword_condition,
            0.9
        ).when(
            col("sentiment").cast("float") < 0,
            0.7
        ).otherwise(0.1)
    )

    enriched_df = enriched_df.withColumn(
        "risk_score",
        (col("risk_score") * (col("confidence").cast("float") / 100)).cast("float")
    )

    enriched_df = enriched_df.withColumn(
        "doc_id",
        sha2(concat_ws("", col("title"), coalesce(col("publishedAt"), lit("default"))), 256)
    ).withColumn(
        "publishedAt",
        coalesce(
            to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
            to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            to_timestamp(col("publishedAt"), "yyyy-MM-dd HH:mm:ss")
        ).cast("long") * 1000
    )

    # Select all original + new fields for output
    final_df = enriched_df.select(
        "title", "description", "content", "publishedAt", "source",
        "sentiment", "confidence", "risk_score", "key_phrases",
        "category", "summary", "impact_assessment",
        "link", "image_url", "doc_id"
    )

    # === Debug: Show sample batch for new fields ===
    print("Final DataFrame Preview:")
    final_df.select(
        "title", "key_phrases", "category", "summary", "impact_assessment"
    ).show(truncate=False)

    try:
        final_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.nodes.wan.only", "true") \
            .option("es.net.ssl", "false") \
            .option("es.mapping.id", "doc_id") \
            .option("es.write.operation", "upsert") \
            .option("es.index.auto.create", "false") \
            .option("es.resource", "news-analysis") \
            .mode("append") \
            .save()
        print(" Batch written to Elasticsearch")
    except Exception as e:
        print(f"Error writing batch: {e}")
        # Temporary debug output
        final_df.write.mode("overwrite").json("file:///tmp/debug_output")

# === Start Stream ===
query = df_json.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_es) \
    .option("checkpointLocation", "D:/big data pipeline/T/checkpoint") \
    .start()

query.awaitTermination() 
