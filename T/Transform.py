from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, when, to_timestamp, coalesce, lit, log
from pyspark.sql.types import StructType, StringType, StructField, DoubleType
from pyspark.sql.functions import sha2, concat_ws
import re
import os
import shutil

# Clear checkpoint directory to avoid any stale state
CHECKPOINT_PATH = "D:/big data pipeline/T/checkpoint"
if os.path.exists(CHECKPOINT_PATH):
    try:
        shutil.rmtree(CHECKPOINT_PATH)
        print("Checkpoint directory cleared")
    except Exception as e:
        print(f"Could not clear checkpoint: {str(e)}")

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
    StructField("sentiment", DoubleType()),   
    StructField("confidence", DoubleType()),  
    StructField("risk_score", DoubleType()),
    StructField("link", StringType()),
    StructField("image_url", StringType()),
    StructField("key_phrases", StringType()),
    StructField("category", StringType()),
    StructField("summary", StringType()),
    StructField("impact_assessment", StringType()),
    StructField("symbol", StringType()),
    StructField("entity_name", StringType()),
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

# === Read risk keywords from file ===
KEYWORDS_PATH = os.path.join(os.path.dirname(__file__), "risk_keywords.txt")
try:
    with open(KEYWORDS_PATH, 'r') as f:
        keywords = [line.strip().lower() for line in f if line.strip()]
except:
    keywords = ["risk", "loss", "default", "bankruptcy", "crash"]
    print(f"Warning: Could not load risk keywords, using default list")

# Build condition for any keyword match in content or title
keyword_condition = None
for word in keywords:
    cond = (lower(col("content")).contains(word)) | (lower(col("title")).contains(word))
    keyword_condition = cond if keyword_condition is None else (keyword_condition | cond)

# === Map sentiment to impact assessment directly in SQL expressions ===
def write_to_es(batch_df, batch_id):
    try:
        if batch_df.count() == 0:
            print("Batch is empty, skipping.")
            return

        print(f"Processing batch {batch_id} with {batch_df.count()} records")

        # Fill in nulls to avoid errors
        batch_df = batch_df.fillna({
            'sentiment': 0.0, 
            'confidence': 0.0, 
            'content': '', 
            'description': '', 
            'title': '',
            'risk_score': 0.0,
            'impact_assessment': 'NEUTRAL',
            'symbol': ''
        })

        # CHANGE 1: Improved confidence normalization using logarithmic scaling
        # This prevents saturation and maintains resolution across the confidence range
        max_conf = 200  # Tunable parameter based on your data distribution
        from pyspark.sql.functions import log

        enriched_df = batch_df.withColumn(
            "conf_norm",
            when(col("confidence").isNull() | (col("confidence") <= 0), 0.0)
            .otherwise(log(1 + col("confidence")) / log(1 + lit(max_conf)))
        )

        # CHANGE 2: Calculate risk level as a categorical first
        enriched_df = enriched_df.withColumn(
            "risk_level",
            when(
                (col("sentiment").cast("float") < -0.2) | keyword_condition,
                lit("high_risk")
            ).when(
                col("sentiment").cast("float") < 0,
                lit("moderate_risk")
            ).otherwise(lit("low_risk"))
        )

        # CHANGE 3: Convert risk level to numeric base score
        enriched_df = enriched_df.withColumn(
            "risk_base",
            when(col("risk_level") == "high_risk", 0.9)
            .when(col("risk_level") == "moderate_risk", 0.7)
            .otherwise(0.1)
        )

        # CHANGE 4: Calculate final risk_score with hybrid adjustment
        enriched_df = enriched_df.withColumn(
            "risk_score",
            (col("risk_base") * 0.8) + (col("risk_base") * col("conf_norm") * 0.2)
        )

        # Store both raw and adjusted risk scores for analysis
        enriched_df = enriched_df.withColumn("risk_raw", col("risk_base"))
        enriched_df = enriched_df.withColumn("risk_adj", col("risk_score"))

        # CHANGE 5: Generate unique doc_id using symbol+source+title to prevent duplicates
        enriched_df = enriched_df.withColumn(
            "doc_id",
            sha2(concat_ws("_", 
                          coalesce(col("symbol"), lit("")), 
                          coalesce(col("source"), lit("")),
                          col("title"), 
                          coalesce(col("publishedAt"), lit("default"))), 256)
        ).withColumn(
            "publishedAt",
            coalesce(
                to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
                to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col("publishedAt"), "yyyy-MM-dd HH:mm:ss")
            ).cast("long") * 1000
        )

        # CHANGE 6: Add a config flag to control prioritization
        # Set to true if risk should override sentiment (even if bullish)
        risk_priority = True  

        # Refined impact assessment with five sentiment tiers plus risk overlay
        if risk_priority:
            # Risk takes priority - original ordering
            enriched_df = enriched_df.withColumn(
                "impact_assessment",
                when(col("risk_raw") >= 0.8, "HIGH RISK")
                .when(col("risk_raw") >= 0.5, "MODERATE RISK")
                .when(col("confidence") < 20, "NEUTRAL")  # Was UNCERTAIN, now NEUTRAL
                .when(col("sentiment") >= 0.7, "BULLISH")
                .when((col("sentiment") >= 0.4) & (col("sentiment") < 0.7), "POSITIVE")
                .when((col("sentiment") >= 0.1) & (col("sentiment") < 0.4), "SLIGHTLY POSITIVE")
                .when((col("sentiment") <= -0.7), "BEARISH")
                .when((col("sentiment") <= -0.4) & (col("sentiment") > -0.7), "NEGATIVE")
                .when((col("sentiment") <= -0.1) & (col("sentiment") > -0.4), "SLIGHTLY NEGATIVE")
                .otherwise("NEUTRAL")
            )
        else:
            # Sentiment-first evaluation with risk as secondary factor
            enriched_df = enriched_df.withColumn(
                "impact_assessment",
                when(col("confidence") < 20, "NEUTRAL")  # Was UNCERTAIN, now NEUTRAL
                .when(col("sentiment") >= 0.7, "BULLISH")
                .when((col("sentiment") >= 0.4) & (col("sentiment") < 0.7), "POSITIVE")
                .when((col("sentiment") >= 0.1) & (col("sentiment") < 0.4), "SLIGHTLY POSITIVE")
                .when(col("sentiment") <= -0.7, "BEARISH")
                .when((col("sentiment") <= -0.4) & (col("sentiment") > -0.7), "NEGATIVE")
                .when((col("sentiment") <= -0.1) & (col("sentiment") > -0.4), "SLIGHTLY NEGATIVE")
                .when(col("risk_raw") >= 0.8, "HIGH RISK")
                .when(col("risk_raw") >= 0.5, "MODERATE RISK")
                .otherwise("NEUTRAL")
            )

        # CHANGE 7: Updated impact_score mapping with more granular values
        enriched_df = enriched_df.withColumn(
            "impact_score",
            when(col("impact_assessment") == "BULLISH", 2.0)
            .when(col("impact_assessment") == "POSITIVE", 1.0)
            .when(col("impact_assessment") == "SLIGHTLY POSITIVE", 0.5)
            .when(col("impact_assessment") == "NEUTRAL", 0.0)
            .when(col("impact_assessment") == "SLIGHTLY NEGATIVE", -0.5)
            .when(col("impact_assessment") == "NEGATIVE", -1.0)
            .when(col("impact_assessment") == "BEARISH", -2.0)
            .when(col("impact_assessment") == "HIGH RISK", -3.0)
            .when(col("impact_assessment") == "MODERATE RISK", -1.5)
            .otherwise(0.0)
        )

        # CHANGE 8: Improved final_score formula - confidence affects sentiment, not risk
        risk_weight = 2.0  # Tunable parameter
        enriched_df = enriched_df.withColumn(
            "final_score",
            (col("impact_score") * col("conf_norm")) - (col("risk_raw") * risk_weight)
        )

        # CHANGE 9: Add sentiment_confidence_score for analysis
        enriched_df = enriched_df.withColumn(
            "sentiment_confidence_score", 
            col("sentiment") * col("conf_norm")
        )
        
        # Select fields for output - including both raw and adjusted risk scores
        final_df = enriched_df.select(
            "title", "description", "content", "publishedAt", "source",
            "sentiment", "confidence", "conf_norm", "risk_level", "risk_raw", "risk_adj", "key_phrases",
            "category", "summary", "impact_assessment", "impact_score", "final_score", 
            "sentiment_confidence_score", "symbol", "entity_name", "link", "image_url", "doc_id"
        )

        # Test Elasticsearch connection
        import socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect(("localhost", 9200))
            s.close()
            print("Elasticsearch port is reachable")
        except Exception as e:
            print(f"ERROR: Cannot connect to Elasticsearch: {e}")
            return

        # DEBUG: Print sample row for inspection
        try:
            sample_row = final_df.limit(1)
            sample_row_dict = sample_row.toPandas().to_dict('records')
            if sample_row_dict:
                print("Sample row for debugging:")
                for k, v in sample_row_dict[0].items():
                    if isinstance(v, str) and len(v) > 50:
                        print(f"  {k}: {v[:50]}...")
                    else:
                        print(f"  {k}: {v}")
        except Exception as e:
            print(f"Could not print sample data: {e}")

        # Write to Elasticsearch with more resilient options
        try:
            print("Writing batch to Elasticsearch...")
            final_df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "localhost") \
                .option("es.port", "9200") \
                .option("es.nodes.wan.only", "true") \
                .option("es.net.ssl", "false") \
                .option("es.mapping.id", "doc_id") \
                .option("es.write.operation", "upsert") \
                .option("es.index.auto.create", "true") \
                .option("es.resource", "news-analysis") \
                .option("es.batch.size.entries", "100") \
                .option("es.batch.write.retry.count", "3") \
                .option("es.http.timeout", "60s") \
                .option("es.mapping.date.rich", "false") \
                .mode("append") \
                .save()
            print("Batch successfully written to Elasticsearch")
        except Exception as e:
            import traceback
            print(f"ERROR writing to Elasticsearch: {e}")
            traceback.print_exc()
    except Exception as e:
        import traceback
        print(f"ERROR in write_to_es: {e}")
        traceback.print_exc()

# === Start Stream ===
query = df_json.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_es) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

# Create direct index with Python instead of using the Spark connector
import requests
import json
try:
    # Check if index exists
    index_exists = requests.head("http://localhost:9200/news-analysis")
    
    if index_exists.status_code == 404:
        print("Creating Elasticsearch index manually...")
        # Create index with mappings
        index_mappings = {
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "description": {"type": "text"},
                    "content": {"type": "text"},
                    "publishedAt": {"type": "date", "format": "epoch_millis"},
                    "source": {"type": "keyword"},
                    "sentiment": {"type": "float"},
                    "confidence": {"type": "float"},
                    "conf_norm": {"type": "float"},
                    "risk_level": {"type": "keyword"},
                    "risk_raw": {"type": "float"},
                    "risk_adj": {"type": "float"},
                    "key_phrases": {"type": "text"},
                    "category": {"type": "keyword"},
                    "summary": {"type": "text"},
                    "impact_assessment": {"type": "keyword"},
                    "impact_score": {"type": "float"},
                    "final_score": {"type": "float"},
                    "sentiment_confidence_score": {"type": "float"},
                    "symbol": {"type": "keyword"},
                    "entity_name": {"type": "keyword"},
                    "link": {"type": "keyword"},
                    "image_url": {"type": "keyword"},
                    "doc_id": {"type": "keyword"}
                }
            }
        }
        
        create_response = requests.put(
            "http://localhost:9200/news-analysis",
            data=json.dumps(index_mappings),
            headers={"Content-Type": "application/json"}
        )
        print(f"Index creation response: {create_response.status_code}")
        print(f"Response text: {create_response.text}")
    else:
        print(f"Index already exists: status code {index_exists.status_code}")
except Exception as e:
    print(f"Could not create Elasticsearch index: {e}")

print("Streaming query started, awaiting termination...")
query.awaitTermination()