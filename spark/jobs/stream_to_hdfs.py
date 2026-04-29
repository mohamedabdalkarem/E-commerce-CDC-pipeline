"""
Spark Batch Job: Kafka → HDFS (Parquet)
----------------------------------------
Reads Avro-encoded Debezium CDC messages from Kafka,
deserializes using Schema Registry, extracts the 'after'
payload, and writes each table to HDFS as Parquet.

Usage:
    spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
      --conf spark.jars.ivy=/home/spark/.ivy2 \
      /opt/spark-apps/jobs/stream_to_hdfs.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_to_hdfs")

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP      = "kafka:29092"
SCHEMA_REGISTRY_URL  = "http://schema-registry:8081"
TOPIC_PREFIX         = "postgres-server.public"
HDFS_BASE_PATH       = "hdfs://namenode:9000/ecommerce/raw"

TABLES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "products",
    "sellers",
    "geolocation",
    "product_category_name_translation",
    "leads_qualified",
    "leads_closed",
]


# ─────────────────────────────────────────────
# SCHEMA REGISTRY HELPER
# ─────────────────────────────────────────────
def get_value_schema(table: str) -> str:
    """
    Fetch the latest Avro schema string for a topic's value from Schema Registry.
    Confluent subject naming: <topic>-value
    """
    subject = f"{TOPIC_PREFIX}.{table}-value"
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()["schema"]   # JSON string of the Avro schema
    except Exception as e:
        logger.error(f"Schema fetch failed for {subject}: {e}")
        return None


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
def get_spark():
    return (
        SparkSession.builder
        .appName("KafkaToHDFS_Batch")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# READ + DECODE ONE TOPIC
# ─────────────────────────────────────────────
def read_topic(spark: SparkSession, table: str):
    # Import here so Spark has loaded the avro package by this point
    from pyspark.sql.avro.functions import from_avro

    topic = f"{TOPIC_PREFIX}.{table}"
    logger.info(f"Reading topic: {topic}")

    # ── 1. Read raw Kafka messages ────────────
    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    count = raw_df.count()
    if count == 0:
        logger.warning(f"No messages in topic: {topic}")
        return None

    logger.info(f"  Raw messages: {count:,}")

    # ── 2. Fetch Avro schema from Schema Registry ──
    schema_json = get_value_schema(table)
    if schema_json is None:
        return None

    # ── 3. Strip the 5-byte Confluent magic header ──
    # Wire format: [0x00 magic][4-byte schema_id][avro payload]
    # substring in Spark SQL is 1-indexed
    stripped_df = raw_df.select(
        expr("substring(value, 6, length(value) - 5)").alias("avro_value")
    )

    # ── 4. Deserialize Avro bytes → struct ────
    decoded_df = stripped_df.select(
        from_avro(col("avro_value"), schema_json).alias("envelope")
    )

    # ── 5. Extract the 'after' payload ────────
    # Debezium envelope schema: {before, after, source, op, ts_ms, transaction}
    # 'after' = the row state AFTER the change (None for deletes)
    # op values: 'r'=snapshot read, 'c'=insert, 'u'=update, 'd'=delete
    after_df = (
        decoded_df
        .filter(col("envelope.op").isin("c", "r", "u"))
        .filter(col("envelope.after").isNotNull())
        .select(
            col("envelope.after.*"),
            col("envelope.op").alias("_op"),
            col("envelope.ts_ms").alias("_event_ts_ms"),
        )
    )

    # ── 6. Add ingestion metadata ─────────────
    final_df = (
        after_df
        .withColumn("_ingested_at",   current_timestamp())
        .withColumn("_source_table",  lit(table))
        .withColumn("_source_topic",  lit(topic))
    )

    return final_df


# ─────────────────────────────────────────────
# WRITE TO HDFS AS PARQUET
# ─────────────────────────────────────────────
def write_to_hdfs(df, table: str):
    output_path = f"{HDFS_BASE_PATH}/{table}"
    logger.info(f"Writing → {output_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
    logger.info(f"✅ Written: {output_path}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    success = []
    failed  = []

    for table in TABLES:
        try:
            df = read_topic(spark, table)
            if df is None:
                failed.append((table, "empty topic or missing schema"))
                continue

            row_count = df.count()
            write_to_hdfs(df, table)
            success.append((table, row_count))

        except Exception as e:
            logger.error(f"❌ {table}: {e}")
            failed.append((table, str(e)))

    # ── Summary ──────────────────────────────
    print("\n" + "=" * 57)
    print("  KAFKA → HDFS BATCH JOB SUMMARY")
    print("=" * 57)
    print(f"  ✅ Success ({len(success)} tables):")
    for t, rows in success:
        print(f"     {t:<42} {rows:>8,} rows")
    if failed:
        print(f"\n  ❌ Failed ({len(failed)} tables):")
        for t, reason in failed:
            print(f"     {t:<42} {reason}")
    print("=" * 57 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()