"""
Spark Transformation Job: HDFS Raw -> HDFS Transformed
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, datediff, when, lit, trim, upper, lower,
    round as spark_round, coalesce, regexp_replace,
    length, current_timestamp, year, month
)

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transform")

HDFS_RAW         = "hdfs://namenode:9000/ecommerce/raw"
HDFS_TRANSFORMED = "hdfs://namenode:9000/ecommerce/transformed"
META_COLS = ["_op", "_event_ts_ms", "_source_table", "_source_topic", "_ingested_at"]


def get_spark():
    return (
        SparkSession.builder
        .appName("EcommerceTransformations")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


def ms_to_ts(c):
    return (col(c).cast("long") / 1_000_000).cast("timestamp")


def read_raw(spark, table):
    path = f"{HDFS_RAW}/{table}"
    logger.info(f"Reading raw: {path}")
    df = spark.read.parquet(path)
    existing_meta = [c for c in META_COLS if c in df.columns]
    return df.drop(*existing_meta)


def write_transformed(df, table):
    path = f"{HDFS_TRANSFORMED}/{table}"
    logger.info(f"Writing -> {path}")
    df.write.mode("overwrite").parquet(path)
    logger.info(f"Done: {table}")


# ── ORDERS ────────────────────────────────────────────────────────────────────
def transform_orders(spark):
    df = read_raw(spark, "orders")

    transformed = (
        df
        .withColumn("order_purchase_ts",          ms_to_ts("order_purchase_timestamp"))
        .withColumn("order_approved_ts",           ms_to_ts("order_approved_at"))
        .withColumn("order_delivered_carrier_ts",  ms_to_ts("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_ts", ms_to_ts("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_ts", ms_to_ts("order_estimated_delivery_date"))

        .withColumn("actual_delivery_days",
            datediff(col("order_delivered_customer_ts"), col("order_purchase_ts")))
        .withColumn("estimated_delivery_days",
            datediff(col("order_estimated_delivery_ts"), col("order_purchase_ts")))
        .withColumn("is_late_delivery",
            when(col("order_delivered_customer_ts") > col("order_estimated_delivery_ts"),
                 lit(True)).otherwise(lit(False)))

        .withColumn("order_year",  year(col("order_purchase_ts")).cast("string"))
        .withColumn("order_month", month(col("order_purchase_ts")).cast("string"))
        .withColumn("order_status", trim(lower(col("order_status"))))

        .drop("order_purchase_timestamp", "order_approved_at",
              "order_delivered_carrier_date", "order_delivered_customer_date",
              "order_estimated_delivery_date")
        .withColumn("_ingested_at", current_timestamp())
    )
    write_transformed(transformed, "orders")
    return transformed.count()


# ── ORDER ITEMS ───────────────────────────────────────────────────────────────
def transform_order_items(spark):
    items    = read_raw(spark, "order_items")
    products = read_raw(spark, "products").select(
        "product_id", "product_category_name", "product_weight_g")
    category = read_raw(spark, "product_category_name_translation")

    transformed = (
        items
        .withColumn("total_price",    spark_round(col("price") + col("freight_value"), 2))
        .withColumn("freight_ratio",  spark_round(col("freight_value") / (col("price") + lit(0.01)), 4))
        .withColumn("shipping_limit_ts", ms_to_ts("shipping_limit_date"))
        .drop("shipping_limit_date")
        .join(products, on="product_id", how="left")
        .join(
            category.withColumnRenamed("product_category_name_english", "category_english"),
            on="product_category_name", how="left"
        )
        .withColumn("_ingested_at", current_timestamp())
    )
    write_transformed(transformed, "order_items")
    return transformed.count()


# ── ORDER PAYMENTS ────────────────────────────────────────────────────────────
def transform_order_payments(spark):
    df = read_raw(spark, "order_payments")
    transformed = (
        df
        .withColumn("payment_type", trim(lower(col("payment_type"))))
        .withColumn("is_installment",
            when(col("payment_sequential") > 1, lit(True)).otherwise(lit(False)))
        .withColumn("payment_value_bucket",
            when(col("payment_value") < 50,    lit("low"))
            .when(col("payment_value") < 200,  lit("medium"))
            .when(col("payment_value") < 1000, lit("high"))
            .otherwise(lit("premium")))
        .withColumn("_ingested_at", current_timestamp())
    )
    write_transformed(transformed, "order_payments")
    return transformed.count()


# ── ORDER REVIEWS ─────────────────────────────────────────────────────────────
def transform_order_reviews(spark):
    df = read_raw(spark, "order_reviews")
    transformed = (
        df
        .withColumn("sentiment",
            when(col("review_score") >= 4, lit("positive"))
            .when(col("review_score") == 3, lit("neutral"))
            .otherwise(lit("negative")))
        .withColumn("has_comment",
            when(col("review_comment_message").isNotNull() &
                 (length(trim(col("review_comment_message"))) > 0),
                 lit(True)).otherwise(lit(False)))
        .withColumn("review_comment_message",
            when(col("has_comment"), trim(col("review_comment_message")))
            .otherwise(lit(None)))
        .withColumn("review_creation_ts", ms_to_ts("review_creation_date"))
        .withColumn("review_answer_ts",   ms_to_ts("review_answer_timestamp"))
        .drop("review_creation_date", "review_answer_timestamp")
        .withColumn("_ingested_at", current_timestamp())
    )
    write_transformed(transformed, "order_reviews")
    return transformed.count()


# ── CUSTOMERS ─────────────────────────────────────────────────────────────────
def transform_customers(spark):
    df = read_raw(spark, "customers")
    transformed = (
        df
        .dropDuplicates(["customer_unique_id"])
        .withColumn("customer_state", trim(upper(col("customer_state"))))
        .withColumn("customer_city",
            trim(lower(regexp_replace(col("customer_city"), r"\s+", " "))))
        .withColumn("_ingested_at", current_timestamp())
    )
    write_transformed(transformed, "customers")
    return transformed.count()


# ── PRODUCTS ──────────────────────────────────────────────────────────────────
def transform_products(spark):
    df = read_raw(spark, "products")
    transformed = (
        df
        .fillna(0, subset=["product_weight_g", "product_length_cm",
                            "product_height_cm", "product_width_cm",
                            "product_photos_qty", "product_description_lenght",
                            "product_name_lenght"])
        .withColumn("product_category_name",
            coalesce(trim(lower(col("product_category_name"))), lit("unknown")))
        .withColumn("weight_bucket",
            when(col("product_weight_g") <= 500,   lit("light"))
            .when(col("product_weight_g") <= 2000,  lit("medium"))
            .when(col("product_weight_g") <= 10000, lit("heavy"))
            .otherwise(lit("very_heavy")))
        .withColumn("has_description",
            when(col("product_description_lenght") > 0, lit(True)).otherwise(lit(False)))
        .withColumn("_ingested_at", current_timestamp())
    )
    write_transformed(transformed, "products")
    return transformed.count()


# ── SELLERS ───────────────────────────────────────────────────────────────────
def transform_sellers(spark):
    df = read_raw(spark, "sellers")
    transformed = (
        df
        .withColumn("seller_state", trim(upper(col("seller_state"))))
        .withColumn("seller_city",
            trim(lower(regexp_replace(col("seller_city"), r"\s+", " "))))
        .withColumn("_ingested_at", current_timestamp())
    )
    write_transformed(transformed, "sellers")
    return transformed.count()


# ── GEOLOCATION ───────────────────────────────────────────────────────────────
def transform_geolocation(spark):
    df = read_raw(spark, "geolocation")
    transformed = (
        df
        .filter(col("geolocation_lat").between(-33.75, 5.27) &
                col("geolocation_lng").between(-73.99, -34.79))
        .withColumn("geolocation_state", trim(upper(col("geolocation_state"))))
        .withColumn("geolocation_city",
            trim(lower(regexp_replace(col("geolocation_city"), r"\s+", " "))))
        .dropDuplicates(["geolocation_zip_code_prefix"])
        .withColumn("_ingested_at", current_timestamp())
    )
    write_transformed(transformed, "geolocation")
    return transformed.count()


# ── LEADS ─────────────────────────────────────────────────────────────────────
def transform_leads(spark):
    qualified = read_raw(spark, "leads_qualified")
    closed    = read_raw(spark, "leads_closed")
    transformed = (
        qualified
        .join(closed, on="mql_id", how="left")
        .withColumn("is_converted",
            when(col("seller_id").isNotNull(), lit(True)).otherwise(lit(False)))
        .withColumn("first_contact_ts", ms_to_ts("first_contact_date"))
        .withColumn("won_date_ts",      ms_to_ts("won_date"))
        .withColumn("days_to_close",
            datediff(col("won_date_ts"), col("first_contact_ts")))
        .drop("first_contact_date", "won_date")
        .withColumn("_ingested_at", current_timestamp())
    )
    write_transformed(transformed, "leads")
    return transformed.count()


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    jobs = [
        ("orders",         transform_orders),
        ("order_items",    transform_order_items),
        ("order_payments", transform_order_payments),
        ("order_reviews",  transform_order_reviews),
        ("customers",      transform_customers),
        ("products",       transform_products),
        ("sellers",        transform_sellers),
        ("geolocation",    transform_geolocation),
        ("leads",          transform_leads),
    ]

    success, failed = [], []
    for name, fn in jobs:
        try:
            rows = fn(spark)
            success.append((name, rows))
        except Exception as e:
            logger.error(f"FAILED {name}: {e}")
            failed.append((name, str(e)))

    print("\n" + "=" * 57)
    print("  TRANSFORMATION JOB SUMMARY")
    print("=" * 57)
    print(f"  Success ({len(success)} tables):")
    for t, rows in success:
        print(f"     {t:<30} {rows:>10,} rows")
    if failed:
        print(f"\n  Failed ({len(failed)} tables):")
        for t, reason in failed:
            print(f"     {t:<30} {reason[:80]}")
    print("=" * 57 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()