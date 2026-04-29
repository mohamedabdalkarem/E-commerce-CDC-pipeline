"""
Spark Star Schema Job: HDFS Transformed (Silver) -> HDFS Gold
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, lit, coalesce,
    monotonically_increasing_id, current_timestamp,
    to_date, year, month, dayofmonth, dayofweek,
    date_format, quarter, round as spark_round
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_layer")

# ✅ Gold reads from SILVER (transformed), not raw
HDFS_TRANSFORMED = "hdfs://namenode:9000/ecommerce/transformed"
HDFS_GOLD        = "hdfs://namenode:9000/ecommerce/gold"


def get_spark():
    return (
        SparkSession.builder
        .appName("EcommerceGoldLayer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_silver(spark, table):
    path = f"{HDFS_TRANSFORMED}/{table}"
    logger.info(f"Reading silver: {path}")
    return spark.read.parquet(path)


def write_gold(df, name):
    path = f"{HDFS_GOLD}/{name}"
    logger.info(f"Writing gold: {path}")
    df.write.mode("overwrite").parquet(path)


# ─────────────────────────────────────────────────────────────
# DIMENSIONS
# ─────────────────────────────────────────────────────────────

def build_dim_customers(spark):
    df = read_silver(spark, "customers")

    dim = (
        df
        .dropDuplicates(["customer_unique_id"])
        .withColumn("customer_sk", monotonically_increasing_id())
        .withColumn("customer_city",  lower(trim(col("customer_city"))))
        .withColumn("customer_state", upper(trim(col("customer_state"))))
        .select(
            "customer_sk",
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state"
        )
        .withColumn("_ingested_at", current_timestamp())
    )
    write_gold(dim, "dim_customers")
    return dim


def build_dim_products(spark):
    df = read_silver(spark, "products")

    dim = (
        df
        .withColumn("product_sk", monotonically_increasing_id())
        .withColumn("category",
            coalesce(lower(trim(col("product_category_name"))), lit("unknown")))
        .select(
            "product_sk",
            "product_id",
            "category",
            "product_weight_g",
            "weight_bucket"       # ✅ already computed in silver
        )
        .withColumn("_ingested_at", current_timestamp())
    )
    write_gold(dim, "dim_products")
    return dim


def build_dim_sellers(spark):
    df = read_silver(spark, "sellers")

    dim = (
        df
        .withColumn("seller_sk", monotonically_increasing_id())
        .withColumn("seller_city",  lower(trim(col("seller_city"))))
        .withColumn("seller_state", upper(trim(col("seller_state"))))
        .select(
            "seller_sk",
            "seller_id",
            "seller_city",
            "seller_state"
        )
        .withColumn("_ingested_at", current_timestamp())
    )
    write_gold(dim, "dim_sellers")
    return dim


def build_dim_date(spark):
    """
    Build a proper date dimension from all order purchase timestamps.
    order_purchase_ts in silver is already a real timestamp column.
    """
    orders = read_silver(spark, "orders")

    dim = (
        orders
        # order_purchase_ts is already a timestamp — just cast to date
        .withColumn("date_id", to_date(col("order_purchase_ts")))
        .dropDuplicates(["date_id"])
        .filter(col("date_id").isNotNull())

        # ✅ Use proper Spark date functions — not substr()
        .withColumn("year",          year(col("date_id")))
        .withColumn("month",         month(col("date_id")))
        .withColumn("day",           dayofmonth(col("date_id")))
        .withColumn("quarter",       quarter(col("date_id")))
        .withColumn("day_of_week",   dayofweek(col("date_id")))          # 1=Sun, 7=Sat
        .withColumn("month_name",    date_format(col("date_id"), "MMMM"))
        .withColumn("is_weekend",
            col("day_of_week").isin(1, 7))                               # Sun or Sat

        .select(
            "date_id",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "is_weekend"
        )
        .withColumn("_ingested_at", current_timestamp())
    )

    write_gold(dim, "dim_date")
    return dim


# ─────────────────────────────────────────────────────────────
# FACT TABLE
# ─────────────────────────────────────────────────────────────

def build_fact_order_items(spark):
    # ✅ All reads from silver — types already clean
    items    = read_silver(spark, "order_items")
    orders   = read_silver(spark, "orders")
    payments = read_silver(spark, "order_payments")

    dim_customers = spark.read.parquet(f"{HDFS_GOLD}/dim_customers")
    dim_products  = spark.read.parquet(f"{HDFS_GOLD}/dim_products")
    dim_sellers   = spark.read.parquet(f"{HDFS_GOLD}/dim_sellers")
    dim_date      = spark.read.parquet(f"{HDFS_GOLD}/dim_date")

    # Aggregate payments per order (one row per order)
    payments_agg = (
        payments
        .groupBy("order_id")
        .agg(
            spark_round(
                __import__("pyspark.sql.functions", fromlist=["sum"]).sum("payment_value"), 2
            ).alias("total_payment_value"),
            __import__("pyspark.sql.functions", fromlist=["max"]).max("payment_installments")
             .alias("max_installments")
        )
    )

    # orders already has order_purchase_ts as a proper timestamp in silver
    orders_with_date = (
        orders
        .withColumn("date_id", to_date(col("order_purchase_ts")))
        .select(
            "order_id",
            "customer_id",
            "date_id",
            "order_status",
            "actual_delivery_days",
            "estimated_delivery_days",
            "is_late_delivery"
        )
    )

    fact = (
        items
        .join(orders_with_date, "order_id")
        .join(payments_agg,    "order_id",   "left")
        .join(dim_customers,   "customer_id")
        .join(dim_products,    "product_id")
        .join(dim_sellers,     "seller_id")
        .join(dim_date,        "date_id")

        .withColumn("total_price",
            spark_round(col("price") + col("freight_value"), 2))

        .select(
            # keys
            col("order_id"),
            col("order_item_id"),
            col("date_id"),
            col("customer_sk"),
            col("product_sk"),
            col("seller_sk"),
            # measures
            col("price"),
            col("freight_value"),
            col("total_price"),
            col("total_payment_value"),
            col("max_installments"),
            # delivery metrics
            col("order_status"),
            col("actual_delivery_days"),
            col("estimated_delivery_days"),
            col("is_late_delivery"),
        )
        .withColumn("_ingested_at", current_timestamp())
    )

    write_gold(fact, "fact_order_items")
    return fact.count()


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Building dimensions...")
        build_dim_customers(spark)
        build_dim_products(spark)
        build_dim_sellers(spark)
        build_dim_date(spark)          # must run before fact

        logger.info("Building fact table...")
        rows = build_fact_order_items(spark)

        print("\n" + "=" * 55)
        print("        GOLD LAYER BUILD COMPLETE")
        print("=" * 55)
        print(f"  fact_order_items rows : {rows:,}")
        print("=" * 55 + "\n")

    except Exception as e:
        logger.error(f"Job Failed: {e}", exc_info=True)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()