from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RegisterHiveTables") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

HDFS_GOLD = "hdfs://namenode:9000/ecommerce/gold"

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_gold")
spark.sql("USE ecommerce_gold")

tables = [
    "fact_order_items",
    "dim_customers",
    "dim_products",
    "dim_sellers",
    "dim_date",
]

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS ecommerce_gold.{table}")
    spark.sql(f"""
        CREATE EXTERNAL TABLE ecommerce_gold.{table}
        STORED AS PARQUET
        LOCATION '{HDFS_GOLD}/{table}'
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM ecommerce_gold.{table}").collect()[0][0]
    print(f"✅  {table:<25} {count:>10,} rows")

spark.stop()