# Databricks notebook source
# =========================
# DATA QUALITY CHECKS – FINAL
# =========================

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from datetime import datetime, timezone

spark.sql("USE stock_pipeline")

# Read Silver data
df = spark.table("stock_pipeline.silver_stock_prices")

# -------------------------
# Rule 1: Empty dataset
# -------------------------
row_count = df.count()
if row_count == 0:
    raise Exception("Silver layer is empty")

# -------------------------
# Rule 2: Invalid prices
# -------------------------
invalid_price_count = df.filter(col("price") <= 0).count()

# -------------------------
# Create log table (controlled schema)
# -------------------------
log_schema = StructType([
    StructField("check_name", StringType(), False),
    StructField("failed_rows", LongType(), False),
    StructField("checked_at", TimestampType(), False)
])

spark.createDataFrame([], log_schema) \
    .write.format("delta") \
    .mode("ignore") \
    .saveAsTable("stock_pipeline.data_quality_logs")

# -------------------------
# Create log record (explicit types)
# -------------------------
log_data = [
    (
        "price_positive_check",
        int(invalid_price_count),  # cast explicitly
        datetime.now(timezone.utc) # timezone-aware
    )
]

log_df = spark.createDataFrame(log_data, schema=log_schema)

# -------------------------
if invalid_price_count > 0:
    raise Exception("Data quality check failed: price <= 0")

display(log_df)
