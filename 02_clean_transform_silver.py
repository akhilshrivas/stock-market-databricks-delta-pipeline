# Databricks notebook source
# =========================
# SILVER LAYER – CLEAN + MERGE
# =========================

from pyspark.sql.functions import col
from delta.tables import DeltaTable

# Use schema
spark.sql("USE stock_pipeline")

# Read Bronze data
bronze_df = spark.table("stock_pipeline.bronze_stock_prices")

# Basic cleaning
clean_df = bronze_df.filter(col("price").isNotNull())

# Create Silver table if not exists
clean_df.limit(0).write.format("delta") \
    .mode("ignore") \
    .saveAsTable("stock_pipeline.silver_stock_prices")

# Delta MERGE (incremental upsert)
silver_table = DeltaTable.forName(
    spark,
    "stock_pipeline.silver_stock_prices"
)

silver_table.alias("t").merge(
    clean_df.alias("s"),
    "t.symbol = s.symbol AND t.event_time = s.event_time"
).whenNotMatchedInsertAll().execute()

# Validate
display(spark.table("stock_pipeline.silver_stock_prices"))
