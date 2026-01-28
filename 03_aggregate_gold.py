# Databricks notebook source
# =========================
# GOLD LAYER – AGGREGATION
# =========================

from pyspark.sql.functions import avg, max, min, date_format

spark.sql("USE stock_pipeline")

silver_df = spark.table("stock_pipeline.silver_stock_prices")

gold_df = silver_df.groupBy(
    "symbol",
    date_format("event_time", "yyyy-MM-dd").alias("date")
).agg(
    avg("price").alias("avg_price"),
    max("price").alias("max_price"),
    min("price").alias("min_price")
)

# Create Gold table (partitioned)
gold_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable("stock_pipeline.gold_daily_stock_metrics")

# Optimize
spark.sql("""
OPTIMIZE stock_pipeline.gold_daily_stock_metrics
ZORDER BY (symbol)
""")

display(gold_df)


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS stock_pipeline.gold_daily_stock_metrics")


# COMMAND ----------

spark.sql("""
SHOW TABLES IN stock_pipeline
""").show()
