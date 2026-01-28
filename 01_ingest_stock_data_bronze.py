# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS stock_pipeline")
spark.sql("USE stock_pipeline")

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

data = [
    Row(symbol="AAPL", price=185.3),
    Row(symbol="AAPL", price=186.1),
    Row(symbol="GOOG", price=141.8),
    Row(symbol="GOOG", price=142.4),
    Row(symbol="MSFT", price=378.2),
]

df = spark.createDataFrame(data) \
          .withColumn("event_time", current_timestamp())

df.write.format("delta") \
  .mode("append") \
  .saveAsTable("stock_pipeline.bronze_stock_prices")


display(df)


# COMMAND ----------


spark.table("stock_pipeline.bronze_stock_prices").show()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT symbol, COUNT(*) 
# MAGIC FROM stock_pipeline.bronze_stock_prices 
# MAGIC GROUP BY symbol;
# MAGIC