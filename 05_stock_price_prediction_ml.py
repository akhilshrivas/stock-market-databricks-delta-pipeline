# Databricks notebook source
spark.sql("USE stock_pipeline")

df = spark.table("stock_pipeline.gold_daily_stock_metrics")
display(df)


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

feature_df = df.withColumn("date_index", df.date.cast("timestamp").cast("long"))

assembler = VectorAssembler(
    inputCols=["date_index"],
    outputCol="features"
)

final_df = assembler.transform(feature_df) \
    .select("features", "avg_price")

lr = LinearRegression(labelCol="avg_price")
model = lr.fit(final_df)

predictions = model.transform(final_df)
display(predictions)
