# Stock Market Data Pipeline using Databricks & Delta Lake

## Overview
An end-to-end stock market data pipeline built on Databricks using Delta Lake and the Medallion (Bronze–Silver–Gold) architecture.  
The pipeline ingests stock data, performs incremental transformations, enforces data quality checks, and supports downstream machine learning for stock price prediction.

## Architecture
Bronze → Silver → Gold → ML

## Key Features
- Delta Lake tables with incremental ingestion
- Silver layer UPSERT using Delta MERGE
- Gold layer aggregation with partitioning and Z-Ordering
- Data quality validation with failure logging
- Databricks Jobs orchestration (DAG-based)
- Stock price prediction using Spark ML
- Serverless execution

## Tech Stack
- Databricks
- Apache Spark (PySpark)
- Delta Lake
- Databricks Jobs
- Spark ML

## Pipeline Flow
1. Bronze: Raw stock data ingestion
2. Silver: Cleaned and deduplicated data (MERGE)
3. Gold: Daily aggregated metrics
4. Quality Checks: Hard rules + logging
5. ML: Stock price prediction using curated Gold data

## Screenshots
See the `/screenshots` folder for pipeline execution and results.

## Future Enhancements
- Real-time ingestion using Databricks Auto Loader
- External stock market APIs
- Advanced time-series models (ARIMA, LSTM)
- Alerting and monitoring
