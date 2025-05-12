# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "edd6afe6-a35e-4241-aca0-556447fcffbe",
# META       "default_lakehouse_name": "Reddit_Data",
# META       "default_lakehouse_workspace_id": "c1bac311-bdf8-44ad-907a-6a2f4314508a"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "6b5c8c08-57a4-bc93-43a3-df73b5992a81",
# META       "known_warehouses": [
# META         {
# META           "id": "6b5c8c08-57a4-bc93-43a3-df73b5992a81",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Install Yahoo Finance client
pip install yfinance

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import yfinance as yf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import reduce

# Define stock tickers
ticker_list = ["TSLA", "MSFT", "AAPL", "NVDA", "GOOGL", "AMZN", "META", "AVGO", "TSM"]

# Define schema
schema = StructType([
    StructField("tickers", ArrayType(StringType()), True)
])

# Create DataFrame of tickers
ticker_df = spark.createDataFrame([(ticker_list,)], schema=schema)

# Extract ticker list from DataFrame
tickers = ticker_df.collect()[0]["tickers"]  # Convert PySpark DataFrame to Python list

all_stock_data = []

for ticker in tickers:
    # Fetch historical data from Yahoo Finance
    df = yf.Ticker(ticker).history(period="1d").reset_index()

    # Convert Pandas DataFrame to PySpark DataFrame
    spark_df = spark.createDataFrame(df)

    # Select relevant columns and add ticker name
    spark_df = spark_df.select("Date", "Close").withColumn("ticker", lit(ticker))

    # Collect all stock data
    all_stock_data.append(spark_df)

# Combine all DataFrames into one
final_spark_df = reduce(DataFrame.union, all_stock_data)
final_spark_df = final_spark_df.withColumn("Date",final_spark_df.Date.cast(DateType()))
final_spark_df.write.mode("overwrite").format("delta").saveAsTable("temp_stock_data")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
