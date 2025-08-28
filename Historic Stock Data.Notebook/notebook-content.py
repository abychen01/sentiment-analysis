# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b3b54ba2-cc81-4a95-b001-84731590108e",
# META       "default_lakehouse_name": "Stock_Data",
# META       "default_lakehouse_workspace_id": "1682a0d7-4d6c-47fd-88f0-3cb543c163d6",
# META       "known_lakehouses": [
# META         {
# META           "id": "b3b54ba2-cc81-4a95-b001-84731590108e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

pip install yfinance


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import yfinance as yf

from pyspark.sql.functions import col, lit, DataFrame, desc
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
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
    df = yf.Ticker(ticker).history(period="3y").reset_index()
    # Convert Pandas DataFrame to PySpark DataFrame
    spark_df = spark.createDataFrame(df)
    # Select relevant columns and add ticker name
    spark_df = spark_df.select("Date", "Close").withColumn("ticker", lit(ticker))
    # Collect all stock data
    all_stock_data.append(spark_df)
# Combine all DataFrames into one
final_spark_df = reduce(DataFrame.union, all_stock_data)
final_spark_df = final_spark_df.withColumn("Date",col("Date").cast("date"))
display(final_spark_df.sort(desc("Date")))

final_spark_df.write.mode("overwrite").format("delta").saveAsTable("stock_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_spark_df.write.mode("overwrite").format("delta").saveAsTable("stock_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("stock_data")
display(df.sort(desc("Date")))

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
