# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "05913245-8d89-4d7f-8eee-4582e03c1258",
# META       "default_lakehouse_name": "Stock",
# META       "default_lakehouse_workspace_id": "a2d18893-d874-4d0f-83d6-91d2ed3d9dfa",
# META       "known_lakehouses": [
# META         {
# META           "id": "05913245-8d89-4d7f-8eee-4582e03c1258"
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
from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import reduce

# Deleting the temp_stock_data table
spark.sql("DROP TABLE IF EXISTS temp_stock_data")

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
    df = yf.Ticker(ticker).history(period="3d").reset_index()
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

display(final_spark_df.sort(desc(col("date"))))


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
