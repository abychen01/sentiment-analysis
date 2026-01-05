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

import pyodbc, os
from pyspark.sql.functions import col, lit
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

import yfinance as yf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import reduce

from pyspark.sql.types import StringType, StructField, StructType, FloatType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_creds = spark.read.parquet('Files/creds')

os.environ["AZURE_CLIENT_ID"] = df_creds.collect()[0]["AZURE_CLIENT_ID"]
os.environ["AZURE_TENANT_ID"] = df_creds.collect()[0]["AZURE_TENANT_ID"]
os.environ["AZURE_CLIENT_SECRET"] = df_creds.collect()[0]["AZURE_CLIENT_SECRET"]


vault_url = "https://vaultforfabric.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

password = client.get_secret("sql-server-password").value

conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE=myFreeDB;"
            f"UID=admin2;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with pyodbc.connect(conn_str, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT max(Date) FROM stock_data 
        """)

          
        while True:
            result = cursor.fetchall()
            if result:
                print('first',result[0][0])
            if not cursor.nextset():
                break

latest_date = result[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
    df = yf.Ticker(ticker).history(start=latest_date).reset_index()
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
display(final_spark_df)

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

from pyspark.sql.types import StringType, StructField, StructType, DoubleType, DateType
from datetime import date

if spark.catalog.tableExists("stock_data") == False:

    stock_data_schema = StructType([
        StructField("Date", DateType()),
        StructField("Close", DoubleType()),
        StructField("ticker", StringType())
    ]) 

    dummy_data = [(date(2000,1,1),0.0, 'AAPL')]

    stock_data_df = spark.createDataFrame(dummy_data,schema=stock_data_schema)
    stock_data_df.write.saveAsTable('stock_data')

else:
    print('Table exists')

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
