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
# META       "default_warehouse": "b5992a81-df73-43a3-bc93-57a46b5c8c08",
# META       "known_warehouses": [
# META         {
# META           "id": "b5992a81-df73-43a3-bc93-57a46b5c8c08",
# META           "type": "Datawarehouse"
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
display(final_spark_df)
final_spark_df.write.mode("overwrite").format("delta").saveAsTable("stock_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install praw textblob pyspark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import praw
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob

# Reddit API Authentication
reddit = praw.Reddit(
    client_id="JG7mziWmSjbcnXH39GCNCQ",
    client_secret="DQyO06RmOXlFefp6wiFV6woh9nIyJQ",
    user_agent="redcap"
)

# Fetch Comments from a Subreddit
comments_list = []
query = "TESLA OR $TSLA OR tesla OR $tsla"
query1 = [
    (["TESLA OR $TSLA OR tesla OR $tsla"],),
    (["Microsoft OR MSFT OR microsoft OR msft OR $MSFT OR $msft"],),
    (["Apple or apple or $AAPL or $aapl"],),
    (["NVIDIA or nvidia or $NVDA or $nvda"],),
    (["Alphabet or alphabet or google or Google or $GOOGL or $googl"],),
    (["Amazon OR amazon OR $AMZN OR $amzn"],),
    (["Meta OR meta OR Facebook OR facebook OR $META OR $meta"],),
    (["Broadcom OR broadcom OR $AVGO OR $avgo"],),
    (["TSMC OR tsmc OR Taiwan Semiconductor OR $TSM OR $tsm"],),
    (["Berkshire Hathaway OR berkshire hathaway OR $BRK.A OR $BRK.B OR $brk.a OR $brk.b"],)
]

data = [(["wallstreetbets", "CanadianInvestor", "Daytrading", "StockMarket", "Stocks","investing"],)]

# ✅ Define Schema
schema = StructType([
    StructField("names", ArrayType(StringType()), True)  # Only an array column
])

schema1 = StructType([
    StructField("ticker", ArrayType(StringType()), True)  # Only an array column
])


# ✅ Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df2 = spark.createDataFrame(query1, schema=schema1)

name_list = df.select("names").collect()[0]["names"]  # Extract the array
ticker_list =df2.collect()

x = 0
y = 0
for y in range(10):
    for name in name_list:
        subreddit_name = name  
        for submission in reddit.subreddit(subreddit_name).\
            search(ticker_list[y]["ticker"], sort = "new", limit=1000):
            if submission.created_utc > 1640995200:  
                comments_list.append((subreddit_name, submission.title, submission.score,\
                submission.created_utc,ticker_list[y]["ticker"]))

schema = StructType([
    StructField("subreddit_name", StringType(), True),
    StructField("post_title", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("time_utc", DoubleType(), True),
    StructField("ticker", StringType(), True)

])
df = spark.createDataFrame(comments_list, schema = schema)

df = df.withColumn("time_utc", col("time_utc").cast("bigint"))
df = df.withColumn("time_utc", from_unixtime(col("time_utc")).cast("timestamp"))
df = df.withColumn("time_est",from_utc_timestamp(col("time_utc"), "America/New_York"))\
    .withColumn("stock_name",regexp_extract(col("ticker"), r"\[([A-Za-z]+)", 1))\
    .withColumn("ticker_id",regexp_extract(col("ticker"), r"\$([A-Za-z]+)", 1))
display(df)

#df.write.format("delta").mode("overwrite").option("mergeSchema",True).saveAsTable("reddit_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.drop("ticker","actual_ticker")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install textblob

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from transformers import pipeline

#df = spark.read.table("reddit_data")

# Load pre-trained BERT sentiment model
sentiment_pipeline = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")

# Define function for BERT-based sentiment analysis
def get_bert_sentiment(text):
    if text is None:
        return "neutral"
    result = sentiment_pipeline(text)[0]
    label_map = {"LABEL_0": "negative", "LABEL_1": "neutral", "LABEL_2": "positive"}  # Ensure correct mapping
    
    return label_map.get(result["label"], "neutral")  

# Convert function to UDF
bert_sentiment_udf = udf(get_bert_sentiment, StringType())

# Apply to DataFrame
df3 = df.withColumn("sentiment_label", bert_sentiment_udf(df["post_title"]))

# Show results
display(df3.sort("time_utc"))
#df.write.mode("overwrite").option("mergeSchema",True).format("delta").saveAsTable("reddit_data")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3.write.mode("overwrite").option("mergeSchema",True).format("delta").saveAsTable("reddit_data")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Reddit_Data.reddit_data")
display(df)

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
