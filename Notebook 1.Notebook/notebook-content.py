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
# META       "default_lakehouse_workspace_id": "c1bac311-bdf8-44ad-907a-6a2f4314508a",
# META       "known_lakehouses": [
# META         {
# META           "id": "edd6afe6-a35e-4241-aca0-556447fcffbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

pip install praw textblob pyspark


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# testing........


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

dff = spark.read.table("reddit_data")
dff = dff.select(dff.time_utc).orderBy(desc(dff.time_utc)).limit(1)
dff = (dff.withColumn("time_utc",unix_timestamp(dff.time_utc)))
max_timestamp = dff.select("time_utc").collect()[0][0]

x = 0
y = 0
for y in range(10):
    for name in name_list:
        subreddit_name = name  
        for submission in reddit.subreddit(subreddit_name).\
            search(ticker_list[y]["ticker"], sort = "new", limit=1000):
            if submission.created_utc > max_timestamp:  
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
df = df.withColumn("time_utc", from_unixtime(col("time_utc")).cast("timestamp"))\
    .withColumn("time_est",from_utc_timestamp(col("time_utc"), "America/New_York"))\
    .withColumn("stock_name",regexp_extract(col("ticker"), r"\[([A-Za-z]+)", 1))\
    .withColumn("ticker_id",regexp_extract(col("ticker"), r"\$([A-Za-z]+)", 1))
df = df.drop("ticker","actual_ticker")


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
df3.write.format("delta").mode("append").option("mergeSchema",True).saveAsTable("reddit_data")

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
