# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "54b3eb0f-12b7-496e-a704-f423005a6a09",
# META       "default_lakehouse_name": "Reddit",
# META       "default_lakehouse_workspace_id": "fa56728c-4e8f-4cf0-b06b-130982363f52",
# META       "known_lakehouses": [
# META         {
# META           "id": "54b3eb0f-12b7-496e-a704-f423005a6a09"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

pip install praw textblob

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.parquet("Files/creds")
reddit_id = df2.collect()[0]['reddit_id']
reddit_secret = df2.collect()[0]['reddit_secret']
reddit_user_agent = df2.collect()[0]['reddit_user_agent']


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

# Reddit API Authentication using PRAW

reddit = praw.Reddit(
    client_id=reddit_id,
    client_secret=reddit_secret,
    user_agent=reddit_user_agent
)

#  Define subreddit and ticker lists
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

# Define Schema
subreddit_schema = StructType([
    StructField("names", ArrayType(StringType()), True)  # Only an array column
])

ticker_schema = StructType([
    StructField("ticker", ArrayType(StringType()), True)  # Only an array column
])


# Create DataFrame
df = spark.createDataFrame(data, schema=subreddit_schema)
df2 = spark.createDataFrame(query1, schema=ticker_schema)

name_list = df.select("names").collect()[0]["names"]  # Extract the array
ticker_list =df2.collect()

# Collect posts created since last ingestion timestamp
dff = spark.read.table("reddit_data")
dff = dff.select(dff.time_utc).orderBy(desc(dff.time_utc)).limit(1)
dff = (dff.withColumn("time_utc",unix_timestamp(dff.time_utc)))
max_timestamp = dff.select("time_utc").collect()[0][0]

# searching the reddit posts for posts related to the stock tickers
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

# creating schema for extracted data
table_schema = StructType([
    StructField("subreddit_name", StringType(), True),
    StructField("post_title", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("time_utc", DoubleType(), True),
    StructField("ticker", StringType(), True)

])
df = spark.createDataFrame(comments_list, schema = table_schema)

# Convert UNIX seconds to timestamp, then to America/New_York timezone
# Adjust ET date: if before 4pm ET, count as previous trading day

df = df.withColumn("time_utc", col("time_utc").cast("bigint"))
df = df.withColumn("time_utc", from_unixtime(col("time_utc")).cast("timestamp"))\
    .withColumn("time_est",from_utc_timestamp(col("time_utc"), "America/New_York"))\
    .withColumn("ticker_id",regexp_extract(col("ticker"), r"\$([A-Za-z]+)", 1))
df = df.withColumn("time_est", when(hour("time_est") < 16,\
    (date_sub("time_est",1)).cast("date")).otherwise((df.time_est).cast("date")))
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

# Sentiment Analysis UDFs (RoBERTa + FinBERT)

from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

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

# Load FinBERT model and tokenizer
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

# Define UDF for sentiment analysis
def finbert_sentiment(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    outputs = model(**inputs)
    predictions = torch.argmax(outputs.logits, dim=-1)
    return ["positive", "negative", "neutral"][predictions[0]]

# Register UDF
sentiment_udf = udf(finbert_sentiment, StringType())

# Apply to DataFrame
df3 = df3.withColumn("sentiment_label",\
        when(df3.sentiment_label=="neutral", sentiment_udf(col("post_title")))\
        .otherwise(df3.sentiment_label))

# Show results
#display(df3)
df3.write.format("delta").mode("append").option("mergeSchema",True).saveAsTable("reddit_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
