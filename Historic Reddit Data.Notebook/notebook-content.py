# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "b2f28c43-5191-41e3-a5cd-40721f8bf8e0"
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


import praw
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob

# Reddit API Authentication using PRAW

reddit = praw.Reddit(
    client_id="JG7mziWmSjbcnXH39GCNCQ",
    client_secret="DQyO06RmOXlFefp6wiFV6woh9nIyJQ",
    user_agent="redcap"
)

#  Define subreddit and ticker lists
comments_list = []

#query = "TESLA OR $TSLA OR tesla OR $tsla"

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


# searching the reddit posts for posts related to the stock tickers
x = 0
y = 0
for y in range(10):
    for name in name_list:
        subreddit_name = name  
        for submission in reddit.subreddit(subreddit_name).\
            search(ticker_list[y]["ticker"], sort = "top", limit=1000):
            if submission.created_utc > 1640995200:  
                comments_list.append((subreddit_name, submission.title, submission.score,\
                submission.created_utc,ticker_list[y]["ticker"]))

# creating schema for extracted data
schema = StructType([
    StructField("subreddit_name", StringType(), True),
    StructField("post_title", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("time_utc", DoubleType(), True),
    StructField("ticker", StringType(), True)

])
df = spark.createDataFrame(comments_list, schema = schema)

# Convert UNIX seconds to timestamp, then to America/New_York timezone
# Adjust ET date: if before 4pm ET, count as previous trading day

df = df.withColumn("time_utc", col("time_utc").cast("bigint"))
df = df.withColumn("time_utc", from_unixtime(col("time_utc")).cast("timestamp"))\
    .withColumn("time_est",from_utc_timestamp(col("time_utc"), "America/New_York"))\
    .withColumn("ticker_id",regexp_extract(col("ticker"), r"\$([A-Za-z]+)", 1))
df = df.withColumn("time_est", when(hour("time_est") < 16,\
    (date_sub("time_est",1)).cast("date")).otherwise((df.time_est).cast("date")))
df = df.drop("ticker","actual_ticker")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(df.sort(asc("time_est")))
print(df.count())


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
display(df3)
#df3.write.format("delta").mode("append").option("mergeSchema",True).saveAsTable("reddit_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3.write.format("delta").mode("append").option("mergeSchema",True).saveAsTable("reddit_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("reddit_data")
display(df.sort(desc("time_est")))

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
