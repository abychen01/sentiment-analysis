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
# META     }
# META   }
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df = spark.read.table("reddit_data")
#df = df.select(df.ticker_id, df)
display(df.filter(col("time_est") > "2025-01-01 00:00:00") \
    .where(col("sentiment_label") == "positive") \
    .select("ticker_id") \
    #.groupby("ticker_id") \
    .count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install nltk


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("reddit_data").limit(333)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Install Spark NLP Python library
pip install spark-nlp 5.1.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

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
df = df.withColumn("sentiment_label",\
        when(df.sentiment_label=="neutral", sentiment_udf(col("post_title")))\
        .otherwise(df.sentiment_label))

# Show results
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.where(df.sentiment_label == "neutral").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install afinn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Reddit_Data.reddit_data")
display(df.where(df.sentiment_label == "neutral").count())     

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

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

df = spark.read.table("Reddit_Data.reddit_data")

# Apply to DataFrame
df = df.withColumn("sentiment_label",\
        when(df.sentiment_label=="neutral", sentiment_udf(col("post_title")))\
        .otherwise(df.sentiment_label))

# Show results
display(df.where(df.sentiment_label == "neutral").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df = spark.read.table("Reddit_Data.reddit_data")
display(df.sort(desc("time_utc")).limit(19))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df = spark.read.table("Reddit_Data.reddit_data")
df = df.sort(desc("time_utc")).limit(10)
display(df.select("time_utc","time_est").withColumn("est",from_utc_timestamp(col("time_utc"), "America/New_York")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
df = DeltaTable.forName(spark, "reddit_data")
#display(df.history())
df.restoreToVersion(43)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_version_0 = spark.read.format("delta").option("versionAsOf", 43).table("reddit_data")
display(df_version_0.sort(desc("time_utc")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.restoreToVersion(43)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

df = spark.read.table("Reddit_Data.reddit_data")

display(df.sort(desc("time_utc")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
df = spark.read.table("NYSE_calendar")
df = df.select("date_value","NYSE_holiday")
df = df.withColumn("closing", day("date_value") + year("date_value"))
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.option("mergeSchema", True).mode("overwrite").saveAsTable("testing")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Reddit_Data.reddit_data")
df.summary()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df = df.withColumn("score", col("score").cast("double")) \
       .withColumn("ticker_id", col("ticker_id").cast("int"))

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("SummaryExample").getOrCreate()

# Define schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Salary", DoubleType(), True),
    StructField("Department", StringType(), True)
])

# Create Data
data = [
    ("Alice", 30, 60000.0, "HR"),
    ("Bob", 25, 55000.0, "Finance"),
    ("Charlie", 35, 70000.0, "IT"),
    ("David", 40, 80000.0, "HR"),
    ("Eve", 29, 62000.0, "Finance")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.summary())

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
