 hn xsx# Stock Prices & Reddit Sentiment ETL Pipeline

An automated ETL pipeline on Microsoft Fabric that aggregates daily Reddit posts and stock prices to enable sentiment-driven market analysis for nine major equities. Each morning, a Spark-based notebook fetches recent subreddit discussions, timestamps them in Eastern Time, and applies a two-stage sentiment model (RoBERTa + FinBERT) before appending results to a Delta Lake table. A conditional workflow step checks the NYSE holiday calendar—skipping stock data retrieval on market closures. On trading days, a second notebook pulls daily closing prices via the Yahoo Finance API into a temporary table. Finally, a dataflow filters out already-stored dates and incrementally merges new price data into the central NYSE_stock_data table. This setup ensures robust, repeatable ingestion of social sentiment and price history, ready for downstream analysis or visualization.


[Power BI report](https://app.fabric.microsoft.com/view?r=eyJrIjoiN2NkYjNiNWUtNTY5ZC00YTZhLThhZGEtMjZjNWNlMGNjYjAzIiwidCI6IjZkYWRkOGM5LTMxMGEtNGE2Ni05MzRhLWQ5MGI1OTk5YjViMCJ9)

## Table of Contents

1. [Overview](#overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Setup & Prerequisites](#setup--prerequisites)
4. [Data Pipeline Components](#data-pipeline-components)

   * [1. Reddit Data ETL Notebook](#1-reddit-data-etl-notebook)
   * [2. Holiday Lookup & Conditional Trigger](#2-holiday-lookup--conditional-trigger)
   * [3. Daily Stock Data ETL Notebook](#3-daily-stock-data-etl-notebook)
   * [4. Incremental Load Dataflow](#4-incremental-load-dataflow)

---

## Overview

This repository hosts an end-to-end ETL pipeline built on Microsoft Fabric to:

* Ingest Reddit posts about nine selected stocks daily
* Perform multi-model sentiment analysis on each post
* Fetch daily stock prices (skipping NYSE holidays)
* Incrementally append only new stock data to the warehouse

---

## Architecture Diagram

<img width="1414" alt="image" src="https://github.com/user-attachments/assets/e3f7a9ba-15f2-4f6d-be69-8aa833dc8f0d" />

**High-level workflow:**

1. **Reddit ETL Notebook**: Gather new posts, time-convert, sentiment-label, write to `reddit_data`.
2. **Lookup**: Check `NYSE_holidays` table for today's holiday flag.
3. **If Condition**: If *not* holiday, trigger Stock ETL and Dataflow.
4. **Stock ETL Notebook**: Download daily closes to `temp_stock_data`.
5. **Dataflow**: Filter and append new rows into `NYSE_stock_data`.

---

## Setup & Prerequisites

* **Fabric Account**: Access to Lakehouse & Warehouse
* **Python 3.8+** with packages:

  * `praw`, `textblob`, `transformers`, `torch`, `yfinance`, `pyspark`
* Existing Delta tables:

  * `reddit_data`
  * `NYSE_stock_data`
* Holiday calendar in `[dbo].[NYSE_holidays]`

Configure credentials via Fabric Notebook secrets or environment variables.

---

## Data Pipeline Components

### 1. Reddit Data ETL Notebook

Path: `/notebooks/Daily_Reddit_ETL.ipynb`

### 2. Holiday Lookup & Conditional Trigger

In the Fabric pipeline canvas:

1. **Lookup Activity**: Query `dbo.NYSE_holidays` for today’s `trade_date`, retrieving `NYSE_holiday` flag.
2. **If Activity**:

   * **Expression**: `@equals(activity('Lookup1').output.firstRow.NYSE_holiday, false)`
   * **True branch**: Run Daily Stock ETL Notebook → Dataflow
   * **False branch**: End pipeline (Holiday)

### 3. Daily Stock Data ETL Notebook

Path: `/notebooks/Daily_Stock_Data_ETL.ipynb`


### 4. Incremental Load Dataflow

This Dataflow filters out any rows already present and appends only new `temp_stock_data` entries into `NYSE_stock_data`.

---
