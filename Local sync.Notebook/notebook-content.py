# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b2f28c43-5191-41e3-a5cd-40721f8bf8e0",
# META       "default_lakehouse_name": "Reddit_Data",
# META       "default_lakehouse_workspace_id": "1682a0d7-4d6c-47fd-88f0-3cb543c163d6",
# META       "known_lakehouses": [
# META         {
# META           "id": "b2f28c43-5191-41e3-a5cd-40721f8bf8e0"
# META         },
# META         {
# META           "id": "b3b54ba2-cc81-4a95-b001-84731590108e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

lists = [
    "reddit_data","Stock_Data.Date","Stock_Data.NYSE_calendar","Stock_Data.stock_data"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name in lists:
    # Read table and coalesce to 1 partition for single output file
    df = spark.read.table(table_name).coalesce(1)
    
    # Temporary directory path
    temp_dir = f"Files/{table_name}_temp"
    final_path = f"Files/{table_name}.csv"

    # Save as CSV to temp directory
    df.write.mode("overwrite").option("header", True).csv(temp_dir)

    # Get Spark's Hadoop FileSystem
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(temp_dir)

    # Find part file and rename
    files = fs.listStatus(path)
    for file in files:
        name = file.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            fs.rename(file.getPath(), spark._jvm.org.apache.hadoop.fs.Path(final_path))
            break

    # Delete temp directory
    fs.delete(path, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
