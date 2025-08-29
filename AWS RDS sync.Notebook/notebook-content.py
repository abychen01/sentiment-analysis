# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8400f71-0acf-4158-b761-0b86bf4c9f15",
# META       "default_lakehouse_name": "Reddit__Data",
# META       "default_lakehouse_workspace_id": "81da3283-2446-4563-9f8c-168297009931",
# META       "known_lakehouses": [
# META         {
# META           "id": "5cb9ed76-988c-4842-a3b3-b08c21c8139c"
# META         },
# META         {
# META           "id": "f8400f71-0acf-4158-b761-0b86bf4c9f15"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pyodbc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

password = spark.read.parquet("Files/creds").collect()[0]['password']

table_list = ["Stock_Data.Date","Stock_Data.NYSE_calendar","Stock_Data.stock_data","reddit_data"]
table_list_sql = ["Date","NYSE_calendar","stock_data","reddit_data"]

db = "sentiment_analysis"

conn_str_master = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com,1433;"
            f"DATABASE=master;"
            f"UID=admin;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )
        
conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com,1433;"
            f"DATABASE={db};"
            f"UID=admin;"
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

with pyodbc.connect(conn_str_master, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = ?)
                BEGIN
                SELECT ? + 'doesnt exist';
                EXEC('CREATE DATABASE ' + ?);
                SELECT ? + ' created';
                END
            ELSE
                BEGIN
                SELECT ? + ' exist';
                END
        """,db,db,db,db,db)

          
        while True:
            result = cursor.fetchall()
            if result:
                print(result[0])
            if not cursor.nextset():
                break
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def converts(datatype):
    datatype = datatype.simpleString()

    match datatype:
        case "int":
            return "INT"
        case "string":
            return "NVARCHAR(255)"  # Using NVARCHAR as requested
        case "timestamp":
            return "DATETIME"
        case "double":
            return "FLOAT"
        case "boolean":
            return "BIT"
        case "decimal":
            return "DECIMAL(18,2)"
        case _:
            return "NVARCHAR(255)"  # Default for unsupported types
            
for table in table_list:
    
    df = spark.read.table(table)
    if table == "Stock_Data.NYSE_calendar" or table == "Stock_Data.Date":
        df = df.withColumnsRenamed({"Week of year": "week_of_year", "Day name": "day_name"})
    
    table_cols = [f"{field.name} {converts(field.dataType)}" for field in df.schema.fields]
    

for table in table_list_sql:

    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                IF NOT EXISTS(SELECT name FROM sys.tables WHERE name = ?)
                    BEGIN
                    SELECT '[' + ? + '] doesnt exist'
                    EXEC('CREATE TABLE [' + ? + '] (' + ? + ')')
                    SELECT '[' + ? + '] created';
                    END
                ELSE
                    BEGIN
                    SELECT '[' + ? + '] exist'
                    END
            """, table,table,table,','.join(table_cols),table,table)

            while True:
            
                result = cursor.fetchall()
                if result:
                    print(result[0])
                if not cursor.nextset():
                    break


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

jdbc_url = f"jdbc:sqlserver://fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com:1433;\
            databaseName={db};encrypt=true;trustServerCertificate=true"
jdbc_properties = {
    "user": "admin",
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def converts2(table_name):
    if table_name == "Stock_Data.Date":
        return "Date"
    if table_name =="Stock_Data.NYSE_calendar":
        return "NYSE_calendar"
    if table_name =="Stock_Data.stock_data":
        return "stock_data"

for table in table_list:

    try:
        df = spark.read.table(table)
        if table == "Stock_Data.NYSE_calendar" or table == "Stock_Data.Date":
           df = df.withColumnsRenamed({"Week of year": "week_of_year", "Day name": "day_name"})

        if table != "reddit_data":
            table = converts2(table)
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .option("batchsize", 1000) \
            .mode("overwrite") \
            .save()
        print(f"Successfully wrote data to RDS table '{table}'.")




    except Exception as e:
        print(f"Failed to write to RDS: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# testing.....

for table in table_list_sql:
    

    with pyodbc.connect(conn_str,autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                
                EXEC('SELECT * FROM [' + ? + ']'
                
            
            )""",table)
            
            print(table)
            while True:
                result = cursor.fetchall()
                if result:
                    print(result[0])
                if not cursor.nextset():
                    break




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with pyodbc.connect(conn_str, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM ticker")

        while True:
            result = cursor.fetchall()
            if result:
                print(result)
                print()
            if not cursor.nextset():
                break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
# create ticker table

with pyodbc.connect(conn_str, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute(""" 
            CREATE TABLE ticker (
            ticker VARCHAR(10),
            name VARCHAR(100),
            description VARCHAR(255)
        );

        INSERT INTO ticker (ticker, name, description) VALUES
        ('TSLA', 'Tesla, Inc.', 'Tesla designs and manufactures electric vehicles and clean energy products.'),
        ('MSFT', 'Microsoft Corporation', 'Microsoft develops software, services, and hardware products, including Windows and Azure.'),
        ('AAPL', 'Apple Inc.', 'Apple designs and sells electronics, software, and online services, best known for the iPhone.'),
        ('GOOGL', 'Alphabet Inc.', 'Alphabet is the parent company of Google, specializing in internet services and products.'),
        ('NVDA', 'NVIDIA Corporation', 'NVIDIA designs graphics processing units and AI hardware/software solutions.'),
        ('AMZN', 'Amazon.com, Inc.', 'Amazon operates e-commerce platforms and provides cloud computing via AWS.'),
        ('META', 'Meta Platforms, Inc.', 'Meta operates social media services like Facebook, Instagram, and WhatsApp.'),
        ('AVGO', 'Broadcom Inc.', 'Broadcom designs, develops, and supplies semiconductor and infrastructure software solutions.'),
        ('TSM', 'Taiwan Semiconductor Manufacturing Company', 'TSMC manufactures semiconductors for global electronics companies.');
        """)

'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
