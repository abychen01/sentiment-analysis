# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ffe7fd25-f36f-428b-89a9-8fadad4239bf",
# META       "default_lakehouse_name": "Reddit",
# META       "default_lakehouse_workspace_id": "a2d18893-d874-4d0f-83d6-91d2ed3d9dfa",
# META       "known_lakehouses": [
# META         {
# META           "id": "ffe7fd25-f36f-428b-89a9-8fadad4239bf"
# META         },
# META         {
# META           "id": "05913245-8d89-4d7f-8eee-4582e03c1258"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Imports

# CELL ********************

import pyodbc, os
from pyspark.sql.functions import col, lit
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Definitions

# CELL ********************

df_creds = spark.read.parquet('Files/creds')

os.environ["AZURE_CLIENT_ID"] = df_creds.collect()[0]["AZURE_CLIENT_ID"]
os.environ["AZURE_TENANT_ID"] = df_creds.collect()[0]["AZURE_TENANT_ID"]
os.environ["AZURE_CLIENT_SECRET"] = df_creds.collect()[0]["AZURE_CLIENT_SECRET"]


vault_url = "https://vaultforfabric.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

password = client.get_secret("sql-server-password").value


table_list = ["Stock.stock_data","reddit_data"]
table_list_sql = ["Date","NYSE_calendar","stock_data","reddit_data"]

db = "myFreeDB"

conn_str_master = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE=master;"
            f"UID=admin2;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )
        
conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE={db};"
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

# MARKDOWN ********************

# #### DB check

# CELL ********************

with pyodbc.connect(conn_str_master, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = ?)
                BEGIN
                SELECT ? + 'doesnt exist';
                END
            ELSE
                BEGIN
                SELECT ? + ' exist';
                END
        """,db,db,db)

          
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

# MARKDOWN ********************

# #### Table check

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
    if table == "Stock_Data.NYSE_calendar":
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

# MARKDOWN ********************

# #### Write

# CELL ********************


jdbc_url = "jdbc:sqlserver://myfreesqldbserver66.database.windows.net:1433;" \
           "databaseName=myFreeDB;" \
           "encrypt=true;" \
           "trustServerCertificate=false;" \
           "hostNameInCertificate=*.database.windows.net;" \
           "loginTimeout=30;"

jdbc_properties = {
    "user": "admin2",
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def converts2(table_name):

    if table_name =="Stock_Data.NYSE_calendar":
        return "NYSE_calendar"
    if table_name =="Stock.stock_data":
        return "stock_data"


def latest_date(table_name):
    
    if table_name == 'reddit_data':
        date_value = 'time_utc'

    else:
        date_value = 'Date'

    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""

                EXEC('select top 1 [' + ? + '] from [' + ? + ']
                        order by [' + ? + '] desc')

            """, date_value, table_name, date_value)

            while True:
            
                result = cursor.fetchall()
                if result:
                    print(result[0])

                if not cursor.nextset():
                    break

    print('latest_date is',table_name)
    return result[0]

for table in table_list:

    try:
        df = spark.read.table(table)
        if table == "Stock_Data.NYSE_calendar":
            df = df.withColumnsRenamed({"Week of year": "week_of_year", "Day name": "day_name"})
            mode = "overwrite"
        else:
            mode = "append"

        if table != "reddit_data":
            table = converts2(table)
        
        if table == 'reddit_data' or 'stock_data':
            print('Table name is ',table,'and sending to the fn')


        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .option("batchsize", 1000) \
            .mode(mode) \
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

# MARKDOWN ********************

# #### Testing

# CELL ********************

# testing.....

'''
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


'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# create ticker table
'''

with pyodbc.connect(conn_str, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute(""" 
            IF NOT EXISTS(SELECT name FROM sys.tables WHERE name = ?)
                    BEGIN
                    SELECT '[' + ? + '] doesnt exist'
                    EXEC('CREATE TABLE ticker (
                            ticker VARCHAR(10),
                            name VARCHAR(100),
                            description VARCHAR(255)
                        );
                    ')
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
                    SELECT '[' + ? + '] created';
                    END
                ELSE
                    BEGIN
                    SELECT '[' + ? + '] exist'
                    END
            

            
        
        ""","ticker","ticker","ticker","ticker")

'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


jdbc_url = "jdbc:sqlserver://myfreesqldbserver66.database.windows.net:1433;" \
           "databaseName=myFreeDB;" \
           "encrypt=true;" \
           "trustServerCertificate=false;" \
           "hostNameInCertificate=*.database.windows.net;" \
           "loginTimeout=30;"

jdbc_properties = {
    "user": "admin2",
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def converts2(table_name):

    if table_name =="Stock_Data.NYSE_calendar":
        return "NYSE_calendar"
    if table_name =="Stock.stock_data":
        return "stock_data"


def latest_date(table_name, date_value):

    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""

                EXEC('select top 1 [' + ? + '] from [' + ? + ']
                        order by [' + ? + '] desc')

            """, date_value, table_name, date_value)

            while True:
            
                result = cursor.fetchall()
                if result:
                    print()

                if not cursor.nextset():
                    break

    return result[0]

for table in table_list:

    try:
        df = spark.read.table(table)
        if table == "Stock_Data.NYSE_calendar":
            df = df.withColumnsRenamed({"Week of year": "week_of_year", "Day name": "day_name"})
            mode = "overwrite"
        else:
            mode = "append"

        if table != "reddit_data":
            table = converts2(table)
        
        if table == 'reddit_data':
            date_value = 'time_utc'
        else:
            date_value = 'Date'


        if table == 'reddit_data' or 'stock_data':
            fn_return = latest_date(table, date_value)

        df = df.where(col(date_value)>(fn_return[0]))

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .option("batchsize", 1000) \
            .mode(mode) \
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
