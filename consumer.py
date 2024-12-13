from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import time
import random

# Event Hub connection details
EVENT_HUB_CONNECTION_STRING = "Endpoint=sb://realtime-trades-bucket.servicebus.windows.net/;SharedAccessKeyName=Databricks;SharedAccessKey=mrFOxfVTT8872Vk7GetP53I74FJebv1Xk+AEhIaUz20="
event_hub_config = {"eventhubs.connectionString": EVENT_HUB_CONNECTION_STRING}

# Azure Cosmos DB for MongoDB API connection details
cosmos_connection_string = "mongodb://cosmos1streaming1trades:mGdhp2qIOnxVx4cCbF0gn4O99x9M1l9FiKyTzW5ir3iYrKO9quvdOgYAUyA4ZsZKyWEf6GJU5n4OACDb9N7kag==@cosmos1streaming1trades.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@cosmos1streaming1trades@"
mongo_client = MongoClient(cosmos_connection_string)

# Define schema for trade data
datatrade_schema = StructType([
    StructField("tradeId", StringType(), True),
    StructField("assetClass", StringType(), True),
    StructField("tradeCaptureSystem", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("tradeType", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("traderId", StringType(), True),
    StructField("exchange", StringType(), True)
])

# Read from Event Hub
event_hub_stream = spark.readStream \
    .format("eventhubs") \
    .options(**event_hub_config) \
    .load()

# Decode the Event Hub body (if it's in a byte array format)
def decode_eventhub_body(body):
    return body.decode("utf-8") if isinstance(body, bytes) else body

decode_udf = udf(decode_eventhub_body, StringType())

# Parse the Event Hub data
parsed_stream = event_hub_stream \
    .withColumn("body", decode_udf(col("body"))) \
    .select(from_json(col("body"), datatrade_schema).alias("data")) \
    .filter(col("data").isNotNull()) \
    .select("data.*")

# Define a function to get database and collection names
def get_cosmos_database_and_collection(row):
    asset_class = row.get('assetClass', 'default_database')
    trade_capture_system = row.get('tradeCaptureSystem', 'default_collection')
    return asset_class, trade_capture_system

# Write data to Cosmos DB
def write_to_cosmos(batch_df, batch_id):
    def insert_into_cosmos(row):
        try:
            data_dict = row.asDict()
            print(f"Inserting into Cosmos DB: {data_dict}")  # Debugging line
            database, collection = get_cosmos_database_and_collection(data_dict)
            db = mongo_client[database]
            collection = db[collection]
            collection.insert_one(data_dict)
        except Exception as e:
            print(f"Failed to insert row: {row}, Error: {e}")
    
    # Iterate through each row in the batch and insert into Cosmos DB
    batch_df.foreach(lambda row: insert_into_cosmos(row))

# Write the parsed data to Cosmos DB
parsed_stream.writeStream \
    .foreachBatch(write_to_cosmos) \
    .start()

# Await termination
spark.streams.awaitAnyTermination(3600)  # Waits for 1 hour
