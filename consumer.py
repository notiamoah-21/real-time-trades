# Create a storage account 
# within the storage account create directory 
# Get access keys from the storage account you create 
# replace adls, with access keys, this will mount storage account 
# replace other variables 


# create a new cluster 13.3 LTS Databricsk run time 
# install libraries via maven 
com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22
org.mongodb.spark:mongo-spark-connector_2.12:10.4.0



## add this in a new cell 
# replace variables with your values

adls_access_key = ''
storageAccountName = ""
blobContainerName = ""
mountPoint = "/mnt/checkpoints/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()): 
    try:
       dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
            mount_point = mountPoint,
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': adls}
       )  
       print("mount successful")
    except Exception as e:
       print("mount exception", e)




# Make surce you have a cosmo db for mongo db, and create a database called trade and within that created a collection 
## add this code in a new cell 
# replace variables with your values, mongodb_uri and connectionString


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# MongoDB connection URI for Azure Cosmos DB Mongo API
mongodb_uri = "" 
# Step 2: Set up Event Hubs connection
connectionString = ""
ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Monthly_Sales_Aggregation") \
    .config("spark.jars", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

trade_schema = StructType([
    StructField("tradeId", StringType(), True),
    StructField("assetClass", StringType(), True),
    StructField("tradeCaptureSystem", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("tradeType", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("traderId", StringType(), True),
    StructField("exchange", StringType(), True)
])

data_stream = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load() 


# Convert body to String and parse JSON
parsed_stream = data_stream.selectExpr("CAST(body AS STRING) AS body") \
    .select(from_json(col("body"), trade_schema).alias("trade")) \
    .select("trade.*")  # Flatten the JSON structure


query = parsed_stream.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/mnt/checkpoints/mongo_streaming/") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option("spark.mongodb.connection.uri", mongodb_uri) \
    .option("spark.mongodb.database", "Trades") \
    .option("spark.mongodb.collection", "Collection1") \
    .outputMode("append") \
    .start()
